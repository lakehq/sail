// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::{SortColumn, lexsort_to_indices, take, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, plan_err,
    resources_datafusion_err, resources_err,
};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use object_store::ObjectStoreExt;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sail_common_datafusion::schema_evolution::{
    StructFieldMatching, cast_array_with_schema_evolution,
};

use crate::datasource::type_converter::iceberg_struct_to_arrow;
use crate::io::StoreContext;
use crate::metadata_relation::files::{partition_value, unified_partition_type};
use crate::spec::avro_utils::LimitedWriter;
use crate::spec::{
    DataContentType, DataFileFormat, FormatVersion, Manifest, ManifestFile, ManifestList,
    ManifestStatus, NestedField, PartitionStatisticsFile, PrimitiveType, Snapshot, StructType,
    TableMetadata, Type,
};
use crate::table::files::inherit_manifest_entry;

// The coordinator keeps the groups, sorts a single batch and stages one encoded file.
const MAX_MANIFEST_ENTRIES: usize = 1_000_000;
const MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const MAX_GROUPS: usize = 100_000;
const MAX_MATERIALIZED_BYTES: usize = 256 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 256 * 1024 * 1024;

#[derive(Default)]
struct Groups {
    values: HashMap<(ScalarValue, i32), Counters>,
    materialized_bytes: usize,
}

impl Groups {
    fn counters(&mut self, partition: ScalarValue, spec: i32) -> Result<&mut Counters> {
        let count = self.values.len();
        match self.values.entry((partition, spec)) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                if count >= MAX_GROUPS {
                    return resources_err!(
                        "Iceberg partition statistics group limit ({MAX_GROUPS}) exceeded"
                    );
                }
                // Also bound variable-width keys and the scalar columns built for sorting.
                let row_bytes =
                    entry.key().0.size() + 12 * size_of::<ScalarValue>() + size_of::<Counters>();
                reserve_materialized(&mut self.materialized_bytes, row_bytes)?;
                Ok(entry.insert(Counters::default()))
            }
        }
    }
}

pub(super) fn reserve_materialized(used: &mut usize, bytes: usize) -> Result<()> {
    if bytes > MAX_MATERIALIZED_BYTES - *used {
        return resources_err!(
            "Iceberg partition statistics materialization byte limit ({MAX_MATERIALIZED_BYTES}) exceeded"
        );
    }
    *used += bytes;
    Ok(())
}

async fn read_input(result: object_store::GetResult, remaining: &mut u64) -> Result<Bytes> {
    *remaining = remaining.checked_sub(result.meta.size).ok_or_else(|| {
        resources_datafusion_err!(
            "Iceberg partition statistics input byte limit ({MAX_INPUT_BYTES}) exceeded"
        )
    })?;
    Ok(result.bytes().await?)
}

#[derive(Default)]
struct Counters {
    data_records: i64,
    data_files: i32,
    data_bytes: i64,
    position_records: i64,
    position_files: i32,
    equality_records: i64,
    equality_files: i32,
    total_records: Option<i64>,
    last_updated: Option<i64>,
    last_snapshot: Option<i64>,
    dvs: i32,
}

// Drop column metrics, bounds and file paths as each Avro record is decoded.
struct FileMetrics {
    content: DataContentType,
    file_format: DataFileFormat,
    record_count: u64,
    file_size_in_bytes: u64,
}

struct PartitionEntry {
    key: ScalarValue,
    snapshot_id: Option<i64>,
    status: ManifestStatus,
    file: FileMetrics,
}

fn partition_entries(
    bytes: &[u8],
    manifest: &ManifestFile,
    metadata: &TableMetadata,
    partition: &StructType,
) -> Result<Vec<PartitionEntry>> {
    let mut next_row_id = manifest.first_row_id;
    let mut used = 0;
    let (_, entries) = Manifest::decode_avro_entries_with_limit(
        bytes,
        MAX_MANIFEST_ENTRIES,
        move |mut entry, _, version| {
            inherit_manifest_entry(manifest, version, &mut next_row_id, &mut entry)
                .map_err(|e| e.to_string())?;
            let file = entry.data_file;
            let key = partition_value(metadata, &file, partition).map_err(|e| e.to_string())?;
            reserve_materialized(
                &mut used,
                key.size() + size_of::<PartitionEntry>() - size_of::<ScalarValue>(),
            )
            .map_err(|e| e.to_string())?;
            Ok(PartitionEntry {
                key,
                status: entry.status,
                snapshot_id: entry.snapshot_id,
                file: FileMetrics {
                    content: file.content,
                    file_format: file.file_format,
                    record_count: file.record_count,
                    file_size_in_bytes: file.file_size_in_bytes,
                },
            })
        },
    )
    .map_err(DataFusionError::Execution)?;
    Ok(entries)
}

impl Counters {
    fn apply(&mut self, file: &FileMetrics, sign: i32) -> Result<()> {
        // The live row count can only be carried forward for untouched partitions.
        self.total_records = None;
        let signed = |value| {
            i64::try_from(value)
                .map(|v| i64::from(sign) * v)
                .map_err(|_| exec_datafusion_err!("Iceberg file metric exceeds int64"))
        };
        match file.content {
            DataContentType::Data => {
                self.data_records += signed(file.record_count)?;
                self.data_files += sign;
                self.data_bytes += signed(file.file_size_in_bytes)?;
            }
            DataContentType::PositionDeletes => {
                self.position_records += signed(file.record_count)?;
                if file.file_format == DataFileFormat::Puffin {
                    self.dvs += sign;
                } else {
                    self.position_files += sign;
                }
            }
            DataContentType::EqualityDeletes => {
                self.equality_records += signed(file.record_count)?;
                self.equality_files += sign;
            }
        }
        Ok(())
    }

    fn row(self, partition: ScalarValue, spec: i32, v3: bool) -> Vec<ScalarValue> {
        let mut row = vec![
            partition,
            spec.into(),
            self.data_records.into(),
            self.data_files.into(),
            self.data_bytes.into(),
            self.position_records.into(),
            self.position_files.into(),
            self.equality_records.into(),
            self.equality_files.into(),
            ScalarValue::Int64(self.total_records),
            ScalarValue::Int64(self.last_updated),
            ScalarValue::Int64(self.last_snapshot),
        ];
        if v3 {
            row.push(self.dvs.into());
        }
        row
    }
}

fn stats_schema(partition: StructType, v3: bool) -> StructType {
    let int = Type::Primitive(PrimitiveType::Int);
    let long = Type::Primitive(PrimitiveType::Long);
    let mut fields = vec![
        ("partition", Type::Struct(partition), false),
        ("spec_id", int.clone(), false),
        ("data_record_count", long.clone(), false),
        ("data_file_count", int.clone(), false),
        ("total_data_file_size_in_bytes", long.clone(), false),
        ("position_delete_record_count", long.clone(), !v3),
        ("position_delete_file_count", int.clone(), !v3),
        ("equality_delete_record_count", long.clone(), !v3),
        ("equality_delete_file_count", int.clone(), !v3),
        ("total_record_count", long.clone(), true),
        ("last_updated_at", long.clone(), true),
        ("last_updated_snapshot_id", long.clone(), true),
    ];
    if v3 {
        fields.push(("dv_count", int.clone(), false));
    }
    StructType::new(
        fields
            .into_iter()
            .zip(1..)
            .map(|((name, ty, nullable), id)| Arc::new(NestedField::new(id, name, ty, !nullable)))
            .collect(),
    )
}

// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/PartitionStatsHandler.java
pub(super) async fn compute(
    store: &StoreContext,
    metadata: &TableMetadata,
    snapshot: &Snapshot,
    ctx: &datafusion::execution::TaskContext,
) -> Result<Option<(Vec<u8>, &'static str)>> {
    let part = unified_partition_type(metadata, true)?;
    if part.fields().is_empty() {
        return plan_err!("Table must be partitioned");
    }
    let format = match metadata
        .properties
        .get("write.format.default")
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        None | Some("parquet") => "parquet",
        Some("avro") => "avro",
        Some(other) => {
            return datafusion_common::not_impl_err!(
                "Writing Iceberg partition statistics in {other} format"
            );
        }
    };
    let v3 = metadata.format_version >= FormatVersion::V3;
    let fields = stats_schema(part.clone(), v3);
    let DataType::Struct(arrow_fields) = iceberg_struct_to_arrow(&fields)? else {
        unreachable!()
    };
    let schema = Arc::new(Schema::new(arrow_fields));
    let snapshots: HashMap<_, _> = metadata
        .snapshots
        .iter()
        .map(|s| (s.snapshot_id(), s))
        .collect();
    let files: HashMap<_, _> = metadata
        .partition_statistics
        .iter()
        .map(|f| (f.snapshot_id, f))
        .collect();
    let mut pending = vec![];
    let mut seen = HashSet::new();
    let mut ancestor = Some(snapshot);
    let mut prior = None;
    let mut input_remaining = MAX_INPUT_BYTES;
    while let Some(s) = ancestor {
        if !seen.insert(s.snapshot_id()) {
            return exec_err!("Cycle in snapshot ancestry");
        }
        if let Some(file) = files.get(&s.snapshot_id()) {
            match read_stats(ctx, store, file, &fields, &schema, &mut input_remaining).await {
                Ok(groups) => prior = Some(groups),
                Err(error @ DataFusionError::ResourcesExhausted(_)) => return Err(error),
                Err(error) => log::warn!(
                    "Recomputing partition statistics after failing to read {}: {error}",
                    file.statistics_path
                ),
            }
            break;
        }
        pending.push(s);
        ancestor = s
            .parent_snapshot_id()
            .and_then(|id| snapshots.get(&id).copied());
    }

    let incremental = prior.is_some();
    let mut groups = prior.unwrap_or_default();
    if !incremental {
        pending = vec![snapshot];
    }
    for s in pending.into_iter().rev() {
        let (source, path) = store.resolve(s.manifest_list())?;
        let data = read_input(source.get(&path).await?, &mut input_remaining).await?;
        let list = ManifestList::read_entries(&data).map_err(DataFusionError::Execution)?;
        for manifest in list {
            let manifest = manifest.map_err(DataFusionError::Execution)?;
            if incremental && manifest.added_snapshot_id != s.snapshot_id() {
                continue;
            }
            let (source, path) = store.resolve(&manifest.manifest_path)?;
            let data = read_input(source.get(&path).await?, &mut input_remaining).await?;
            for entry in partition_entries(&data, &manifest, metadata, &part)? {
                let counters = groups.counters(entry.key, manifest.partition_spec_id)?;
                let sign = match (entry.status, incremental) {
                    (ManifestStatus::Deleted, true) => -1,
                    (ManifestStatus::Existing, true) => continue,
                    (ManifestStatus::Deleted, false) => 0,
                    _ => 1,
                };
                if sign != 0 {
                    counters.apply(&entry.file, sign)?;
                }
                if let Some(updated) = entry.snapshot_id.and_then(|id| snapshots.get(&id))
                    && counters
                        .last_updated
                        .is_none_or(|time| time < updated.timestamp_ms())
                {
                    counters.last_updated = Some(updated.timestamp_ms());
                    counters.last_snapshot = Some(updated.snapshot_id());
                }
            }
        }
    }
    if groups.values.is_empty() {
        return Ok(None);
    }
    let mut columns: Vec<_> = schema
        .fields
        .iter()
        .map(|_| Vec::with_capacity(groups.values.len()))
        .collect();
    for ((partition, spec), counts) in groups.values {
        for (col, value) in columns.iter_mut().zip(counts.row(partition, spec, v3)) {
            col.push(value);
        }
    }
    let arrays = columns
        .into_iter()
        .map(ScalarValue::iter_to_array)
        .collect::<Result<_>>()?;
    let batch = RecordBatch::try_new(schema, arrays)?;
    let order = lexsort_to_indices(
        &[SortColumn {
            values: batch.column(0).clone(),
            options: None,
        }],
        None,
    )?;
    let batch = take_record_batch(&batch, &order)?;
    let bytes = encode(&batch, &fields, &part, format, MAX_OUTPUT_BYTES)?;
    Ok(Some((bytes, format)))
}

fn encode(
    batch: &RecordBatch,
    fields: &StructType,
    partition: &StructType,
    format: &str,
    limit: usize,
) -> Result<Vec<u8>> {
    if format == "avro" {
        return super::avro::write(batch, fields, partition, limit);
    }
    let mut writer =
        ArrowWriter::try_new(LimitedWriter::new(Vec::new(), limit), batch.schema(), None)?;
    writer.write(batch)?;
    Ok(writer.into_inner()?.into_inner())
}

async fn read_stats(
    ctx: &datafusion::execution::TaskContext,
    store: &StoreContext,
    file: &PartitionStatisticsFile,
    fields: &StructType,
    schema: &SchemaRef,
    remaining: &mut u64,
) -> Result<Groups> {
    let (store, path) = store.statistics_object(ctx, &file.statistics_path)?;
    let data = read_input(store.get(&path).await?, remaining).await?;
    let reader: Box<dyn Iterator<Item = Result<RecordBatch>> + '_> = if data.starts_with(b"Obj\x01")
    {
        Box::new(std::iter::once(super::avro::read(
            &data,
            fields,
            schema.clone(),
        )))
    } else {
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        if builder.metadata().file_metadata().num_rows() > MAX_GROUPS as i64 {
            return resources_err!(
                "Iceberg partition statistics group limit ({MAX_GROUPS}) exceeded"
            );
        }
        let mut batch_size = 1024;
        for group in builder.metadata().row_groups() {
            let size = usize::try_from(group.total_byte_size())
                .map_err(|_| exec_datafusion_err!("Invalid Parquet row group size"))?;
            if size > MAX_MATERIALIZED_BYTES {
                return resources_err!(
                    "Iceberg partition statistics Parquet row group exceeds {MAX_MATERIALIZED_BYTES} bytes"
                );
            }
            // A dictionary value can expand to its full size in every decoded row.
            batch_size = batch_size.min(MAX_MATERIALIZED_BYTES / size.max(1));
        }
        Box::new(
            builder
                .with_batch_size(batch_size)
                .build()?
                .map(|batch| Ok(batch?)),
        )
    };
    let mut groups = Groups::default();
    for batch in reader {
        let batch = batch?;
        let input = batch.schema();
        let by_id = input
            .fields
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let id =
                    crate::datasource::type_converter::iceberg_field_id(f)?.ok_or_else(|| {
                        exec_datafusion_err!(
                            "Missing field ID for partition statistic {}",
                            f.name()
                        )
                    })?;
                Ok((id, idx))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let field = |id: i32| by_id.get(&id).map(|idx| batch.column(*idx));
        let keys = cast_array_with_schema_evolution(
            field(1).ok_or_else(|| exec_datafusion_err!("Missing partition"))?,
            schema.field(0),
            &DEFAULT_CAST_OPTIONS,
            StructFieldMatching::FieldId,
        )?;
        for row in 0..batch.num_rows() {
            let int = |id: i32| -> Result<Option<i32>> {
                let Some(array) = field(id) else {
                    return Ok(None);
                };
                match ScalarValue::try_from_array(array, row)? {
                    ScalarValue::Int32(value) => Ok(value),
                    _ => exec_err!("Invalid integer statistic {id}"),
                }
            };
            let long = |id: i32| -> Result<Option<i64>> {
                let Some(array) = field(id) else {
                    return Ok(None);
                };
                match ScalarValue::try_from_array(array, row)? {
                    ScalarValue::Int64(value) => Ok(value),
                    _ => exec_err!("Invalid long statistic {id}"),
                }
            };
            let spec = int(2)?.ok_or_else(|| exec_datafusion_err!("Missing spec_id"))?;
            // Own only this key when retaining it beyond the reader batch.
            let key = take(&keys, &UInt32Array::from(vec![row as u32]), None)?;
            *groups.counters(ScalarValue::try_from_array(&key, 0)?.compacted(), spec)? = Counters {
                data_records: long(3)?
                    .ok_or_else(|| exec_datafusion_err!("Missing data_record_count"))?,
                data_files: int(4)?
                    .ok_or_else(|| exec_datafusion_err!("Missing data_file_count"))?,
                data_bytes: long(5)?
                    .ok_or_else(|| exec_datafusion_err!("Missing total_data_file_size_in_bytes"))?,
                position_records: long(6)?.unwrap_or_default(),
                position_files: int(7)?.unwrap_or_default(),
                equality_records: long(8)?.unwrap_or_default(),
                equality_files: int(9)?.unwrap_or_default(),
                total_records: long(10)?,
                last_updated: long(11)?,
                last_snapshot: long(12)?,
                dvs: int(13)?.unwrap_or_default(),
            };
        }
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_budget_only_charges_new_partition_spec_pairs() -> Result<()> {
        let mut groups = Groups::default();
        for spec in 0..MAX_GROUPS as i32 {
            groups.counters(1.into(), spec)?;
        }
        let bytes = groups.materialized_bytes;
        groups.counters(1.into(), 0)?;
        assert_eq!(groups.materialized_bytes, bytes);
        assert!(matches!(
            groups.counters(2.into(), 0),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        Ok(())
    }

    #[test]
    fn materialization_budget_includes_variable_width_keys() -> Result<()> {
        let mut groups = Groups {
            materialized_bytes: MAX_MATERIALIZED_BYTES - 4096,
            ..Default::default()
        };
        groups.counters("small".into(), 0)?;
        assert!(matches!(
            groups.counters("x".repeat(4096).into(), 0),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(groups.values.len(), 1);
        Ok(())
    }

    #[test]
    fn encoded_limit_includes_file_headers_and_footers() -> Result<()> {
        let part = StructType::new(vec![Arc::new(NestedField::optional(
            1000,
            "p",
            Type::Primitive(PrimitiveType::Int),
        ))]);
        let fields = stats_schema(part.clone(), false);
        let DataType::Struct(arrow_fields) = iceberg_struct_to_arrow(&fields)? else {
            unreachable!()
        };
        let batch = RecordBatch::new_empty(Arc::new(Schema::new(arrow_fields)));
        for format in ["avro", "parquet"] {
            let size = encode(&batch, &fields, &part, format, MAX_OUTPUT_BYTES)?.len();
            assert_eq!(encode(&batch, &fields, &part, format, size)?.len(), size);
            assert!(encode(&batch, &fields, &part, format, size - 1).is_err());
        }
        Ok(())
    }
}
