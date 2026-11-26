// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{SortColumn, lexsort_to_indices, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::{Result, ScalarValue, exec_datafusion_err, exec_err, plan_err};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use object_store::ObjectStoreExt;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sail_common_datafusion::schema_evolution::{
    StructFieldMatching, cast_array_with_schema_evolution,
};

use crate::datasource::type_converter::iceberg_struct_to_arrow;
use crate::io::{StoreContext, load_manifest_list_with_version};
use crate::metadata_relation::files::{partition_value, unified_partition_type};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestStatus, NestedField,
    PartitionStatisticsFile, PrimitiveType, Snapshot, StructType, TableMetadata, Type,
};
use crate::table::files::manifest_entries;

type Groups = HashMap<(ScalarValue, i32), Counters>;

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

impl Counters {
    fn apply(&mut self, file: &DataFile, sign: i32) -> Result<()> {
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
    while let Some(s) = ancestor {
        if !seen.insert(s.snapshot_id()) {
            return exec_err!("Cycle in snapshot ancestry");
        }
        if let Some(file) = files.get(&s.snapshot_id()) {
            match read_stats(ctx, store, file, &fields, &schema).await {
                Ok(groups) => prior = Some(groups),
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
        let list =
            load_manifest_list_with_version(store, s.manifest_list(), metadata.format_version)
                .await?;
        for manifest in list.entries() {
            if incremental && manifest.added_snapshot_id != s.snapshot_id() {
                continue;
            }
            for entry in manifest_entries(store, manifest).await? {
                let file = &entry.data_file;
                let key = partition_value(metadata, file, &part)?;
                let counters = groups.entry((key, file.partition_spec_id)).or_default();
                let sign = match (entry.status, incremental) {
                    (ManifestStatus::Deleted, true) => -1,
                    (ManifestStatus::Existing, true) => continue,
                    (ManifestStatus::Deleted, false) => 0,
                    _ => 1,
                };
                if sign != 0 {
                    counters.apply(file, sign)?;
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
    if groups.is_empty() {
        return Ok(None);
    }
    let mut columns = vec![Vec::new(); schema.fields.len()];
    for ((partition, spec), counts) in groups {
        for (col, value) in columns.iter_mut().zip(counts.row(partition, spec, v3)) {
            col.push(value);
        }
    }
    let arrays = columns
        .into_iter()
        .map(ScalarValue::iter_to_array)
        .collect::<Result<_>>()?;
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let order = lexsort_to_indices(
        &[SortColumn {
            values: batch.column(0).clone(),
            options: None,
        }],
        None,
    )?;
    let batch = take_record_batch(&batch, &order)?;
    let bytes = if format == "avro" {
        super::avro::write(&batch, &fields, &part)?
    } else {
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
        writer.write(&batch)?;
        writer.into_inner()?
    };
    Ok(Some((bytes, format)))
}

async fn read_stats(
    ctx: &datafusion::execution::TaskContext,
    store: &StoreContext,
    file: &PartitionStatisticsFile,
    fields: &StructType,
    schema: &SchemaRef,
) -> Result<Groups> {
    let (store, path) = store.statistics_object(ctx, &file.statistics_path)?;
    let data = store.get(&path).await?.bytes().await?;
    let reader: Box<dyn Iterator<Item = Result<RecordBatch>> + '_> = if data.starts_with(b"Obj\x01")
    {
        Box::new(std::iter::once(super::avro::read(
            &data,
            fields,
            schema.clone(),
        )))
    } else {
        Box::new(
            ParquetRecordBatchReaderBuilder::try_new(data)?
                .build()?
                .map(|batch| Ok(batch?)),
        )
    };
    let mut groups = Groups::new();
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
            groups.insert(
                (ScalarValue::try_from_array(&keys, row)?, spec),
                Counters {
                    data_records: long(3)?
                        .ok_or_else(|| exec_datafusion_err!("Missing data_record_count"))?,
                    data_files: int(4)?
                        .ok_or_else(|| exec_datafusion_err!("Missing data_file_count"))?,
                    data_bytes: long(5)?.ok_or_else(|| {
                        exec_datafusion_err!("Missing total_data_file_size_in_bytes")
                    })?,
                    position_records: long(6)?.unwrap_or_default(),
                    position_files: int(7)?.unwrap_or_default(),
                    equality_records: long(8)?.unwrap_or_default(),
                    equality_files: int(9)?.unwrap_or_default(),
                    total_records: long(10)?,
                    last_updated: long(11)?,
                    last_snapshot: long(12)?,
                    dvs: int(13)?.unwrap_or_default(),
                },
            );
        }
    }
    Ok(groups)
}
