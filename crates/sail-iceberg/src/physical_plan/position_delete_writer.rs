use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use datafusion_common::{DataFusionError, Result};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use roaring::RoaringTreemap;
use url::Url;

use crate::io::StoreContext;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::physical_plan::delete_writer_common;
use crate::physical_plan::write_context::IcebergBaseWriteContext;
use crate::row_level_metadata::{MERGE_PARTITION_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};
use crate::spec::types::values::Literal;
use crate::spec::{DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestStatus};

#[derive(Debug, Clone, PartialEq)]
struct PositionDeleteTarget {
    partition_spec_id: i32,
    partition_json: String,
    partition: Vec<Option<Literal>>,
}

#[derive(Debug)]
struct PositionDeleteRows {
    target: PositionDeleteTarget,
    positions_by_file: BTreeMap<String, RoaringTreemap>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PositionDeleteGroupKey {
    File(String),
    Partition {
        partition_spec_id: i32,
        partition_json: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PositionDeleteGranularity {
    File,
    Partition,
}

impl PositionDeleteGranularity {
    fn from_base_context(base_table_context: &IcebergBaseWriteContext) -> Result<Self> {
        const PROPERTY: &str = "write.delete.granularity";
        match base_table_context
            .properties
            .get(PROPERTY)
            .map(String::as_str)
        {
            None => Ok(Self::Partition),
            Some(value) if value.eq_ignore_ascii_case("file") => Ok(Self::File),
            Some(value) if value.eq_ignore_ascii_case("partition") => Ok(Self::Partition),
            Some(value) => Err(DataFusionError::Plan(format!(
                "Unknown delete granularity: {value}"
            ))),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PositionDeleteAccumulator {
    // FIXME: Stream sorted positions into rolling delete files and aggregate all
    // emitted files before commit instead of buffering every position in memory.
    granularity: PositionDeleteGranularity,
    rows_by_group: BTreeMap<PositionDeleteGroupKey, PositionDeleteRows>,
}

impl PositionDeleteAccumulator {
    pub(crate) fn try_new(base_table_context: &IcebergBaseWriteContext) -> Result<Self> {
        Ok(Self {
            granularity: if base_table_context.format_version == FormatVersion::V3 {
                PositionDeleteGranularity::File
            } else {
                PositionDeleteGranularity::from_base_context(base_table_context)?
            },
            rows_by_group: BTreeMap::new(),
        })
    }

    pub(crate) fn add_batch(
        &mut self,
        base_table_context: &IcebergBaseWriteContext,
        batch: &ArrowRecordBatch,
        file_column_name: &str,
        row_index_column_name: &str,
    ) -> Result<()> {
        let file_paths = batch
            .column_by_name(file_column_name)
            .ok_or_else(|| DataFusionError::Internal(format!("missing column {file_column_name}")))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{file_column_name} must be a Utf8 column"))
            })?;
        let row_indices = batch
            .column_by_name(row_index_column_name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!("missing column {row_index_column_name}"))
            })?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{row_index_column_name} must be an Int64 column"
                ))
            })?;
        let partition_spec_ids = batch
            .column_by_name(MERGE_PARTITION_SPEC_ID_COLUMN)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "missing column {MERGE_PARTITION_SPEC_ID_COLUMN}"
                ))
            })?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{MERGE_PARTITION_SPEC_ID_COLUMN} must be an Int32 column"
                ))
            })?;
        let partitions = batch
            .column_by_name(MERGE_PARTITION_COLUMN)
            .ok_or_else(|| {
                DataFusionError::Internal(format!("missing column {MERGE_PARTITION_COLUMN}"))
            })?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{MERGE_PARTITION_COLUMN} must be a Utf8 column"))
            })?;

        for row in 0..batch.num_rows() {
            if file_paths.is_null(row) || row_indices.is_null(row) {
                continue;
            }
            if partition_spec_ids.is_null(row) || partitions.is_null(row) {
                return Err(DataFusionError::Plan(
                    "MERGE position delete rows require Iceberg partition metadata".to_string(),
                ));
            }

            let file_path = file_paths.value(row);
            let partition_spec_id = partition_spec_ids.value(row);
            let partition_json = partitions.value(row);
            let group_key = match self.granularity {
                PositionDeleteGranularity::File => {
                    PositionDeleteGroupKey::File(file_path.to_string())
                }
                PositionDeleteGranularity::Partition => PositionDeleteGroupKey::Partition {
                    partition_spec_id,
                    partition_json: partition_json.to_string(),
                },
            };
            let rows = match self.rows_by_group.entry(group_key) {
                Entry::Occupied(entry) => {
                    let rows = entry.into_mut();
                    if rows.target.partition_spec_id != partition_spec_id
                        || rows.target.partition_json != partition_json
                    {
                        return Err(DataFusionError::Plan(format!(
                            "inconsistent Iceberg partition metadata for MERGE target file {file_path}"
                        )));
                    }
                    rows
                }
                Entry::Vacant(entry) => entry.insert(PositionDeleteRows {
                    target: position_delete_target(
                        base_table_context,
                        file_path,
                        partition_spec_id,
                        partition_json,
                    )?,
                    positions_by_file: BTreeMap::new(),
                }),
            };

            let row_index = row_indices.value(row);
            if row_index < 0 {
                return Err(DataFusionError::Plan(format!(
                    "MERGE position delete row index must be non-negative, got {row_index}"
                )));
            }
            rows.positions_by_file
                .entry(file_path.to_string())
                .or_default()
                .insert(row_index as u64);
        }
        Ok(())
    }

    pub(crate) async fn finish_deletion_vectors(
        self,
        base: &IcebergBaseWriteContext,
        table_store: &StoreContext,
        data_store: &StoreContext,
        data_url: &Url,
    ) -> Result<Vec<DataFile>> {
        if self.rows_by_group.is_empty() {
            return Ok(vec![]);
        }
        let manifest_list = base.current_manifest_list.as_ref().ok_or_else(|| {
            datafusion_common::exec_datafusion_err!(
                "Iceberg deletion vectors require a pinned snapshot"
            )
        })?;
        let manifests = crate::io::load_manifest_list(table_store, manifest_list).await?;
        let deletes = crate::io::load_delete_file_index(
            &base.partition_specs,
            base.format_version,
            table_store,
            &manifests,
        )
        .await?;
        let mut targets = BTreeMap::new();
        for manifest_file in manifests
            .entries()
            .iter()
            .filter(|file| file.content == ManifestContentType::Data)
        {
            let manifest =
                crate::io::load_manifest(table_store, &manifest_file.manifest_path).await?;
            for entry in manifest
                .entries()
                .iter()
                .filter(|entry| entry.status != ManifestStatus::Deleted)
            {
                if self
                    .rows_by_group
                    .contains_key(&PositionDeleteGroupKey::File(
                        entry.data_file.file_path.clone(),
                    ))
                {
                    let mut file = entry.data_file.clone();
                    file.partition_spec_id = manifest_file.partition_spec_id;
                    targets.insert(
                        file.file_path.clone(),
                        (
                            file,
                            entry
                                .sequence_number
                                .unwrap_or(manifest_file.sequence_number),
                        ),
                    );
                }
            }
        }
        let mut output = Vec::new();
        for rows in self.rows_by_group.into_values() {
            for (path, positions) in rows.positions_by_file {
                let (target, sequence) = targets.get(&path).ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!(
                        "Iceberg deletion vector target is not live in the pinned snapshot: {path}"
                    )
                })?;
                if target.partition_spec_id != rows.target.partition_spec_id
                    || target.partition != rows.target.partition
                {
                    return datafusion_common::exec_err!(
                        "Iceberg deletion vector target partition does not match: {path}"
                    );
                }
                let applicable = deletes.for_data_file(target, *sequence);
                let mut combined = positions;
                combined |= super::delete_apply_exec::load_deleted_positions(
                    table_store,
                    &applicable.positional,
                    &path,
                )
                .await?;
                if combined
                    .max()
                    .is_some_and(|position| position >= target.record_count)
                {
                    return datafusion_common::exec_err!(
                        "Iceberg deletion vector position exceeds the data file row count: {path}"
                    );
                }
                output.push(
                    crate::io::deletion_vector::write_deletion_vector(
                        data_store, data_url, target, combined,
                    )
                    .await?,
                );
            }
        }
        Ok(output)
    }

    pub(crate) async fn finish(
        self,
        data_store_ctx: &StoreContext,
        data_url: &Url,
    ) -> Result<Vec<DataFile>> {
        let mut delete_files = Vec::with_capacity(self.rows_by_group.len());
        for rows in self.rows_by_group.into_values() {
            let referenced_data_file = match self.granularity {
                PositionDeleteGranularity::File => rows.positions_by_file.keys().next(),
                PositionDeleteGranularity::Partition => None,
            };
            delete_files.push(
                write_position_delete_file(
                    data_store_ctx,
                    data_url,
                    &rows.target,
                    &rows.positions_by_file,
                    referenced_data_file.map(String::as_str),
                )
                .await?,
            );
        }
        Ok(delete_files)
    }
}

fn position_delete_target(
    base_table_context: &IcebergBaseWriteContext,
    file_path: &str,
    partition_spec_id: i32,
    partition_json: &str,
) -> Result<PositionDeleteTarget> {
    if !base_table_context
        .partition_specs
        .iter()
        .any(|spec| spec.spec_id() == partition_spec_id)
    {
        return Err(DataFusionError::Plan(format!(
            "MERGE target file uses unknown Iceberg partition spec {partition_spec_id}: {file_path}"
        )));
    }
    let partition = serde_json::from_str(partition_json).map_err(|error| {
        DataFusionError::Plan(format!(
            "failed to decode Iceberg partition metadata for {file_path}: {error}"
        ))
    })?;
    Ok(PositionDeleteTarget {
        partition_spec_id,
        partition_json: partition_json.to_string(),
        partition,
    })
}

const POSITION_DELETE_FILE_PATH_COL: &str = "file_path";
const POSITION_DELETE_POS_COL: &str = "pos";
const POSITION_DELETE_FILE_PATH_ID: &str = "2147483546";
const POSITION_DELETE_POS_ID: &str = "2147483545";

async fn write_position_delete_file(
    data_store_ctx: &StoreContext,
    data_url: &Url,
    target: &PositionDeleteTarget,
    positions_by_file: &BTreeMap<String, RoaringTreemap>,
    referenced_data_file: Option<&str>,
) -> Result<DataFile> {
    let delete_schema = position_delete_arrow_schema();
    let row_count = usize::try_from(
        positions_by_file
            .values()
            .map(RoaringTreemap::len)
            .sum::<u64>(),
    )
    .map_err(|_| {
        datafusion_common::exec_datafusion_err!("Iceberg position delete count exceeds usize")
    })?;
    let mut file_paths = Vec::with_capacity(row_count);
    let mut pos_values = Vec::with_capacity(row_count);
    for (file_path, positions) in positions_by_file {
        for position in positions {
            file_paths.push(Some(file_path.as_str()));
            pos_values.push(position as i64);
        }
    }
    let batch = ArrowRecordBatch::try_new(
        Arc::new(delete_schema.clone()),
        vec![
            Arc::new(StringArray::from(file_paths)),
            Arc::new(Int64Array::from(pos_values)),
        ],
    )?;

    let mut writer =
        ArrowParquetWriter::try_new(&delete_schema, WriterProperties::default(), Vec::new())
            .map_err(DataFusionError::Execution)?;
    writer
        .write_batch(&batch)
        .await
        .map_err(DataFusionError::Execution)?;
    let mut delete_file = delete_writer_common::write_delete_parquet_file(
        data_store_ctx,
        data_url,
        "delete",
        writer,
        target.partition_spec_id,
        target.partition.clone(),
    )
    .await?;
    delete_file.content = DataContentType::PositionDeletes;
    delete_file.referenced_data_file = referenced_data_file.map(str::to_string);
    delete_file.sort_order_id = None;
    delete_file.equality_ids.clear();
    Ok(delete_file)
}

fn position_delete_arrow_schema() -> Schema {
    Schema::new(vec![
        Arc::new(
            Field::new(POSITION_DELETE_FILE_PATH_COL, DataType::Utf8, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    POSITION_DELETE_FILE_PATH_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        ),
        Arc::new(
            Field::new(POSITION_DELETE_POS_COL, DataType::Int64, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    POSITION_DELETE_POS_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        ),
    ])
}
