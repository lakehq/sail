use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use datafusion_common::{DataFusionError, Result};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::io::StoreContext;
use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::physical_plan::delete_writer_common;
use crate::row_level_metadata::{MERGE_PARTITION_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};
use crate::spec::types::values::Literal;
use crate::spec::{DataContentType, DataFile, TableMetadata};

#[derive(Debug, Clone, PartialEq)]
struct PositionDeleteTarget {
    file_path: String,
    partition_spec_id: i32,
    partition_json: String,
    partition: Vec<Option<Literal>>,
}

#[derive(Debug)]
struct PositionDeleteRows {
    target: PositionDeleteTarget,
    positions: BTreeSet<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct PositionDeleteAccumulator {
    // FIXME: Stream sorted positions into rolling delete files and aggregate all
    // emitted files before commit instead of buffering every position per data file.
    rows_by_file: BTreeMap<String, PositionDeleteRows>,
}

impl PositionDeleteAccumulator {
    pub(crate) fn add_batch(
        &mut self,
        table_meta: &TableMetadata,
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
            let rows = match self.rows_by_file.entry(file_path.to_string()) {
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
                        table_meta,
                        file_path,
                        partition_spec_id,
                        partition_json,
                    )?,
                    positions: BTreeSet::new(),
                }),
            };

            let row_index = row_indices.value(row);
            if row_index < 0 {
                return Err(DataFusionError::Plan(format!(
                    "MERGE position delete row index must be non-negative, got {row_index}"
                )));
            }
            rows.positions.insert(row_index);
        }
        Ok(())
    }

    pub(crate) async fn finish(
        self,
        store_ctx: &StoreContext,
        table_url: &Url,
        data_dir: &str,
    ) -> Result<Vec<DataFile>> {
        let mut delete_files = Vec::with_capacity(self.rows_by_file.len());
        for rows in self.rows_by_file.into_values() {
            delete_files.push(
                write_position_delete_file(
                    store_ctx,
                    table_url,
                    data_dir,
                    &rows.target,
                    &rows.positions,
                )
                .await?,
            );
        }
        Ok(delete_files)
    }
}

fn position_delete_target(
    table_meta: &TableMetadata,
    file_path: &str,
    partition_spec_id: i32,
    partition_json: &str,
) -> Result<PositionDeleteTarget> {
    if !table_meta
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
        file_path: file_path.to_string(),
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
    store_ctx: &StoreContext,
    table_url: &Url,
    data_dir: &str,
    target: &PositionDeleteTarget,
    positions: &BTreeSet<i64>,
) -> Result<DataFile> {
    let delete_schema = position_delete_arrow_schema();
    let file_paths = (0..positions.len())
        .map(|_| Some(target.file_path.as_str()))
        .collect::<Vec<_>>();
    let pos_values = positions.iter().copied().collect::<Vec<_>>();
    let batch = ArrowRecordBatch::try_new(
        Arc::new(delete_schema.clone()),
        vec![
            Arc::new(StringArray::from(file_paths)),
            Arc::new(Int64Array::from(pos_values)),
        ],
    )?;

    let mut writer = ArrowParquetWriter::try_new(&delete_schema, WriterProperties::default())
        .map_err(DataFusionError::Execution)?;
    writer
        .write_batch(&batch)
        .await
        .map_err(DataFusionError::Execution)?;
    let mut delete_file = delete_writer_common::write_delete_parquet_file(
        store_ctx,
        table_url,
        data_dir,
        "delete",
        writer,
        target.partition_spec_id,
        target.partition.clone(),
    )
    .await?;
    delete_file.content = DataContentType::PositionDeletes;
    delete_file.referenced_data_file = Some(target.file_path.clone());
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
