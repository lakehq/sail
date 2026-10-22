use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray,
};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, MERGE_SOURCE_METRIC_COLUMN, OPERATION_COLUMN,
    RowLevelOperationType, RowLevelWriteMode,
};

use crate::row_level_metadata::{MERGE_FILE_METADATA_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};

#[derive(Debug, Clone)]
pub(crate) struct IcebergMergeRowProjection {
    operation_index: usize,
    data_indices: Vec<usize>,
    data_schema: SchemaRef,
}

impl IcebergMergeRowProjection {
    pub(crate) fn try_new(input_schema: SchemaRef) -> Result<Self> {
        let operation_index = input_schema.index_of(OPERATION_COLUMN)?;
        let internal_columns = [
            MERGE_FILE_COLUMN,
            MERGE_ROW_INDEX_COLUMN,
            MERGE_SOURCE_METRIC_COLUMN,
            OPERATION_COLUMN,
            MERGE_PARTITION_SPEC_ID_COLUMN,
            MERGE_FILE_METADATA_COLUMN,
            crate::row_lineage::ROW_ID_COLUMN,
            crate::row_lineage::LAST_UPDATED_SEQUENCE_COLUMN,
        ];
        let data_indices = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                (!internal_columns.contains(&field.name().as_str())).then_some(index)
            })
            .collect::<Vec<_>>();
        let fields = data_indices
            .iter()
            .map(|index| input_schema.field(*index).clone())
            .collect::<Vec<_>>();
        let data_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        Ok(Self {
            operation_index,
            data_indices,
            data_schema,
        })
    }

    pub(crate) fn data_schema(&self) -> SchemaRef {
        Arc::clone(&self.data_schema)
    }

    pub(crate) fn project_data_rows(
        &self,
        batch: &RecordBatch,
        mode: Option<RowLevelWriteMode>,
        preserve_lineage: bool,
    ) -> Result<RecordBatch> {
        let mask = merge_operation_mask(batch, self.operation_index, |value| {
            merge_operation_writes_data(value)
                || (mode == Some(RowLevelWriteMode::CopyOnWrite)
                    && value == RowLevelOperationType::Copy.as_i32())
        })?;
        let filtered = filter_record_batch(batch, &mask)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let mut columns = self
            .data_indices
            .iter()
            .map(|index| filtered.column(*index).clone())
            .collect::<Vec<_>>();
        let schema = if preserve_lineage {
            let operations = cast(filtered.column(self.operation_index), &DataType::Int32)?;
            let operations = operations
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!(
                        "Expected Iceberg operation integers"
                    )
                })?;
            for name in crate::row_lineage::LINEAGE_COLUMNS {
                let values = filtered
                    .column_by_name(name)
                    .map(|array| cast(array, &DataType::Int64))
                    .transpose()?;
                let values = values
                    .as_ref()
                    .and_then(|array| array.as_any().downcast_ref::<Int64Array>());
                let retained = (0..filtered.num_rows())
                    .map(|row| {
                        let operation = operations.value(row);
                        let retain = operation != RowLevelOperationType::Insert.as_i32()
                            && (name == crate::row_lineage::ROW_ID_COLUMN
                                || operation == RowLevelOperationType::Copy.as_i32());
                        if !retain {
                            return Ok(None);
                        }
                        let values = values.ok_or_else(|| {
                            datafusion_common::exec_datafusion_err!(
                                "Iceberg row-level input is missing lineage column {name}"
                            )
                        })?;
                        Ok(values.is_valid(row).then(|| values.value(row)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                columns.push(Arc::new(Int64Array::from(retained)) as ArrayRef);
            }
            Arc::new(crate::row_lineage::append_lineage_fields(
                &self.data_schema,
            )?)
        } else {
            self.data_schema.clone()
        };
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    pub(crate) fn removed_file_paths(&self, batch: &RecordBatch) -> Result<Vec<String>> {
        let paths = batch.column_by_name(MERGE_FILE_COLUMN).ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("Iceberg COW input is missing file paths")
        })?;
        let paths = cast(paths, &DataType::Utf8)?;
        let paths = paths
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "Iceberg COW file paths must be strings"
                )
            })?;
        paths
            .iter()
            .flatten()
            .map(|path| {
                if path.is_empty() {
                    return datafusion_common::exec_err!("Iceberg COW file path cannot be empty");
                }
                Ok(path.to_string())
            })
            .collect()
    }

    pub(crate) fn project_position_delete_rows(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mask = merge_operation_mask(
            batch,
            self.operation_index,
            merge_operation_writes_position_delete,
        )?;
        filter_record_batch(batch, &mask)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

fn merge_operation_mask(
    batch: &RecordBatch,
    operation_index: usize,
    keep: impl Fn(i32) -> bool,
) -> Result<BooleanArray> {
    let column = batch.column(operation_index);

    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        let mask = (0..values.len())
            .map(|idx| (!values.is_null(idx)).then(|| keep(values.value(idx))))
            .collect::<BooleanArray>();
        return Ok(mask);
    }

    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        let mask = (0..values.len())
            .map(|idx| (!values.is_null(idx)).then(|| keep(values.value(idx) as i32)))
            .collect::<BooleanArray>();
        return Ok(mask);
    }

    Err(DataFusionError::Internal(format!(
        "{OPERATION_COLUMN} must be Int32 or Int64"
    )))
}

fn merge_operation_writes_data(value: i32) -> bool {
    value == RowLevelOperationType::Update.as_i32()
        || value == RowLevelOperationType::Insert.as_i32()
        || value == RowLevelOperationType::MatchedUpdate.as_i32()
        || value == RowLevelOperationType::NotMatchedBySourceUpdate.as_i32()
}

fn merge_operation_writes_position_delete(value: i32) -> bool {
    value == RowLevelOperationType::Delete.as_i32()
        || value == RowLevelOperationType::Update.as_i32()
        || value == RowLevelOperationType::MatchedDelete.as_i32()
        || value == RowLevelOperationType::MatchedUpdate.as_i32()
        || value == RowLevelOperationType::NotMatchedBySourceDelete.as_i32()
        || value == RowLevelOperationType::NotMatchedBySourceUpdate.as_i32()
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn merge_projection_routes_updates_to_data_and_position_deletes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new(MERGE_FILE_COLUMN, DataType::Utf8, true),
            Field::new(MERGE_ROW_INDEX_COLUMN, DataType::Int64, true),
            Field::new(MERGE_PARTITION_SPEC_ID_COLUMN, DataType::Int32, true),
            Field::new(MERGE_FILE_METADATA_COLUMN, DataType::Utf8, true),
            Field::new(OPERATION_COLUMN, DataType::Int32, false),
            Field::new(MERGE_SOURCE_METRIC_COLUMN, DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None])),
                Arc::new(StringArray::from(vec![
                    Some("data.parquet"),
                    Some("data.parquet"),
                    None,
                    None,
                ])),
                Arc::new(Int64Array::from(vec![Some(0), Some(1), None, None])),
                Arc::new(Int32Array::from(vec![Some(0), Some(0), None, None])),
                Arc::new(StringArray::from(vec![Some("[]"), Some("[]"), None, None])),
                Arc::new(Int32Array::from(vec![
                    RowLevelOperationType::MatchedUpdate.as_i32(),
                    RowLevelOperationType::MatchedDelete.as_i32(),
                    RowLevelOperationType::Insert.as_i32(),
                    RowLevelOperationType::SourceMetric.as_i32(),
                ])),
                Arc::new(Int64Array::from(vec![None, None, None, Some(3)])),
            ],
        )
        .expect("merge batch");
        let projection = IcebergMergeRowProjection::try_new(schema).expect("merge projection");

        let data_rows = projection
            .project_data_rows(&batch, Some(RowLevelWriteMode::MergeOnRead), false)
            .expect("data rows");
        let data_ids = data_rows
            .column_by_name("id")
            .expect("id")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 ids");
        assert_eq!(data_ids.values(), &[1, 3]);

        let delete_rows = projection
            .project_position_delete_rows(&batch)
            .expect("position-delete rows");
        let positions = delete_rows
            .column_by_name(MERGE_ROW_INDEX_COLUMN)
            .expect("row index")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 row indices");
        assert_eq!(positions.values(), &[0, 1]);
    }
}
