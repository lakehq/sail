use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, StringArray, StructArray,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::{OPERATION_COLUMN, RowLevelOperationType};

use super::writer_exec::change_data_schema;

/// Expand accepted postimages once. The same arrays feed both main data and CDC output.
/// Preimages were packed before postimage expressions and identity allocation were evaluated.
pub(super) fn split_merge_change_data(
    batch: RecordBatch,
    preimage_column: Option<&str>,
    table_schema: &SchemaRef,
    merge_on_read: bool,
) -> Result<(RecordBatch, Vec<RecordBatch>)> {
    let operations = batch.column_by_name(OPERATION_COLUMN).ok_or_else(|| {
        DataFusionError::Internal("MERGE change data is missing row operation tags".into())
    })?;
    let operation = |row: usize| -> Result<i64> {
        if operations.is_null(row) {
            return Err(DataFusionError::Internal(
                "MERGE change data has null row operation".into(),
            ));
        }
        if let Some(values) = operations.as_any().downcast_ref::<Int32Array>() {
            Ok(i64::from(values.value(row)))
        } else if let Some(values) = operations.as_any().downcast_ref::<Int64Array>() {
            Ok(values.value(row))
        } else {
            Err(DataFusionError::Internal(
                "invalid MERGE row operation type".into(),
            ))
        }
    };
    let tags = (0..batch.num_rows())
        .map(operation)
        .collect::<Result<Vec<_>>>()?;
    let is = |tag: i64, expected: RowLevelOperationType| tag == i64::from(expected.as_i32());
    let updated = |tag| {
        is(tag, RowLevelOperationType::MatchedUpdate)
            || is(tag, RowLevelOperationType::NotMatchedBySourceUpdate)
    };
    let deleted = |tag| {
        is(tag, RowLevelOperationType::MatchedDelete)
            || is(tag, RowLevelOperationType::NotMatchedBySourceDelete)
    };
    let mut changes = Vec::new();
    for (change_type, before) in [
        ("insert", false),
        ("update_postimage", false),
        ("update_preimage", true),
        ("delete", true),
    ] {
        let mask = BooleanArray::from_iter(tags.iter().map(|&tag| {
            Some(match change_type {
                "insert" => is(tag, RowLevelOperationType::Insert),
                "update_postimage" | "update_preimage" => updated(tag),
                _ => deleted(tag),
            })
        }));
        let selected = filter_record_batch(&batch, &mask)?;
        if selected.num_rows() == 0 {
            continue;
        }
        let preimage = if before && let Some(preimage_column) = preimage_column {
            Some(
                selected
                    .column_by_name(preimage_column)
                    .and_then(|array| array.as_any().downcast_ref::<StructArray>())
                    .ok_or_else(|| {
                        DataFusionError::Internal("missing packed MERGE preimage".into())
                    })?,
            )
        } else {
            None
        };
        let mut arrays = table_schema
            .fields()
            .iter()
            .map(|field| {
                let column = match preimage {
                    Some(preimage) => preimage.column_by_name(field.name()),
                    None => selected.column_by_name(field.name()),
                };
                column.cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "missing MERGE change column {}",
                        field.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        arrays.push(Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n(change_type, selected.num_rows()),
        )));
        changes.push(RecordBatch::try_new(
            change_data_schema(table_schema),
            arrays,
        )?);
    }
    let main = if merge_on_read {
        let mask = BooleanArray::from_iter(
            tags.iter()
                .map(|&tag| Some(!deleted(tag) && !is(tag, RowLevelOperationType::Copy))),
        );
        filter_record_batch(&batch, &mask)?
    } else {
        batch
    };
    let indices = main
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            (Some(field.name().as_str()) != preimage_column).then_some(index)
        })
        .collect::<Vec<_>>();
    Ok((main.project(&indices)?, changes))
}
