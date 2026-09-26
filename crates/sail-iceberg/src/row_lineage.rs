use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::{Result, exec_datafusion_err, plan_err};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

pub(crate) const ROW_ID_COLUMN: &str = "_row_id";
pub(crate) const LAST_UPDATED_SEQUENCE_COLUMN: &str = "_last_updated_sequence_number";
pub(crate) const LINEAGE_COLUMNS: [&str; 2] = [ROW_ID_COLUMN, LAST_UPDATED_SEQUENCE_COLUMN];

#[derive(Debug, Clone, Copy)]
pub struct RowLineage {
    pub first_row_id: Option<i64>,
    pub data_sequence_number: i64,
}

pub(crate) fn lineage_fields() -> [FieldRef; 2] {
    [
        (ROW_ID_COLUMN, 2147483540),
        (LAST_UPDATED_SEQUENCE_COLUMN, 2147483539),
    ]
    .map(|(name, id)| {
        Arc::new(
            Field::new(name, DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
        )
    })
}

pub(crate) fn append_lineage_fields(schema: &Schema) -> Result<Schema> {
    let mut fields = schema.fields().to_vec();
    for field in lineage_fields() {
        if schema.field_with_name(field.name()).is_ok() {
            return plan_err!(
                "Iceberg row lineage column '{}' conflicts with a table column",
                field.name()
            );
        }
        fields.push(field);
    }
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

pub(crate) fn materialize_lineage(
    batch: &RecordBatch,
    lineage: RowLineage,
    row_positions: &Int64Array,
) -> Result<Vec<ArrayRef>> {
    if row_positions.len() != batch.num_rows()
        || row_positions.null_count() != 0
        || row_positions.values().iter().any(|position| *position < 0)
    {
        return Err(exec_datafusion_err!(
            "Invalid Iceberg physical row positions"
        ));
    }
    let mut columns = batch.columns().to_vec();
    for name in LINEAGE_COLUMNS {
        let index = batch.schema().index_of(name)?;
        let values = columns[index]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| exec_datafusion_err!("Iceberg lineage column {name} must be long"))?;
        let values = (0..batch.num_rows())
            .map(|row| {
                if lineage.first_row_id.is_none() {
                    return Ok(None);
                }
                if values.is_valid(row) {
                    return Ok(Some(values.value(row)));
                }
                if name == LAST_UPDATED_SEQUENCE_COLUMN {
                    return Ok(Some(lineage.data_sequence_number));
                }
                lineage
                    .first_row_id
                    .map(|first| {
                        first
                            .checked_add(row_positions.value(row))
                            .filter(|id| *id >= 0)
                            .ok_or_else(|| exec_datafusion_err!("Iceberg row ID overflow"))
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        columns[index] = Arc::new(Int64Array::from(values));
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_lineage_uses_file_positions_and_retains_existing_values() -> Result<()> {
        let schema = Arc::new(Schema::new(lineage_fields().to_vec()));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![None, Some(20), None])),
                Arc::new(Int64Array::from(vec![Some(2), None, None])),
            ],
        )?;
        let columns = materialize_lineage(
            &batch,
            RowLineage {
                first_row_id: Some(100),
                data_sequence_number: 7,
            },
            &Int64Array::from(vec![4096, 4098, 4100]),
        )?;
        assert_eq!(
            columns[0].as_ref(),
            &Int64Array::from(vec![4196, 20, 4200]) as &dyn Array
        );
        assert_eq!(
            columns[1].as_ref(),
            &Int64Array::from(vec![2, 7, 7]) as &dyn Array
        );
        assert!(
            materialize_lineage(
                &batch,
                RowLineage {
                    first_row_id: Some(i64::MAX),
                    data_sequence_number: 7
                },
                &Int64Array::from(vec![1, 2, 3])
            )
            .is_err()
        );
        Ok(())
    }
}
