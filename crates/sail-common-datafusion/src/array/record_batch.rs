use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, LargeListArray, ListArray, MapArray, PrimitiveArray,
    RecordBatch, RecordBatchOptions, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, FieldRef, Fields, Schema, SchemaRef, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion_common::{DataFusionError, Result};

pub fn cast_record_batch(batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let fields = schema.fields();
    let columns = batch.columns();
    let columns = fields
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            let data_type = field.data_type();
            let column = cast(column, data_type)?;
            Ok(column)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Cast a RecordBatch to match the target schema's data types while preserving the
/// source batch's field names. This is used when the source data has deduplicated
/// field names (e.g., from Arrow IPC sent by PySpark) but the target schema has
/// duplicate field names (e.g., as specified by the user in Spark). Preserving the
/// source field names ensures that the result can be serialized back to Arrow without
/// errors from duplicate struct field names.
///
/// Target field metadata is preserved on the output fields.
pub fn cast_record_batch_preserving_names(
    batch: RecordBatch,
    target_schema: SchemaRef,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let source_fields = source_schema.fields();
    let target_fields = target_schema.fields();
    if source_fields.len() != target_fields.len() {
        return Err(DataFusionError::Internal(format!(
            "source schema has {} fields but target schema has {} fields",
            source_fields.len(),
            target_fields.len()
        )));
    }
    let (new_fields, new_columns): (Vec<FieldRef>, Vec<ArrayRef>) = source_fields
        .iter()
        .zip(target_fields.iter())
        .zip(batch.columns().iter())
        .map(|((src_field, tgt_field), column)| {
            let new_column =
                cast_array_preserving_names(column, src_field.data_type(), tgt_field.data_type())?;
            let new_type = new_column.data_type().clone();
            let new_field = Arc::new(
                src_field
                    .as_ref()
                    .clone()
                    .with_data_type(new_type)
                    .with_metadata(tgt_field.metadata().clone()),
            );
            Ok((new_field, new_column))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        target_schema.metadata().clone(),
    ));
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}

/// Cast an array to the target data type while preserving the source field names for
/// struct types. This handles duplicate field names by using positional matching.
/// Target field metadata is preserved on output struct fields.
fn cast_array_preserving_names(
    column: &ArrayRef,
    src_type: &DataType,
    tgt_type: &DataType,
) -> Result<ArrayRef> {
    if src_type == tgt_type {
        return Ok(column.clone());
    }
    match (src_type, tgt_type) {
        (DataType::Struct(src_children), DataType::Struct(tgt_children))
            if src_children.len() == tgt_children.len() =>
        {
            let struct_array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Internal("expected StructArray".to_string()))?;
            let (new_fields, new_arrays): (Vec<FieldRef>, Vec<ArrayRef>) = src_children
                .iter()
                .zip(tgt_children.iter())
                .enumerate()
                .map(|(i, (src_child, tgt_child))| {
                    let child_array = struct_array.column(i);
                    let new_child = cast_array_preserving_names(
                        child_array,
                        src_child.data_type(),
                        tgt_child.data_type(),
                    )?;
                    let new_type = new_child.data_type().clone();
                    let new_field = Arc::new(
                        src_child
                            .as_ref()
                            .clone()
                            .with_data_type(new_type)
                            .with_metadata(tgt_child.metadata().clone()),
                    );
                    Ok((new_field, new_child))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            let new_struct = StructArray::try_new(
                Fields::from(new_fields),
                new_arrays,
                struct_array.nulls().cloned(),
            )?;
            Ok(Arc::new(new_struct))
        }
        _ => Ok(cast(column, tgt_type)?),
    }
}

/// Helper function to handle timezone adjustment for timestamp arrays.
fn adjust_timestamp_timezone<T>(array: &ArrayRef, target_tz: Option<Arc<str>>) -> Result<ArrayRef>
where
    T: ArrowTimestampType,
{
    let timestamp_array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(format!(
                "Failed to downcast to timestamp array type: {:?}",
                array.data_type()
            ))
        })?;

    Ok(Arc::new(
        timestamp_array.clone().with_timezone_opt(target_tz),
    ))
}

/// Cast a RecordBatch to a target schema with relaxed timezone handling.
///
/// This function is similar to `cast_record_batch` but handles timestamp timezone
/// differences more gracefully by reinterpreting timezone metadata without converting
/// the underlying values. This is useful for Iceberg writes where timezone metadata
/// needs to be adjusted but the actual timestamp values should remain unchanged.
pub fn cast_record_batch_relaxed_tz(
    batch: &RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch> {
    if target.fields().is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            target.clone(),
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
        )?);
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());

    for field in target.fields() {
        let idx = batch.schema().index_of(field.name());
        let src = match idx {
            Ok(i) => batch.column(i),
            Err(_) => {
                if field.is_nullable() {
                    cols.push(new_null_array(field.data_type(), batch.num_rows()));
                    continue;
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Missing required column '{}' in input batch",
                        field.name()
                    )));
                }
            }
        };

        let casted = cast_array_recursively(src, field.data_type())?;
        cols.push(casted);
    }

    Ok(RecordBatch::try_new(target.clone(), cols)?)
}

fn cast_array_recursively(src: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    let src_type = src.data_type();
    if src_type == target_type {
        return Ok(src.clone());
    }

    // Handle timestamp timezone metadata adjustments before diving into nested logic.
    if let (DataType::Timestamp(src_unit, _), DataType::Timestamp(target_unit, target_tz)) =
        (src_type, target_type)
    {
        if src_unit == target_unit {
            let adjusted = match src_unit {
                TimeUnit::Second => {
                    adjust_timestamp_timezone::<TimestampSecondType>(src, target_tz.clone())?
                }
                TimeUnit::Millisecond => {
                    adjust_timestamp_timezone::<TimestampMillisecondType>(src, target_tz.clone())?
                }
                TimeUnit::Microsecond => {
                    adjust_timestamp_timezone::<TimestampMicrosecondType>(src, target_tz.clone())?
                }
                TimeUnit::Nanosecond => {
                    adjust_timestamp_timezone::<TimestampNanosecondType>(src, target_tz.clone())?
                }
            };
            return Ok(adjusted);
        }
    }

    match (src_type, target_type) {
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            cast_struct_array(src, target_fields)
        }
        (DataType::List(_), DataType::List(target_field)) => cast_list_array(src, target_field),
        (DataType::LargeList(_), DataType::LargeList(target_field)) => {
            cast_large_list_array(src, target_field)
        }
        (DataType::Map(_, _), DataType::Map(target_field, sorted)) => {
            cast_map_array(src, target_field, *sorted)
        }
        _ => {
            let casted = cast(src, target_type)?;
            Ok(casted)
        }
    }
}

fn cast_struct_array(src: &ArrayRef, target_fields: &Fields) -> Result<ArrayRef> {
    let struct_array = src.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to StructArray".to_string())
    })?;

    let mut new_children: Vec<ArrayRef> = Vec::with_capacity(target_fields.len());
    for target_field in target_fields.iter() {
        if let Some(child) = struct_array.column_by_name(target_field.name()) {
            new_children.push(cast_array_recursively(child, target_field.data_type())?);
        } else if target_field.is_nullable() {
            new_children.push(new_null_array(target_field.data_type(), struct_array.len()));
        } else {
            return Err(DataFusionError::Plan(format!(
                "Missing required field '{}' in nested struct",
                target_field.name()
            )));
        }
    }

    let new_struct = StructArray::try_new(
        target_fields.clone(),
        new_children,
        struct_array.nulls().cloned(),
    )?;
    Ok(Arc::new(new_struct))
}

fn cast_list_array(src: &ArrayRef, target_field: &FieldRef) -> Result<ArrayRef> {
    let list_array = src.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to ListArray".to_string())
    })?;

    let values = cast_array_recursively(list_array.values(), target_field.data_type())?;
    let new_list = ListArray::try_new(
        target_field.clone(),
        list_array.offsets().clone(),
        values,
        list_array.nulls().cloned(),
    )?;
    Ok(Arc::new(new_list))
}

fn cast_large_list_array(src: &ArrayRef, target_field: &FieldRef) -> Result<ArrayRef> {
    let list_array = src
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast array to LargeListArray".to_string())
        })?;

    let values = cast_array_recursively(list_array.values(), target_field.data_type())?;
    let new_list = LargeListArray::try_new(
        target_field.clone(),
        list_array.offsets().clone(),
        values,
        list_array.nulls().cloned(),
    )?;
    Ok(Arc::new(new_list))
}

fn cast_map_array(src: &ArrayRef, target_field: &FieldRef, sorted: bool) -> Result<ArrayRef> {
    let map_array = src.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to MapArray".to_string())
    })?;
    let entries: ArrayRef = Arc::new(map_array.entries().clone());
    let cast_entries = cast_array_recursively(&entries, target_field.data_type())?;
    let struct_entries = cast_entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Map entries must be struct arrays after casting".to_string())
        })?
        .clone();
    let new_map = MapArray::try_new(
        target_field.clone(),
        map_array.offsets().clone(),
        struct_entries,
        map_array.nulls().cloned(),
        sorted,
    )?;
    Ok(Arc::new(new_map))
}

pub fn read_record_batches(data: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn write_record_batches(batches: &[RecordBatch], schema: &Schema) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(output)
}

pub fn record_batch_with_schema(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        batch.columns().to_vec(),
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )?)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int32Array, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};

    use super::*;

    fn make_struct_array(fields: Vec<Field>, columns: Vec<ArrayRef>) -> StructArray {
        let field_refs_vec: Vec<FieldRef> = fields.into_iter().map(Arc::new).collect();
        let field_refs: Fields = field_refs_vec.into();
        StructArray::new(field_refs, columns, None)
    }

    #[test]
    fn cast_struct_reorders_fields_by_name() {
        let a_values = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let b_values = Arc::new(Int32Array::from(vec![Some(10), Some(20)]));
        let a_ref: ArrayRef = a_values.clone();
        let b_ref: ArrayRef = b_values.clone();
        let struct_array = Arc::new(make_struct_array(
            vec![
                Field::new("b", DataType::Int32, true),
                Field::new("a", DataType::Int32, true),
            ],
            vec![b_ref, a_ref],
        ));

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("b", DataType::Int32, true)),
                    Arc::new(Field::new("a", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        )]));

        let batch = RecordBatch::try_new(input_schema, vec![struct_array]).unwrap();

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("a", DataType::Int32, true)),
                    Arc::new(Field::new("b", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        )]));

        let casted = cast_record_batch_relaxed_tz(&batch, &target_schema).unwrap();
        let payload = casted
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a_cast = payload
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b_cast = payload
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(a_cast.values(), a_values.values());
        assert_eq!(b_cast.values(), b_values.values());
    }

    #[test]
    fn cast_struct_populates_missing_optional_fields() {
        let a_values = Arc::new(Int32Array::from(vec![Some(5), Some(6)]));
        let a_ref: ArrayRef = a_values.clone();
        let struct_array = Arc::new(make_struct_array(
            vec![Field::new("a", DataType::Int32, true)],
            vec![a_ref],
        ));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![Arc::new(Field::new("a", DataType::Int32, true))].into()),
            true,
        )]));
        let batch = RecordBatch::try_new(input_schema, vec![struct_array]).unwrap();

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("a", DataType::Int32, true)),
                    Arc::new(Field::new("b", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        )]));

        let casted = cast_record_batch_relaxed_tz(&batch, &target_schema).unwrap();
        let payload = casted
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let b_cast = payload
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(b_cast.null_count(), b_cast.len());
    }
}
