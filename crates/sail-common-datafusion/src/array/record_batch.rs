use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, PrimitiveArray, RecordBatch,
    RecordBatchOptions, StructArray, make_array, new_null_array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, FieldRef, Fields, Schema, SchemaRef, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UnionFields,
};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion_common::{DataFusionError, Result};

fn retag_timestamp_field(field: &FieldRef, timezone: &Arc<str>) -> Result<FieldRef> {
    Ok(Arc::new(field.as_ref().clone().with_data_type(
        retag_timestamp_data_type(field.data_type(), timezone)?,
    )))
}

pub fn retag_timestamp_data_type(data_type: &DataType, timezone: &Arc<str>) -> Result<DataType> {
    Ok(match data_type {
        DataType::Timestamp(unit, Some(_)) => {
            DataType::Timestamp(*unit, Some(Arc::clone(timezone)))
        }
        DataType::List(field) => DataType::List(retag_timestamp_field(field, timezone)?),
        DataType::ListView(field) => DataType::ListView(retag_timestamp_field(field, timezone)?),
        DataType::FixedSizeList(field, length) => {
            DataType::FixedSizeList(retag_timestamp_field(field, timezone)?, *length)
        }
        DataType::LargeList(field) => DataType::LargeList(retag_timestamp_field(field, timezone)?),
        DataType::LargeListView(field) => {
            DataType::LargeListView(retag_timestamp_field(field, timezone)?)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| retag_timestamp_field(field, timezone))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        DataType::Map(field, sorted) => {
            DataType::Map(retag_timestamp_field(field, timezone)?, *sorted)
        }
        DataType::Union(fields, mode) => {
            let mut type_ids = Vec::with_capacity(fields.len());
            let mut output_fields = Vec::with_capacity(fields.len());
            for (type_id, field) in fields.iter() {
                type_ids.push(type_id);
                output_fields.push(retag_timestamp_field(field, timezone)?);
            }
            DataType::Union(UnionFields::try_new(type_ids, output_fields)?, *mode)
        }
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(retag_timestamp_data_type(key, timezone)?),
            Box::new(retag_timestamp_data_type(value, timezone)?),
        ),
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            retag_timestamp_field(run_ends, timezone)?,
            retag_timestamp_field(values, timezone)?,
        ),
        _ => data_type.clone(),
    })
}

pub fn retag_timestamp_array(array: &ArrayRef, timezone: &Arc<str>) -> Result<ArrayRef> {
    let data_type = retag_timestamp_data_type(array.data_type(), timezone)?;
    if &data_type == array.data_type() {
        return Ok(Arc::clone(array));
    }
    let data = array.to_data();
    let child_data = data
        .child_data()
        .iter()
        .map(|child| {
            let child = make_array(child.clone());
            Ok(retag_timestamp_array(&child, timezone)?.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    let data = data
        .into_builder()
        .data_type(data_type)
        .child_data(child_data);
    // SAFETY: `data` came from an existing valid array. This preserves its length,
    // offset, buffers, nulls, and container layout; it changes only timestamp
    // timezone metadata and recursively retags children to match their parent fields.
    Ok(make_array(unsafe { data.build_unchecked() }))
}

pub fn retag_schema_timestamp_timezone(schema: &Schema, timezone: &str) -> Result<Schema> {
    let timezone = Arc::<str>::from(timezone);
    let fields = schema
        .fields()
        .iter()
        .map(|field| retag_timestamp_field(field, &timezone))
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

pub fn retag_record_batch_timestamp_timezone(
    batch: &RecordBatch,
    timezone: &str,
) -> Result<RecordBatch> {
    let timezone = Arc::<str>::from(timezone);
    let input_schema = batch.schema_ref();
    let fields = input_schema
        .fields()
        .iter()
        .map(|field| retag_timestamp_field(field, &timezone))
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    if schema.as_ref() == input_schema.as_ref() {
        return Ok(batch.clone());
    }

    let columns = batch
        .columns()
        .iter()
        .map(|array| retag_timestamp_array(array, &timezone))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )?)
}

pub fn cast_record_batch_positionally(
    batch: RecordBatch,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let fields = schema.fields();
    let columns = batch.columns();
    let columns = fields
        .iter()
        .zip(columns)
        .map(|(field, column)| cast_array_positionally_recursively(column, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() {
        Ok(RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
        )?)
    } else {
        Ok(RecordBatch::try_new(schema, columns)?)
    }
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

    if cols.is_empty() {
        Ok(RecordBatch::try_new_with_options(
            target.clone(),
            cols,
            &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
        )?)
    } else {
        Ok(RecordBatch::try_new(target.clone(), cols)?)
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

pub fn cast_array_recursively(src: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    let src_type = src.data_type();
    if src_type == target_type {
        return Ok(src.clone());
    }

    // Handle timestamp timezone metadata adjustments before diving into nested logic.
    if let (DataType::Timestamp(src_unit, _), DataType::Timestamp(target_unit, target_tz)) =
        (src_type, target_type)
        && src_unit == target_unit
    {
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

pub fn cast_array_positionally_recursively(
    src: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef> {
    let src_type = src.data_type();
    if src_type == target_type {
        return Ok(src.clone());
    }

    if let (DataType::Timestamp(src_unit, _), DataType::Timestamp(target_unit, target_tz)) =
        (src_type, target_type)
        && src_unit == target_unit
    {
        return match src_unit {
            TimeUnit::Second => {
                adjust_timestamp_timezone::<TimestampSecondType>(src, target_tz.clone())
            }
            TimeUnit::Millisecond => {
                adjust_timestamp_timezone::<TimestampMillisecondType>(src, target_tz.clone())
            }
            TimeUnit::Microsecond => {
                adjust_timestamp_timezone::<TimestampMicrosecondType>(src, target_tz.clone())
            }
            TimeUnit::Nanosecond => {
                adjust_timestamp_timezone::<TimestampNanosecondType>(src, target_tz.clone())
            }
        };
    }

    match (src_type, target_type) {
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            cast_struct_array_positionally(src, target_fields)
        }
        (DataType::List(_), DataType::List(target_field)) => {
            cast_list_array_positionally(src, target_field)
        }
        (DataType::LargeList(_), DataType::LargeList(target_field)) => {
            cast_large_list_array_positionally(src, target_field)
        }
        (DataType::Map(_, _), DataType::Map(target_field, sorted)) => {
            cast_map_array_positionally(src, target_field, *sorted)
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

fn cast_struct_array_positionally(src: &ArrayRef, target_fields: &Fields) -> Result<ArrayRef> {
    let struct_array = src.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to StructArray".to_string())
    })?;
    if struct_array.num_columns() != target_fields.len() {
        return Err(DataFusionError::Plan(format!(
            "Struct field count mismatch: expected {} fields but found {} fields",
            target_fields.len(),
            struct_array.num_columns()
        )));
    }

    let new_children = target_fields
        .iter()
        .zip(struct_array.columns())
        .map(|(target_field, child)| {
            cast_array_positionally_recursively(child, target_field.data_type())
        })
        .collect::<Result<Vec<_>>>()?;
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

fn cast_list_array_positionally(src: &ArrayRef, target_field: &FieldRef) -> Result<ArrayRef> {
    let list_array = src.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to ListArray".to_string())
    })?;

    let values =
        cast_array_positionally_recursively(list_array.values(), target_field.data_type())?;
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

fn cast_large_list_array_positionally(src: &ArrayRef, target_field: &FieldRef) -> Result<ArrayRef> {
    let list_array = src
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast array to LargeListArray".to_string())
        })?;

    let values =
        cast_array_positionally_recursively(list_array.values(), target_field.data_type())?;
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

fn cast_map_array_positionally(
    src: &ArrayRef,
    target_field: &FieldRef,
    sorted: bool,
) -> Result<ArrayRef> {
    let map_array = src.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to MapArray".to_string())
    })?;
    let entries: ArrayRef = Arc::new(map_array.entries().clone());
    let cast_entries = cast_array_positionally_recursively(&entries, target_field.data_type())?;
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
    use datafusion::arrow::array::{
        ArrayData, ArrayRef, DictionaryArray, Int8Array, Int16Array, Int32Array, RunArray,
        StructArray, TimestampMicrosecondArray, UnionArray,
    };
    use datafusion::arrow::buffer::ScalarBuffer;
    use datafusion::arrow::datatypes::{Field, Fields, Int8Type, Int16Type};

    use super::*;

    fn make_struct_array(fields: Vec<Field>, columns: Vec<ArrayRef>) -> StructArray {
        let field_refs_vec: Vec<FieldRef> = fields.into_iter().map(Arc::new).collect();
        let field_refs: Fields = field_refs_vec.into();
        StructArray::new(field_refs, columns, None)
    }

    fn assert_physical_buffers_shared(left: &ArrayData, right: &ArrayData) {
        assert_eq!(left.len(), right.len());
        assert_eq!(left.offset(), right.offset());
        assert_eq!(left.buffers().len(), right.buffers().len());
        for (left, right) in left.buffers().iter().zip(right.buffers()) {
            assert!(left.ptr_eq(right));
        }
        match (left.nulls(), right.nulls()) {
            (Some(left), Some(right)) => assert!(left.buffer().ptr_eq(right.buffer())),
            (None, None) => {}
            _ => panic!("null buffers differ"),
        }
        assert_eq!(left.child_data().len(), right.child_data().len());
        for (left, right) in left.child_data().iter().zip(right.child_data()) {
            assert_physical_buffers_shared(left, right);
        }
    }

    #[test]
    fn retag_timestamp_timezone_recursively_without_changing_values() {
        let timestamp: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(-3_723_000_000), None])
                .with_timezone("+01:02:03"),
        );
        let timestamp_data = timestamp.to_data();
        let values_buffer = timestamp_data.buffers()[0].as_ptr();
        let validity_buffer = timestamp_data.nulls().unwrap().buffer().as_ptr();
        let nested_fields: Fields = vec![Arc::new(Field::new(
            "ts",
            timestamp.data_type().clone(),
            true,
        ))]
        .into();
        let payload: ArrayRef = Arc::new(StructArray::new(
            nested_fields.clone(),
            vec![timestamp],
            None,
        ));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Struct(nested_fields),
                true,
            )])),
            vec![payload],
        )
        .unwrap();
        let internal = retag_record_batch_timestamp_timezone(&batch, "UTC").unwrap();
        let internal_schema = internal.schema();
        let DataType::Struct(fields) = internal_schema.field(0).data_type() else {
            panic!("expected struct");
        };
        assert_eq!(
            fields[0].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
        let payload = internal
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let timestamp = payload
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamp.value(0), -3_723_000_000);
        assert!(timestamp.is_null(1));
        let timestamp_data = timestamp.to_data();
        assert_eq!(timestamp_data.buffers()[0].as_ptr(), values_buffer);
        assert_eq!(
            timestamp_data.nulls().unwrap().buffer().as_ptr(),
            validity_buffer
        );

        let external = retag_record_batch_timestamp_timezone(&internal, "+01:02:03").unwrap();
        let external_schema = external.schema();
        let DataType::Struct(fields) = external_schema.field(0).data_type() else {
            panic!("expected struct");
        };
        assert_eq!(
            fields[0].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+01:02:03")),)
        );
    }

    #[test]
    fn retag_timestamp_timezone_reuses_unchanged_batch_storage() {
        let timestamp: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(0)]).with_timezone("UTC"));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                timestamp.data_type().clone(),
                false,
            )])),
            vec![timestamp],
        )
        .unwrap();
        let output = retag_record_batch_timestamp_timezone(&batch, "UTC").unwrap();

        assert!(Arc::ptr_eq(batch.schema_ref(), output.schema_ref()));
        assert!(Arc::ptr_eq(batch.column(0), output.column(0)));
    }

    #[test]
    fn retag_sliced_union_dictionary_and_run_array_remains_valid_and_zero_copy() {
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0_i8, 1]),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(10), Some(20)])
                        .with_timezone("+01:02:03"),
                ),
            )
            .unwrap(),
        );
        let run_ends = Int16Array::from(vec![1_i16, 3]);
        let run_values: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(30), Some(40)]).with_timezone("+01:02:03"),
        );
        let run: ArrayRef =
            Arc::new(RunArray::<Int16Type>::try_new(&run_ends, run_values.as_ref()).unwrap());
        let fields = UnionFields::try_new(
            [3_i8, 9_i8],
            [
                Field::new("dictionary", dictionary.data_type().clone(), true),
                Field::new("run", run.data_type().clone(), true),
            ],
        )
        .unwrap();
        let union = UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![3_i8, 9, 3]),
            Some(ScalarBuffer::from(vec![0_i32, 0, 1])),
            vec![dictionary, run],
        )
        .unwrap();
        let input: ArrayRef = Arc::new(union.slice(1, 2));
        let before = input.to_data();

        let output = retag_timestamp_array(&input, &Arc::from("UTC")).unwrap();
        let after = output.to_data();

        after.validate_full().unwrap();
        assert_physical_buffers_shared(&before, &after);
        let union = output.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_ids().as_ref(), &[9, 3]);
        assert_eq!(
            union
                .child(3)
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .unwrap()
                .values()
                .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
        assert_eq!(
            union
                .child(9)
                .as_any()
                .downcast_ref::<RunArray<Int16Type>>()
                .unwrap()
                .values()
                .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
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
