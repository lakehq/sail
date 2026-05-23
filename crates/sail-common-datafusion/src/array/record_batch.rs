use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, LargeListArray, ListArray, MapArray, PrimitiveArray,
    RecordBatch, RecordBatchOptions, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion_common::{DataFusionError, Result};

pub fn cast_record_batch_positionally(
    batch: RecordBatch,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let fields = schema.fields();
    let columns = batch.columns();
    let columns = fields
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            let data_type = field.data_type();
            let column = cast_array_recursively(column, data_type, StructFieldMatching::Position)?;
            Ok(column)
        })
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

        let casted = cast_array_recursively(src, field.data_type(), StructFieldMatching::Name)?;
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

#[derive(Debug, Clone, Copy)]
enum StructFieldMatching {
    Name,
    Position,
}

fn cast_array_recursively(
    src: &ArrayRef,
    target_type: &DataType,
    struct_field_matching: StructFieldMatching,
) -> Result<ArrayRef> {
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
            cast_struct_array(src, target_fields, struct_field_matching)
        }
        (DataType::List(_), DataType::List(target_field)) => {
            cast_list_array(src, target_field, struct_field_matching)
        }
        (DataType::LargeList(_), DataType::LargeList(target_field)) => {
            cast_large_list_array(src, target_field, struct_field_matching)
        }
        (DataType::Map(_, _), DataType::Map(target_field, sorted)) => {
            cast_map_array(src, target_field, *sorted, struct_field_matching)
        }
        _ => {
            let casted = cast(src, target_type)?;
            Ok(casted)
        }
    }
}

fn cast_struct_array(
    src: &ArrayRef,
    target_fields: &Fields,
    struct_field_matching: StructFieldMatching,
) -> Result<ArrayRef> {
    let struct_array = src.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to StructArray".to_string())
    })?;

    let mut new_children: Vec<ArrayRef> = Vec::with_capacity(target_fields.len());
    for (index, target_field) in target_fields.iter().enumerate() {
        let child = match struct_field_matching {
            StructFieldMatching::Name => struct_array.column_by_name(target_field.name()),
            StructFieldMatching::Position => struct_array.columns().get(index),
        };
        if let Some(child) = child {
            new_children.push(cast_array_recursively(
                child,
                target_field.data_type(),
                struct_field_matching,
            )?);
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

fn cast_list_array(
    src: &ArrayRef,
    target_field: &FieldRef,
    struct_field_matching: StructFieldMatching,
) -> Result<ArrayRef> {
    let list_array = src.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to ListArray".to_string())
    })?;

    let values = cast_array_recursively(
        list_array.values(),
        target_field.data_type(),
        struct_field_matching,
    )?;
    let new_list = ListArray::try_new(
        target_field.clone(),
        list_array.offsets().clone(),
        values,
        list_array.nulls().cloned(),
    )?;
    Ok(Arc::new(new_list))
}

fn cast_large_list_array(
    src: &ArrayRef,
    target_field: &FieldRef,
    struct_field_matching: StructFieldMatching,
) -> Result<ArrayRef> {
    let list_array = src
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast array to LargeListArray".to_string())
        })?;

    let values = cast_array_recursively(
        list_array.values(),
        target_field.data_type(),
        struct_field_matching,
    )?;
    let new_list = LargeListArray::try_new(
        target_field.clone(),
        list_array.offsets().clone(),
        values,
        list_array.nulls().cloned(),
    )?;
    Ok(Arc::new(new_list))
}

fn cast_map_array(
    src: &ArrayRef,
    target_field: &FieldRef,
    sorted: bool,
    struct_field_matching: StructFieldMatching,
) -> Result<ArrayRef> {
    let map_array = src.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        DataFusionError::Internal("Failed to downcast array to MapArray".to_string())
    })?;
    let entries: ArrayRef = Arc::new(map_array.entries().clone());
    let cast_entries =
        cast_array_recursively(&entries, target_field.data_type(), struct_field_matching)?;
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

pub fn record_batch_with_deduplicated_nested_fields(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = deduplicate_nested_fields(batch.schema().as_ref())?;
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| array_with_data_type(column, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )?)
}

fn deduplicate_nested_fields(schema: &Schema) -> Result<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(Arc::new(
                Field::new(
                    field.name(),
                    deduplicate_data_type(field.data_type())?,
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            ))
        })
        .collect::<Result<Vec<FieldRef>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn deduplicate_data_type(data_type: &DataType) -> Result<DataType> {
    match data_type {
        DataType::Struct(fields) => {
            let names = fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>();
            let names = deduplicate_names(&names);
            let fields = fields
                .iter()
                .zip(names)
                .map(|(field, name)| {
                    Ok(Arc::new(
                        Field::new(
                            name,
                            deduplicate_data_type(field.data_type())?,
                            field.is_nullable(),
                        )
                        .with_metadata(field.metadata().clone()),
                    ))
                })
                .collect::<Result<Vec<FieldRef>>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        DataType::List(field) => Ok(DataType::List(Arc::new(
            Field::new(
                field.name(),
                deduplicate_data_type(field.data_type())?,
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
        ))),
        DataType::LargeList(field) => Ok(DataType::LargeList(Arc::new(
            Field::new(
                field.name(),
                deduplicate_data_type(field.data_type())?,
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
        ))),
        DataType::Map(field, sorted) => Ok(DataType::Map(
            Arc::new(
                Field::new(
                    field.name(),
                    deduplicate_data_type(field.data_type())?,
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            ),
            *sorted,
        )),
        _ => Ok(data_type.clone()),
    }
}

/// Deduplicates field names using Spark's Arrow conversion convention.
///
/// Names that are unique are preserved. Duplicate names get zero-based suffixes
/// in their original order (for example, `x`, `x` becomes `x_0`, `x_1`).
fn deduplicate_names(names: &[&str]) -> Vec<String> {
    let mut counts = std::collections::HashMap::<&str, usize>::new();
    for name in names {
        *counts.entry(name).or_default() += 1;
    }
    let mut indices = std::collections::HashMap::<&str, usize>::new();
    names
        .iter()
        .map(|name| {
            if counts[name] > 1 {
                let index = indices.entry(name).or_default();
                let out = format!("{}_{}", name, *index);
                *index += 1;
                out
            } else {
                name.to_string()
            }
        })
        .collect()
}

/// Recursively rebuilds an array so that its nested Arrow data type matches the target.
///
/// This preserves values while updating nested field names/types in structs, lists, and maps.
fn array_with_data_type(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    match (array.data_type(), target_type) {
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Failed to downcast array to StructArray".to_string())
                })?;
            let columns = struct_array
                .columns()
                .iter()
                .zip(target_fields.iter())
                .map(|(column, field)| array_with_data_type(column, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                target_fields.clone(),
                columns,
                struct_array.nulls().cloned(),
            )?))
        }
        (DataType::List(_), DataType::List(target_field)) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Internal("Failed to downcast array to ListArray".to_string())
            })?;
            Ok(Arc::new(ListArray::try_new(
                target_field.clone(),
                list_array.offsets().clone(),
                array_with_data_type(list_array.values(), target_field.data_type())?,
                list_array.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(target_field)) => {
            let list_array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Failed to downcast array to LargeListArray".to_string(),
                    )
                })?;
            Ok(Arc::new(LargeListArray::try_new(
                target_field.clone(),
                list_array.offsets().clone(),
                array_with_data_type(list_array.values(), target_field.data_type())?,
                list_array.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, _), DataType::Map(target_field, sorted)) => {
            let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                DataFusionError::Internal("Failed to downcast array to MapArray".to_string())
            })?;
            let entries: ArrayRef = Arc::new(map_array.entries().clone());
            let entries = array_with_data_type(&entries, target_field.data_type())?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Map entries must be struct arrays".to_string())
                })?
                .clone();
            Ok(Arc::new(MapArray::try_new(
                target_field.clone(),
                map_array.offsets().clone(),
                entries,
                map_array.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => Ok(cast(array, target_type)?),
    }
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

    #[test]
    fn cast_record_batch_positionally_maps_nested_struct_fields_by_position() {
        let first_values = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let second_values = Arc::new(Int32Array::from(vec![Some(10), Some(20)]));
        let struct_array = Arc::new(make_struct_array(
            vec![
                Field::new("x_0", DataType::Int32, true),
                Field::new("x_1", DataType::Int32, true),
            ],
            vec![first_values.clone(), second_values.clone()],
        ));

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("x_0", DataType::Int32, true)),
                    Arc::new(Field::new("x_1", DataType::Int32, true)),
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
                    Arc::new(Field::new("x", DataType::Int32, true)),
                    Arc::new(Field::new("x", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        )]));

        let casted = cast_record_batch_positionally(batch, target_schema).unwrap();
        let payload = casted
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            payload
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            first_values.values()
        );
        assert_eq!(
            payload
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            second_values.values()
        );
    }

    #[test]
    fn record_batch_with_deduplicated_nested_fields_renames_nested_struct_fields() {
        let first_values = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let second_values = Arc::new(Int32Array::from(vec![Some(10), Some(20)]));
        let struct_array = Arc::new(make_struct_array(
            vec![
                Field::new("x", DataType::Int32, true),
                Field::new("x", DataType::Int32, true),
            ],
            vec![first_values.clone(), second_values.clone()],
        ));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("x", DataType::Int32, true)),
                    Arc::new(Field::new("x", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        )]));
        let batch = RecordBatch::try_new(input_schema, vec![struct_array]).unwrap();

        let batch = record_batch_with_deduplicated_nested_fields(&batch).unwrap();
        let schema = batch.schema();
        let DataType::Struct(fields) = schema.field(0).data_type() else {
            panic!("expected struct");
        };
        assert_eq!(fields[0].name(), "x_0");
        assert_eq!(fields[1].name(), "x_1");
        let payload = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            payload
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            first_values.values()
        );
        assert_eq!(
            payload
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            second_values.values()
        );
    }
}
