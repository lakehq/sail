use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, PrimitiveArray, RecordBatch,
    RecordBatchOptions, StructArray, new_null_array,
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

fn normalize_spark_arrow_output_field(
    field: &FieldRef,
    expand_views: bool,
    use_large_var_types: bool,
) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(normalize_spark_arrow_output_data_type(
                field.data_type(),
                expand_views,
                use_large_var_types,
            )),
    )
}

fn normalize_spark_arrow_list_element(
    field: &FieldRef,
    expand_views: bool,
    use_large_var_types: bool,
) -> FieldRef {
    Arc::new(
        normalize_spark_arrow_output_field(field, expand_views, use_large_var_types)
            .as_ref()
            .clone()
            .with_name("element"),
    )
}

fn normalize_spark_arrow_map_entries(
    field: &FieldRef,
    expand_views: bool,
    use_large_var_types: bool,
) -> FieldRef {
    let DataType::Struct(fields) = field.data_type() else {
        return normalize_spark_arrow_output_field(field, expand_views, use_large_var_types);
    };
    let [key, value] = fields.as_ref() else {
        return normalize_spark_arrow_output_field(field, expand_views, use_large_var_types);
    };
    let key = Arc::new(
        normalize_spark_arrow_output_field(key, expand_views, use_large_var_types)
            .as_ref()
            .clone()
            .with_name("key")
            .with_nullable(false),
    );
    let value = Arc::new(
        normalize_spark_arrow_output_field(value, expand_views, use_large_var_types)
            .as_ref()
            .clone()
            .with_name("value"),
    );
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_name("entries")
            .with_nullable(false)
            .with_data_type(DataType::Struct(vec![key, value].into())),
    )
}

fn normalize_spark_arrow_output_data_type(
    data_type: &DataType,
    expand_views: bool,
    use_large_var_types: bool,
) -> DataType {
    match data_type {
        DataType::BinaryView if !expand_views => DataType::BinaryView,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
            if use_large_var_types =>
        {
            DataType::LargeBinary
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => DataType::Binary,
        DataType::Utf8View if !expand_views => DataType::Utf8View,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if use_large_var_types => {
            DataType::LargeUtf8
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field) => DataType::List(normalize_spark_arrow_list_element(
            field,
            expand_views,
            use_large_var_types,
        )),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    normalize_spark_arrow_output_field(field, expand_views, use_large_var_types)
                })
                .collect(),
        ),
        DataType::Dictionary(_, value) => {
            normalize_spark_arrow_output_data_type(value, expand_views, use_large_var_types)
        }
        DataType::Map(field, _) => DataType::Map(
            normalize_spark_arrow_map_entries(field, expand_views, use_large_var_types),
            false,
        ),
        DataType::RunEndEncoded(_, values) => normalize_spark_arrow_output_data_type(
            values.data_type(),
            expand_views,
            use_large_var_types,
        ),
        _ => data_type.clone(),
    }
}

/// Normalizes Arrow types at a boundary consumed by Spark.
///
/// Spark expects offset-based string, binary, and list arrays. The requested string and binary
/// width is applied recursively while Arrow-only encodings are materialized.
pub fn normalize_spark_arrow_data_type(
    data_type: &DataType,
    use_large_var_types: bool,
) -> DataType {
    normalize_spark_arrow_output_data_type(data_type, true, use_large_var_types)
}

/// Normalizes a Spark Connect Arrow output schema according to an action-scoped policy.
///
/// View arrays remain views when `expand_views` is disabled. Existing offset-based string and
/// binary arrays still use the requested regular or large width, including in nested types.
pub fn normalize_spark_arrow_output_schema(
    schema: &Schema,
    expand_views: bool,
    use_large_var_types: bool,
) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                normalize_spark_arrow_output_field(field, expand_views, use_large_var_types)
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

pub fn normalize_spark_arrow_schema(schema: &Schema, use_large_var_types: bool) -> Schema {
    normalize_spark_arrow_output_schema(schema, true, use_large_var_types)
}

pub fn normalize_spark_arrow_array(
    array: &ArrayRef,
    use_large_var_types: bool,
) -> Result<ArrayRef> {
    cast_array_recursively(
        array,
        &normalize_spark_arrow_data_type(array.data_type(), use_large_var_types),
    )
}

pub fn normalize_spark_arrow_record_batch(
    batch: &RecordBatch,
    use_large_var_types: bool,
) -> Result<RecordBatch> {
    let schema = Arc::new(normalize_spark_arrow_schema(
        batch.schema().as_ref(),
        use_large_var_types,
    ));
    if schema.as_ref() == batch.schema().as_ref() {
        Ok(batch.clone())
    } else {
        cast_record_batch_positionally(batch.clone(), schema)
    }
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

fn cast_array_positionally_recursively(src: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    let src_type = src.data_type();
    if src_type == target_type {
        return Ok(src.clone());
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
    // Arrow map key/value fields are positional structural wrappers.
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
    use std::collections::HashMap;

    use datafusion::arrow::array::{
        ArrayRef, BinaryViewArray, BinaryViewBuilder, Int32Array, ListBuilder, MapBuilder,
        StringViewArray, StringViewBuilder, StructArray,
    };
    use datafusion::arrow::datatypes::{Field, Fields};

    use super::*;

    fn make_struct_array(fields: Vec<Field>, columns: Vec<ArrayRef>) -> StructArray {
        let field_refs_vec: Vec<FieldRef> = fields.into_iter().map(Arc::new).collect();
        let field_refs: Fields = field_refs_vec.into();
        StructArray::new(field_refs, columns, None)
    }

    #[test]
    fn normalize_spark_arrow_record_batch_recursively() {
        let mut aliases = ListBuilder::new(StringViewBuilder::new());
        aliases.values().append_value("left");
        aliases.values().append_value("right");
        aliases.append(true);
        aliases.append(false);
        let aliases = aliases.finish();
        let details_fields = vec![
            Field::new("name", DataType::Utf8View, true),
            Field::new("aliases", aliases.data_type().clone(), true),
        ]
        .into();
        let details = StructArray::new(
            details_fields,
            vec![
                Arc::new(StringViewArray::from(vec![Some("nested"), None])),
                Arc::new(aliases),
            ],
            None,
        );
        let mut tags = MapBuilder::new(None, StringViewBuilder::new(), BinaryViewBuilder::new());
        tags.keys().append_value("first");
        tags.values().append_value(b"value");
        tags.append(true).unwrap();
        tags.append(false).unwrap();
        let tags = tags.finish();
        let text_field = Field::new("text", DataType::Utf8View, true)
            .with_metadata(HashMap::from([("kind".to_string(), "label".to_string())]));
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                text_field,
                Field::new("bytes", DataType::BinaryView, true),
                Field::new("details", details.data_type().clone(), true),
                Field::new("tags", tags.data_type().clone(), true),
            ],
            HashMap::from([("owner".to_string(), "spark".to_string())]),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec![Some("top-level"), None])),
                Arc::new(BinaryViewArray::from(vec![Some(b"bytes".as_slice()), None])),
                Arc::new(details),
                Arc::new(tags),
            ],
        )
        .unwrap();

        for (use_large_var_types, string_type, binary_type) in [
            (false, DataType::Utf8, DataType::Binary),
            (true, DataType::LargeUtf8, DataType::LargeBinary),
        ] {
            let materialized =
                normalize_spark_arrow_record_batch(&batch, use_large_var_types).unwrap();
            let schema = materialized.schema();
            assert_eq!(schema.metadata().get("owner"), Some(&"spark".to_string()));
            assert_eq!(
                schema.field(0).metadata().get("kind"),
                Some(&"label".to_string())
            );
            assert_eq!(schema.field(0).data_type(), &string_type);
            assert_eq!(schema.field(1).data_type(), &binary_type);
            assert_eq!(materialized.column(0).null_count(), 1);
            assert_eq!(materialized.column(1).null_count(), 1);
            assert!(matches!(schema.field(2).data_type(), DataType::Struct(_)));
            if let DataType::Struct(fields) = schema.field(2).data_type() {
                assert_eq!(fields[0].data_type(), &string_type);
                assert!(matches!(fields[1].data_type(), DataType::List(_)));
                if let DataType::List(element) = fields[1].data_type() {
                    assert_eq!(element.name(), "element");
                    assert_eq!(element.data_type(), &string_type);
                }
            }
            assert!(matches!(schema.field(3).data_type(), DataType::Map(_, _)));
            if let DataType::Map(entries, sorted) = schema.field(3).data_type() {
                assert!(!sorted);
                assert_eq!(entries.name(), "entries");
                assert!(matches!(entries.data_type(), DataType::Struct(_)));
                if let DataType::Struct(fields) = entries.data_type() {
                    assert_eq!(fields[0].name(), "key");
                    assert_eq!(fields[0].data_type(), &string_type);
                    assert_eq!(fields[1].name(), "value");
                    assert_eq!(fields[1].data_type(), &binary_type);
                }
            }
        }
    }

    #[test]
    fn normalize_spark_arrow_type_canonicalizes_spark_families() {
        for use_large_var_types in [false, true] {
            let binary_type = if use_large_var_types {
                DataType::LargeBinary
            } else {
                DataType::Binary
            };
            assert_eq!(
                normalize_spark_arrow_data_type(&DataType::FixedSizeBinary(4), use_large_var_types),
                binary_type
            );
            let dictionary =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View));
            assert_eq!(
                normalize_spark_arrow_data_type(&dictionary, use_large_var_types),
                if use_large_var_types {
                    DataType::LargeUtf8
                } else {
                    DataType::Utf8
                }
            );
            let list =
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8View, true)), 2);
            let normalized = normalize_spark_arrow_data_type(&list, use_large_var_types);
            assert!(matches!(normalized, DataType::List(_)));
            if let DataType::List(element) = normalized {
                assert_eq!(element.name(), "element");
            }
        }
    }

    #[test]
    fn normalize_spark_arrow_output_schema_applies_view_and_width_policies_independently() {
        let map_entries = Arc::new(Field::new(
            "source_entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("source_key", DataType::Utf8View, false)),
                    Arc::new(Field::new("source_value", DataType::LargeBinary, true)),
                ]
                .into(),
            ),
            false,
        ));
        let schema = Schema::new(vec![
            Field::new("view_text", DataType::Utf8View, true),
            Field::new("view_bytes", DataType::BinaryView, true),
            Field::new("offset_text", DataType::LargeUtf8, true),
            Field::new("offset_bytes", DataType::Binary, true),
            Field::new(
                "nested",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new(
                            "items",
                            DataType::List(Arc::new(Field::new(
                                "source_item",
                                DataType::Utf8View,
                                true,
                            ))),
                            true,
                        )),
                        Arc::new(Field::new("lookup", DataType::Map(map_entries, true), true)),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);

        for (expand_views, use_large_var_types) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let normalized =
                normalize_spark_arrow_output_schema(&schema, expand_views, use_large_var_types);
            let offset_text = if use_large_var_types {
                DataType::LargeUtf8
            } else {
                DataType::Utf8
            };
            let offset_binary = if use_large_var_types {
                DataType::LargeBinary
            } else {
                DataType::Binary
            };
            let view_text = if expand_views {
                offset_text.clone()
            } else {
                DataType::Utf8View
            };
            let view_binary = if expand_views {
                offset_binary.clone()
            } else {
                DataType::BinaryView
            };

            assert_eq!(normalized.field(0).data_type(), &view_text);
            assert_eq!(normalized.field(1).data_type(), &view_binary);
            assert_eq!(normalized.field(2).data_type(), &offset_text);
            assert_eq!(normalized.field(3).data_type(), &offset_binary);

            assert!(matches!(
                normalized.field(4).data_type(),
                DataType::Struct(_)
            ));
            if let DataType::Struct(nested_fields) = normalized.field(4).data_type() {
                assert!(matches!(nested_fields[0].data_type(), DataType::List(_)));
                if let DataType::List(element) = nested_fields[0].data_type() {
                    assert_eq!(element.name(), "element");
                    assert_eq!(element.data_type(), &view_text);
                }
                assert!(matches!(nested_fields[1].data_type(), DataType::Map(_, _)));
                if let DataType::Map(entries, sorted) = nested_fields[1].data_type() {
                    assert!(!sorted);
                    assert_eq!(entries.name(), "entries");
                    assert!(matches!(entries.data_type(), DataType::Struct(_)));
                    if let DataType::Struct(entry_fields) = entries.data_type() {
                        assert_eq!(entry_fields[0].name(), "key");
                        assert_eq!(entry_fields[0].data_type(), &view_text);
                        assert_eq!(entry_fields[1].name(), "value");
                        assert_eq!(entry_fields[1].data_type(), &offset_binary);
                    }
                }
            }
        }
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
