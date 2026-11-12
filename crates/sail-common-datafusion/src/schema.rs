use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, RecordBatch, RecordBatchOptions, make_array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::Result;

use crate::array::record_batch::{
    cast_array_positionally_recursively, cast_record_batch_positionally,
};
use crate::uuid::UUID_EXTENSION_NAME;

/// Spark exposes UUID storage as BinaryType, whose Python values must be bytes,
/// not the uuid.UUID objects produced by Arrow's canonical extension decoder.
/// Adapt only the outbound field, retaining storage and unrelated metadata.
pub fn spark_arrow_field(field: &Field) -> Field {
    let mut field = field
        .clone()
        .with_data_type(spark_arrow_data_type(field.data_type()));
    if field.extension_type_name() == Some(UUID_EXTENSION_NAME) {
        field.metadata_mut().remove(EXTENSION_TYPE_NAME_KEY);
        field.metadata_mut().remove(EXTENSION_TYPE_METADATA_KEY);
    }
    field
}

pub fn spark_arrow_data_type(data_type: &DataType) -> DataType {
    let child = |field: &FieldRef| Arc::new(spark_arrow_field(field));
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(child).collect()),
        DataType::List(field) => DataType::List(child(field)),
        DataType::LargeList(field) => DataType::LargeList(child(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(child(field), *size),
        DataType::ListView(field) => DataType::ListView(child(field)),
        DataType::LargeListView(field) => DataType::LargeListView(child(field)),
        DataType::Map(field, sorted) => DataType::Map(child(field), *sorted),
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(id, field)| (id, child(field)))
                .collect(),
            *mode,
        ),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(spark_arrow_data_type(value)))
        }
        DataType::RunEndEncoded(run_ends, values) => {
            DataType::RunEndEncoded(child(run_ends), child(values))
        }
        _ => data_type.clone(),
    }
}

pub fn spark_arrow_schema(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| spark_arrow_field(field))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

/// Remove Spark-incompatible UUID annotations without casting the storage.
/// Metadata-only changes must also work for types such as Union that Arrow
/// cannot cast to an otherwise identical type with different field metadata.
pub fn spark_arrow_array(array: &ArrayRef) -> Result<ArrayRef> {
    let data_type = spark_arrow_data_type(array.data_type());
    if &data_type == array.data_type() {
        return Ok(array.clone());
    }
    Ok(make_array(spark_arrow_array_data(
        array.to_data(),
        data_type,
    )?))
}

fn spark_arrow_array_data(data: ArrayData, data_type: DataType) -> Result<ArrayData> {
    if &data_type == data.data_type() {
        return Ok(data);
    }
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            spark_arrow_array_data(child.clone(), spark_arrow_data_type(child.data_type()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(data
        .into_builder()
        .data_type(data_type)
        .child_data(children)
        .build()?)
}

pub fn spark_arrow_record_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = Arc::new(spark_arrow_schema(batch.schema().as_ref()));
    if schema.as_ref() == batch.schema().as_ref() {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .map(spark_arrow_array)
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )?)
}

pub fn record_batch_has_views(batch: &RecordBatch) -> bool {
    batch
        .schema()
        .fields()
        .iter()
        .any(|field| data_type_has_views(field.data_type()))
}

fn data_type_has_views(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::ListView(_)
        | DataType::LargeListView(_) => true,

        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => data_type_has_views(field.data_type()),

        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_has_views(field.data_type())),

        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| data_type_has_views(field.data_type())),

        DataType::Dictionary(_, value_type) => data_type_has_views(value_type),

        DataType::RunEndEncoded(_, values) => data_type_has_views(values.data_type()),

        _ => false,
    }
}

pub fn normalize_record_batch(
    batch: &RecordBatch,
    utf8_and_binary_to_large: bool,
    large_to_utf8_and_binary: bool,
) -> Result<RecordBatch> {
    let schema = Arc::new(normalize_schema(
        batch.schema().as_ref(),
        utf8_and_binary_to_large,
        large_to_utf8_and_binary,
    ));
    if schema.as_ref() == batch.schema().as_ref() {
        Ok(batch.clone())
    } else {
        cast_record_batch_positionally(batch.clone(), schema)
    }
}

pub fn normalize_array(
    array: &ArrayRef,
    utf8_and_binary_to_large: bool,
    large_to_utf8_and_binary: bool,
) -> Result<ArrayRef> {
    cast_array_positionally_recursively(
        array,
        &normalize_data_type(
            array.data_type(),
            utf8_and_binary_to_large,
            large_to_utf8_and_binary,
        ),
    )
}

pub fn normalize_schema(
    schema: &Schema,
    utf8_and_binary_to_large: bool,
    large_to_utf8_and_binary: bool,
) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                normalize_field_with_options(
                    field,
                    utf8_and_binary_to_large,
                    large_to_utf8_and_binary,
                )
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

pub fn normalize_field_with_options(
    field: &FieldRef,
    utf8_and_binary_to_large: bool,
    large_to_utf8_and_binary: bool,
) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(normalize_data_type(
        field.data_type(),
        utf8_and_binary_to_large,
        large_to_utf8_and_binary,
    )))
}

pub fn normalize_data_type(
    data_type: &DataType,
    utf8_and_binary_to_large: bool,
    large_to_utf8_and_binary: bool,
) -> DataType {
    match data_type {
        DataType::Binary | DataType::BinaryView if utf8_and_binary_to_large => {
            DataType::LargeBinary
        }
        DataType::Utf8 | DataType::Utf8View if utf8_and_binary_to_large => DataType::LargeUtf8,
        DataType::LargeBinary if large_to_utf8_and_binary => DataType::Binary,
        DataType::LargeUtf8 if large_to_utf8_and_binary => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::Utf8View => DataType::Utf8,
        DataType::List(field) => DataType::List(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
            large_to_utf8_and_binary,
        )),
        DataType::ListView(field) => DataType::List(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
            large_to_utf8_and_binary,
        )),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            normalize_field_with_options(field, utf8_and_binary_to_large, large_to_utf8_and_binary),
            *size,
        ),
        DataType::LargeList(field) => DataType::LargeList(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
            large_to_utf8_and_binary,
        )),
        DataType::LargeListView(field) => DataType::LargeList(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
            large_to_utf8_and_binary,
        )),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    normalize_field_with_options(
                        field,
                        utf8_and_binary_to_large,
                        large_to_utf8_and_binary,
                    )
                })
                .collect(),
        ),
        DataType::Dictionary(_, value) => {
            normalize_data_type(value, utf8_and_binary_to_large, large_to_utf8_and_binary)
        }
        DataType::Map(field, sorted) => DataType::Map(
            normalize_field_with_options(field, utf8_and_binary_to_large, large_to_utf8_and_binary),
            *sorted,
        ),
        DataType::RunEndEncoded(_, values) => normalize_data_type(
            values.data_type(),
            utf8_and_binary_to_large,
            large_to_utf8_and_binary,
        ),
        _ => data_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::extension::Uuid;
    use datafusion::arrow::array::{
        AsArray, FixedSizeBinaryBuilder, Int32Array, ListArray, StructArray, UnionArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::UnionMode;

    use super::*;
    use crate::uuid::is_uuid_field;

    #[test]
    fn spark_arrow_metadata_is_uuid_only_and_recursive() {
        let metadata = [("custom".to_string(), "retained".to_string())].into();
        let storage = Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(metadata);
        let mut uuid = storage.clone().with_extension_type(Uuid);
        uuid.metadata_mut()
            .insert(EXTENSION_TYPE_METADATA_KEY.into(), String::new());
        assert_eq!(spark_arrow_field(&uuid), storage);
        assert!(is_uuid_field(&uuid));
        for name in ["other.extension", "uuid"] {
            let mut other = storage.clone();
            other
                .metadata_mut()
                .insert(EXTENSION_TYPE_NAME_KEY.into(), name.into());
            other
                .metadata_mut()
                .insert(EXTENSION_TYPE_METADATA_KEY.into(), "parameters".into());
            assert_eq!(spark_arrow_field(&other), other);
        }
        assert_eq!(spark_arrow_field(&storage), storage);

        let containers: &[fn(FieldRef) -> DataType] = &[
            DataType::List,
            DataType::LargeList,
            DataType::ListView,
            DataType::LargeListView,
            |field| DataType::FixedSizeList(field, 2),
            |field| DataType::Struct(vec![field].into()),
            |field| {
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![Arc::new(Field::new("key", DataType::Utf8, false)), field].into(),
                        ),
                        false,
                    )),
                    true,
                )
            },
            |field| DataType::Union([(7, field)].into_iter().collect(), UnionMode::Dense),
            |field| {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::List(field)))
            },
            |field| {
                DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int32, false)), field)
            },
        ];
        for container in containers {
            let field = Field::new("nested", container(Arc::new(uuid.clone())), false);
            let expected = field
                .clone()
                .with_data_type(container(Arc::new(storage.clone())));
            let schema =
                Schema::new_with_metadata(vec![uuid.clone(), field], storage.metadata().clone());
            let original = schema.clone();
            assert_eq!(
                spark_arrow_schema(&schema),
                Schema::new_with_metadata(
                    vec![storage.clone(), expected],
                    storage.metadata().clone(),
                )
            );
            assert_eq!(schema, original);
            assert!(is_uuid_field(schema.field(0)));
        }
    }

    #[test]
    fn spark_arrow_array_retains_nested_storage_and_internal_identity() -> Result<()> {
        let uuid = Arc::new(
            Field::new("u", DataType::FixedSizeBinary(16), true).with_extension_type(Uuid),
        );
        let mut builder = FixedSizeBinaryBuilder::new(16);
        builder.append_value([1; 16])?;
        builder.append_null();
        let values: ArrayRef = Arc::new(builder.finish());
        let children = Arc::new(StructArray::try_new(
            vec![uuid.clone()].into(),
            vec![values.clone()],
            None,
        )?);
        let element = Arc::new(Field::new("item", children.data_type().clone(), true));
        let array: ArrayRef = Arc::new(ListArray::try_new(
            element,
            OffsetBuffer::from_lengths([2, 0]),
            children,
            Some(vec![true, false].into()),
        )?);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()])?;
        let output = spark_arrow_record_batch(&batch)?;
        let converted = spark_arrow_array(&array)?;
        assert_eq!(output.column(0), &converted);
        let list = converted.as_list::<i32>();
        assert_eq!(list.offsets(), array.as_list::<i32>().offsets());
        assert_eq!(list.nulls(), array.nulls());
        let child = list.values().as_struct();
        assert!(!is_uuid_field(&child.fields()[0]));
        assert_same_storage(&array.to_data(), &converted.to_data());
        assert!(Arc::ptr_eq(&converted, &spark_arrow_array(&converted)?));
        assert!(is_uuid_field(
            &array.as_list::<i32>().values().as_struct().fields()[0]
        ));
        assert_eq!(batch.schema(), schema);
        assert_eq!(batch.column(0).data_type(), array.data_type());
        Ok(())
    }

    fn assert_same_storage(input: &ArrayData, output: &ArrayData) {
        assert_eq!(input.len(), output.len());
        assert_eq!(input.offset(), output.offset());
        assert_eq!(input.nulls(), output.nulls());
        if let (Some(input), Some(output)) = (input.nulls(), output.nulls()) {
            assert_eq!(input.offset(), output.offset());
            assert_eq!(input.buffer().as_ptr(), output.buffer().as_ptr());
        }
        assert_eq!(input.buffers().len(), output.buffers().len());
        for (input, output) in input.buffers().iter().zip(output.buffers()) {
            assert_eq!(input, output);
            assert_eq!(input.as_ptr(), output.as_ptr());
        }
        assert_eq!(input.child_data().len(), output.child_data().len());
        for (input, output) in input.child_data().iter().zip(output.child_data()) {
            assert_same_storage(input, output);
        }
    }

    #[test]
    fn spark_arrow_union_preserves_dense_and_sparse_storage() -> Result<()> {
        let uuid = Arc::new(
            Field::new("u", DataType::FixedSizeBinary(16), true).with_extension_type(Uuid),
        );
        let integer = Arc::new(Field::new("i", DataType::Int32, false));
        let mut builder = FixedSizeBinaryBuilder::new(16);
        builder.append_value([0; 16])?;
        builder.append_value([1; 16])?;
        builder.append_null();
        builder.append_value([3; 16])?;
        let values: ArrayRef = Arc::new(builder.finish());
        for offsets in [None, Some(vec![0, 1, 2, 3].into())] {
            let array: ArrayRef = Arc::new(UnionArray::try_new(
                [(7, uuid.clone()), (3, integer.clone())]
                    .into_iter()
                    .collect(),
                vec![7, 3, 7, 7].into(),
                offsets,
                vec![
                    values.clone(),
                    Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
                ],
            )?);
            for array in [array.clone(), array.slice(1, 3), array.slice(4, 0)] {
                let input_data = array.to_data();
                let converted = spark_arrow_array(&normalize_array(&array, false, true)?)?;
                converted.to_data().validate_full()?;
                assert_eq!(
                    converted.data_type(),
                    &spark_arrow_data_type(array.data_type())
                );
                assert_ne!(converted.data_type(), array.data_type());
                assert_eq!(converted.logical_nulls(), array.logical_nulls());
                assert_same_storage(&input_data, &converted.to_data());
                assert_eq!(array.to_data(), input_data);
                assert!(Arc::ptr_eq(&converted, &spark_arrow_array(&converted)?));

                let schema = Arc::new(Schema::new(vec![Field::new(
                    "union",
                    array.data_type().clone(),
                    true,
                )]));
                let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()])?;
                let output =
                    spark_arrow_record_batch(&normalize_record_batch(&batch, false, true)?)?;
                assert_eq!(output.schema().as_ref(), &spark_arrow_schema(&schema));
                assert_eq!(output.column(0), &converted);
                assert_same_storage(&input_data, &output.column(0).to_data());
                assert_eq!(batch.schema(), schema);
                let unchanged = spark_arrow_record_batch(&output)?;
                assert!(Arc::ptr_eq(&unchanged.schema(), &output.schema()));
                assert!(Arc::ptr_eq(unchanged.column(0), output.column(0)));
            }
        }
        Ok(())
    }

    #[test]
    fn spark_arrow_record_batch_retains_zero_column_rows() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(3)),
        )?;
        let output = spark_arrow_record_batch(&batch)?;
        assert_eq!(output.num_rows(), 3);
        assert_eq!(output.num_columns(), 0);
        assert!(Arc::ptr_eq(&output.schema(), &schema));
        Ok(())
    }
}
