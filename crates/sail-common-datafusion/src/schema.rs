use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::Result;

use crate::array::record_batch::{
    cast_array_positionally_recursively, cast_record_batch_positionally,
};

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
) -> Result<RecordBatch> {
    let schema = Arc::new(normalize_schema(
        batch.schema().as_ref(),
        utf8_and_binary_to_large,
    ));
    if schema.as_ref() == batch.schema().as_ref() {
        Ok(batch.clone())
    } else {
        cast_record_batch_positionally(batch.clone(), schema)
    }
}

pub fn normalize_array(array: &ArrayRef, utf8_and_binary_to_large: bool) -> Result<ArrayRef> {
    cast_array_positionally_recursively(
        array,
        &normalize_data_type(array.data_type(), utf8_and_binary_to_large),
    )
}

pub fn normalize_schema(schema: &Schema, utf8_and_binary_to_large: bool) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| normalize_field_with_options(field, utf8_and_binary_to_large))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

pub fn normalize_field_with_options(field: &FieldRef, utf8_and_binary_to_large: bool) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(normalize_data_type(
        field.data_type(),
        utf8_and_binary_to_large,
    )))
}

pub fn normalize_data_type(data_type: &DataType, utf8_and_binary_to_large: bool) -> DataType {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            if utf8_and_binary_to_large =>
        {
            DataType::LargeBinary
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if utf8_and_binary_to_large => {
            DataType::LargeUtf8
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::List(field) => DataType::List(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
        )),
        DataType::ListView(field) => DataType::List(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
        )),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            normalize_field_with_options(field, utf8_and_binary_to_large),
            *size,
        ),
        DataType::LargeList(field) => DataType::LargeList(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
        )),
        DataType::LargeListView(field) => DataType::LargeList(normalize_field_with_options(
            field,
            utf8_and_binary_to_large,
        )),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| normalize_field_with_options(field, utf8_and_binary_to_large))
                .collect(),
        ),
        DataType::Dictionary(_, value) => normalize_data_type(value, utf8_and_binary_to_large),
        DataType::Map(field, sorted) => DataType::Map(
            normalize_field_with_options(field, utf8_and_binary_to_large),
            *sorted,
        ),
        DataType::RunEndEncoded(_, values) => {
            normalize_data_type(values.data_type(), utf8_and_binary_to_large)
        }
        _ => data_type.clone(),
    }
}
