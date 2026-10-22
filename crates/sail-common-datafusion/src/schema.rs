use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::Result;

use crate::array::record_batch::{
    cast_array_positionally_recursively, cast_record_batch_positionally,
};

pub fn normalize_field_with_options(field: &FieldRef, large_var_types: bool) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(normalize_data_type(field.data_type(), large_var_types)),
    )
}

pub fn normalize_schema(schema: &Schema, large_var_types: bool) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| normalize_field_with_options(field, large_var_types))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

pub fn normalize_array(array: &ArrayRef, large_var_types: bool) -> Result<ArrayRef> {
    cast_array_positionally_recursively(
        array,
        &normalize_data_type(array.data_type(), large_var_types),
    )
}

pub fn normalize_record_batch(batch: &RecordBatch, large_var_types: bool) -> Result<RecordBatch> {
    let schema = Arc::new(normalize_schema(batch.schema().as_ref(), large_var_types));
    if schema.as_ref() == batch.schema().as_ref() {
        Ok(batch.clone())
    } else {
        cast_record_batch_positionally(batch.clone(), schema)
    }
}

pub fn normalize_data_type(data_type: &DataType, large_var_types: bool) -> DataType {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView if large_var_types => {
            DataType::LargeBinary
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if large_var_types => {
            DataType::LargeUtf8
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::List(field) => {
            DataType::List(normalize_field_with_options(field, large_var_types))
        }
        DataType::ListView(field) => {
            DataType::List(normalize_field_with_options(field, large_var_types))
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(normalize_field_with_options(field, large_var_types), *size)
        }
        DataType::LargeList(field) => {
            DataType::LargeList(normalize_field_with_options(field, large_var_types))
        }
        DataType::LargeListView(field) => {
            DataType::LargeList(normalize_field_with_options(field, large_var_types))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| normalize_field_with_options(field, large_var_types))
                .collect(),
        ),
        DataType::Dictionary(_, value) => normalize_data_type(value, large_var_types),
        DataType::Map(field, sorted) => DataType::Map(
            normalize_field_with_options(field, large_var_types),
            *sorted,
        ),
        DataType::RunEndEncoded(_, values) => {
            normalize_data_type(values.data_type(), large_var_types)
        }
        _ => data_type.clone(),
    }
}
