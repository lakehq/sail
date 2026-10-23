//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;
use parquet_variant_compute::VariantType;
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

use crate::error::invalid_arg_count_exec_err;

pub fn spark_variant_explode_udf_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(variant_explode_return_type())
}

pub fn spark_variant_explode_udf_return_field_from_args(
    name: &str,
    _args: ReturnFieldArgs,
) -> Result<Arc<Field>> {
    Ok(Arc::new(Field::new(
        name,
        variant_explode_return_type(),
        true,
    )))
}

pub fn spark_variant_explode_udf_coerce_types(
    name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(invalid_arg_count_exec_err(name, (1, 1), arg_types.len()));
    }
    Ok(vec![arg_types[0].clone()])
}

pub fn variant_explode_return_type() -> DataType {
    DataType::List(variant_explode_item_field())
}

pub fn variant_explode_item_field() -> Arc<Field> {
    Arc::new(Field::new_struct("item", variant_explode_fields(), true))
}

pub fn variant_explode_fields() -> Fields {
    Fields::from(vec![
        Field::new("pos", DataType::Int32, false),
        Field::new("key", DataType::Utf8, true),
        variant_explode_value_field(),
    ])
}

pub fn variant_explode_value_field() -> Field {
    Field::new(
        VARIANT_VALUE_FIELD_NAME,
        DataType::Struct(Fields::from(vec![
            Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, false),
            variant_metadata_field(DataType::Binary, false),
        ])),
        true,
    )
    .with_extension_type(VariantType)
}
