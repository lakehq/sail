//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;
use parquet_variant_compute::VariantType;
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

pub fn spark_to_variant_object_udf_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Struct(Fields::from(vec![
        Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, false),
        variant_metadata_field(DataType::Binary, false),
    ])))
}

pub fn spark_to_variant_object_udf_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<Arc<Field>> {
    let data_type = spark_to_variant_object_udf_return_type(
        args.arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>()
            .as_slice(),
    )?;
    Ok(Arc::new(
        Field::new(name, data_type, true).with_extension_type(VariantType),
    ))
}

pub fn spark_to_variant_object_udf_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(invalid_arg_count_exec_err(
            "to_variant_object",
            (1, 1),
            arg_types.len(),
        ));
    }
    // Reject types containing Null/VOID (e.g. ARRAY<VOID>, MAP<VOID, VOID>)
    // Spark cannot cast these to VARIANT
    if contains_void_type(&arg_types[0]) {
        return Err(unsupported_data_type_exec_err(
            "to_variant_object",
            "non-void complex type",
            &arg_types[0],
        ));
    }
    Ok(vec![arg_types[0].clone()])
}

pub fn contains_void_type(dt: &DataType) -> bool {
    match dt {
        DataType::Null => true,
        DataType::List(f) | DataType::LargeList(f) => contains_void_type(f.data_type()),
        DataType::Map(f, _) => {
            if let DataType::Struct(fields) = f.data_type() {
                fields
                    .iter()
                    .any(|field| contains_void_type(field.data_type()))
            } else {
                false
            }
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_void_type(field.data_type())),
        _ => false,
    }
}
