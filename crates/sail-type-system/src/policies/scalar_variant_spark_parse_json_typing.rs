//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;
use parquet_variant_compute::VariantType;
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

use crate::error::invalid_arg_count_exec_err;

pub fn spark_parse_json_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    // Use Binary instead of BinaryView for PySpark compatibility.
    // parquet-variant uses BinaryView internally (zero-copy, more efficient),
    // but PySpark doesn't support BinaryView in Arrow-to-Python conversion,
    // failing at gRPC serialization. The ideal approach would be BinaryView
    // internally and convert to Binary only at the Spark Connect serialization
    // layer, but that requires a broader refactor of the serialization path.
    Ok(DataType::Struct(Fields::from(vec![
        Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, false),
        variant_metadata_field(DataType::Binary, false),
    ])))
}

pub fn spark_parse_json_return_field_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<Arc<Field>> {
    let data_type = spark_parse_json_return_type(
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

pub fn spark_parse_json_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(invalid_arg_count_exec_err(name, (1, 1), arg_types.len()));
    }

    // Coerce all string types to Utf8View for consistency
    let coerced_type = match &arg_types[0] {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8View,
        DataType::Null => DataType::Null,
        other => {
            return datafusion_common::plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{}\" due to data type mismatch: The first parameter requires the \"STRING\" type, however the input has the type \"{}\".",
                name,
                other
            );
        }
    };

    Ok(vec![coerced_type])
}
