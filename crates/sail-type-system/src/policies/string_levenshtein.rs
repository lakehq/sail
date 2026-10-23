//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::type_coercion::binary::{binary_to_string_coercion, string_coercion};

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
        .or_else(|| binary_to_string_coercion(&arg_types[0], &arg_types[1]))
    {
        match coercion_data_type {
            DataType::LargeUtf8 => Ok(DataType::Int64),
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Int32),
            other => exec_err!("levenshtein requires Utf8, LargeUtf8 or Utf8View, got {other}"),
        }
    } else {
        exec_err!("Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View")
    }
}
