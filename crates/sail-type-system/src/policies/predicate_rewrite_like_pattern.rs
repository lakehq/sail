//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::{Result, exec_err};

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    match &arg_types[0] {
        DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
        DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
        other => exec_err!("Unsupported data type {other:?} for function rewrite_like_pattern"),
    }
}
