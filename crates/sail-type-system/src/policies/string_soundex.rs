//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    match &arg_types[0] {
        DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
        _ => Ok(DataType::Utf8),
    }
}
