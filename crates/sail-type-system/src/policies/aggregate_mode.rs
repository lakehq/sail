//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    Ok(arg_types[0].clone())
}
