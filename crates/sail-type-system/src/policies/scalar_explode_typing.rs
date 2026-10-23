//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

pub fn explode_return_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    match &arg_types {
        &[DataType::List(f)]
        | &[DataType::LargeList(f)]
        | &[DataType::FixedSizeList(f, _)]
        | &[DataType::Map(f, _)] => Ok(f.data_type().clone()),
        _ => plan_err!("{} should only be called with a list or map", name),
    }
}
