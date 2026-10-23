//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    match &arg_types[0] {
        DataType::List(_) | DataType::LargeList(_) => Ok(arg_types[0].clone()),
        DataType::Null => Ok(DataType::Null),
        _ => plan_err!(
            "spark_array_compact can only accept List or LargeList, got {:?}",
            arg_types[0]
        ),
    }
}
