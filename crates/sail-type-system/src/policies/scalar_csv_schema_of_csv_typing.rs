//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::Result;

pub fn spark_schema_of_csv_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8)
}
