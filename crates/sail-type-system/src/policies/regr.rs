use arrow_schema::DataType;
use datafusion_common::Result;
pub fn return_type(count: bool, _arg_types: &[DataType]) -> Result<DataType> {
    if count {
        Ok(DataType::UInt64)
    } else {
        Ok(DataType::Float64)
    }
}
