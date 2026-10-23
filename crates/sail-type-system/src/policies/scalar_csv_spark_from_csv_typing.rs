//! Shared function type policies.
use arrow_schema::{DataType, Fields};
use datafusion_common::Result;

pub fn spark_from_c_s_v_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    // We cannot know the final DataType result without knowing the schema input args
    Ok(DataType::Struct(Fields::empty()))
}
