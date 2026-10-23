//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

use super::vector_typing::is_float_vector;

pub fn vector_cosine_similarity_return_type(arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 2 || !arg_types.iter().all(is_float_vector) {
        return plan_err!(
            "vector_cosine_similarity expects two ARRAY<FLOAT> arguments, got {arg_types:?}"
        );
    }
    Ok(DataType::Float32)
}
