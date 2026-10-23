//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

use super::vector_typing::is_float_vector;

pub fn vector_norm_return_type(arg_types: &[DataType]) -> Result<DataType> {
    let valid = match arg_types {
        [vector] => is_float_vector(vector),
        [vector, degree] => is_float_vector(vector) && degree == &DataType::Float32,
        _ => false,
    };
    if !valid {
        return plan_err!(
            "vector_norm expects an ARRAY<FLOAT> argument and an optional FLOAT degree, got {arg_types:?}"
        );
    }
    Ok(DataType::Float32)
}
