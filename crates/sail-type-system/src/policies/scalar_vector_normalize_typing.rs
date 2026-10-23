//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::{Result, plan_err};

use super::vector_typing::is_float_vector;

pub fn vector_normalize_return_type(arg_types: &[DataType]) -> Result<DataType> {
    // Spark 4.2 only accepts `ARRAY<FLOAT>` with an optional `FLOAT` degree and does not
    // implicitly cast other numeric types, so wider element types are rejected here as well.
    let valid = match arg_types {
        [vector] => is_float_vector(vector),
        [vector, degree] => is_float_vector(vector) && degree == &DataType::Float32,
        _ => false,
    };
    if !valid {
        return plan_err!(
            "vector_normalize expects an ARRAY<FLOAT> argument and an optional FLOAT degree, got {arg_types:?}"
        );
    }
    let field = Arc::new(Field::new_list_field(DataType::Float32, true));
    match &arg_types[0] {
        DataType::LargeList(_) => Ok(DataType::LargeList(field)),
        _ => Ok(DataType::List(field)),
    }
}
