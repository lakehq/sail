use log::debug;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;

mod common;
mod common_macros;
mod common_union;
mod json_as_text;
mod json_contains;
mod json_get;
mod json_get_array;
mod json_get_bool;
mod json_get_float;
mod json_get_int;
mod json_get_json;
mod json_get_str;
mod json_length;
mod json_object_keys;
mod rewrite;

pub use common_union::{JsonUnionEncoder, JsonUnionValue, JSON_UNION_DATA_TYPE};

pub mod functions {
    pub use crate::json_as_text::json_as_text;
    pub use crate::json_contains::json_contains;
    pub use crate::json_get::json_get;
    pub use crate::json_get_array::json_get_array;
    pub use crate::json_get_bool::json_get_bool;
    pub use crate::json_get_float::json_get_float;
    pub use crate::json_get_int::json_get_int;
    pub use crate::json_get_json::json_get_json;
    pub use crate::json_get_str::json_get_str;
    pub use crate::json_length::json_length;
    pub use crate::json_object_keys::json_object_keys;
}

pub mod udfs {
    pub use crate::json_as_text::json_as_text_udf;
    pub use crate::json_contains::json_contains_udf;
    pub use crate::json_get::json_get_udf;
    pub use crate::json_get_array::json_get_array_udf;
    pub use crate::json_get_bool::json_get_bool_udf;
    pub use crate::json_get_float::json_get_float_udf;
    pub use crate::json_get_int::json_get_int_udf;
    pub use crate::json_get_json::json_get_json_udf;
    pub use crate::json_get_str::json_get_str_udf;
    pub use crate::json_length::json_length_udf;
    pub use crate::json_object_keys::json_object_keys_udf;
}

/// Register all JSON UDFs, and [`rewrite::JsonFunctionRewriter`] with the provided [`FunctionRegistry`].
///
/// # Arguments
///
/// * `registry`: `FunctionRegistry` to register the UDFs
///
/// # Errors
///
/// Returns an error if the UDFs cannot be registered or if the rewriter cannot be registered.
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        json_get::json_get_udf(),
        json_get_bool::json_get_bool_udf(),
        json_get_float::json_get_float_udf(),
        json_get_int::json_get_int_udf(),
        json_get_json::json_get_json_udf(),
        json_get_array::json_get_array_udf(),
        json_as_text::json_as_text_udf(),
        json_get_str::json_get_str_udf(),
        json_contains::json_contains_udf(),
        json_length::json_length_udf(),
        json_object_keys::json_object_keys_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    registry.register_function_rewrite(Arc::new(rewrite::JsonFunctionRewriter))?;
    registry.register_expr_planner(Arc::new(rewrite::JsonExprPlanner))?;

    Ok(())
}
