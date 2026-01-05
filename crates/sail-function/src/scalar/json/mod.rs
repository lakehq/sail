// https://github.com/datafusion-contrib/datafusion-functions-json
// Copyright datafusion-functions-json contributors
// Portions Copyright (2026) LakeSail, Inc.
// Modified in 2026 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;
use log::debug;

mod common;
mod common_macros;
mod common_union;
mod json_as_text;
mod json_length;
mod json_object_keys;
mod rewrite;

pub use common_union::{JsonUnionEncoder, JsonUnionValue, JSON_UNION_DATA_TYPE};

pub mod functions {
    pub use crate::scalar::json::json_as_text::json_as_text;
    pub use crate::scalar::json::json_length::json_length;
    pub use crate::scalar::json::json_object_keys::json_object_keys;
}

pub mod udfs {
    pub use crate::scalar::json::json_as_text::json_as_text_udf;
    pub use crate::scalar::json::json_length::json_length_udf;
    pub use crate::scalar::json::json_object_keys::json_object_keys_udf;
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
        json_as_text::json_as_text_udf(),
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
