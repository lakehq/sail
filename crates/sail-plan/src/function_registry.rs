use std::sync::LazyLock;

use sail_common_datafusion::catalog::FunctionStatus;
use sail_common_datafusion::session::plan::FunctionRegistry;

use crate::function::{
    BUILT_IN_AGGREGATE_FUNCTIONS, BUILT_IN_GENERATOR_FUNCTIONS, BUILT_IN_SCALAR_FUNCTIONS,
    BUILT_IN_WINDOW_FUNCTIONS,
};

/// Cached, sorted, and deduplicated list of all built-in function names.
/// Computed once on first access since the underlying maps are static.
static BUILT_IN_FUNCTION_NAMES: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    let mut names: Vec<&'static str> = BUILT_IN_SCALAR_FUNCTIONS
        .keys()
        .copied()
        .chain(BUILT_IN_GENERATOR_FUNCTIONS.keys().copied())
        .chain(BUILT_IN_AGGREGATE_FUNCTIONS.keys().copied())
        .chain(BUILT_IN_WINDOW_FUNCTIONS.keys().copied())
        .collect();
    names.sort_unstable();
    names.dedup();
    names
});

/// A [`FunctionRegistry`] implementation backed by Spark built-in functions
/// that are bundled with Sail.
///
/// Built-in functions are not stored in any persistent catalog, but they are visible
/// to catalog existence and metadata queries. Consistent with Spark semantics,
/// built-in functions are reported with `is_temporary = true` so that they behave like
/// session-scoped temporary functions.
#[derive(Default)]
pub struct SparkFunctionRegistry;

impl SparkFunctionRegistry {
    fn canonical_name(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    fn is_built_in(name: &str) -> bool {
        let key = Self::canonical_name(name);
        BUILT_IN_SCALAR_FUNCTIONS.contains_key(key.as_str())
            || BUILT_IN_GENERATOR_FUNCTIONS.contains_key(key.as_str())
            || BUILT_IN_AGGREGATE_FUNCTIONS.contains_key(key.as_str())
            || BUILT_IN_WINDOW_FUNCTIONS.contains_key(key.as_str())
    }

    fn make_status(name: String) -> FunctionStatus {
        FunctionStatus {
            catalog: None,
            namespace: None,
            name,
            // Sail built-in functions do not have descriptions or JVM class names.
            // Spark JVM would return something like
            // `org.apache.spark.sql.catalyst.expressions.Abs` for `abs`.
            // Descriptions (docstrings) for built-in functions could be added in the future
            // if function metadata is annotated in the function registry.
            description: None,
            class_name: String::new(),
            is_temporary: true,
        }
    }
}

impl FunctionRegistry for SparkFunctionRegistry {
    fn contains_function(&self, name: &str) -> bool {
        Self::is_built_in(name)
    }

    fn get_function(&self, name: &str) -> Option<FunctionStatus> {
        if Self::is_built_in(name) {
            Some(Self::make_status(Self::canonical_name(name)))
        } else {
            None
        }
    }

    fn list_functions(&self, pattern: Option<&str>) -> Vec<FunctionStatus> {
        sail_catalog::utils::filter_pattern(BUILT_IN_FUNCTION_NAMES.to_vec(), pattern)
            .into_iter()
            .map(Self::make_status)
            .collect()
    }
}
