use sail_common_datafusion::catalog::FunctionStatus;
use sail_common_datafusion::session::plan::FunctionRegistry;

use crate::function::{
    BUILT_IN_AGGREGATE_FUNCTIONS, BUILT_IN_GENERATOR_FUNCTIONS, BUILT_IN_SCALAR_FUNCTIONS,
    BUILT_IN_WINDOW_FUNCTIONS,
};

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
    fn is_built_in(name: &str) -> bool {
        let key = name.to_ascii_lowercase();
        BUILT_IN_SCALAR_FUNCTIONS.contains_key(key.as_str())
            || BUILT_IN_GENERATOR_FUNCTIONS.contains_key(key.as_str())
            || BUILT_IN_AGGREGATE_FUNCTIONS.contains_key(key.as_str())
            || BUILT_IN_WINDOW_FUNCTIONS.contains_key(key.as_str())
    }

    fn built_in_function_names() -> Vec<&'static str> {
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
    }

    fn make_status(name: &str) -> FunctionStatus {
        FunctionStatus {
            catalog: None,
            namespace: None,
            name: name.to_string(),
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
            Some(Self::make_status(&name.to_ascii_lowercase()))
        } else {
            None
        }
    }

    fn list_functions(&self, pattern: Option<&str>) -> Vec<FunctionStatus> {
        let names = Self::built_in_function_names();
        sail_catalog::utils::filter_pattern(names, pattern)
            .into_iter()
            .map(|name| Self::make_status(&name))
            .collect()
    }
}
