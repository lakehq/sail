use std::fmt::Debug;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use sail_common::object::DynObject;

/// A utility to format various data structures in the query plan.
pub trait PlanFormatter: DynObject + Debug + Send + Sync {
    /// Returns a human-readable simple string for the data type.
    fn data_type_to_simple_string(&self, data_type: &DataType) -> Result<String>;

    /// Returns a human-readable string for the literal.
    fn literal_to_string(&self, literal: &ScalarValue, display_timezone: &str) -> Result<String>;

    /// Returns a human-readable string for the function call.
    fn function_to_string(
        &self,
        name: &str,
        arguments: Vec<&str>,
        is_distinct: bool,
    ) -> Result<String>;
}
