use std::fmt::Debug;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use sail_common::object::DynObject;

use crate::catalog::display::CatalogDisplay;
use crate::catalog::FunctionStatus;
use crate::extension::SessionExtension;

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

/// A registry of built-in (system) functions known to the query engine.
/// This is used by the catalog to answer existence and metadata queries for
/// functions that are not stored in a persistent catalog.
pub trait FunctionRegistry: Send + Sync {
    /// Returns `true` if the registry contains a built-in function with the given name.
    /// The comparison is case-insensitive.
    fn contains_function(&self, name: &str) -> bool;

    /// Returns the metadata for the built-in function with the given name, if it exists.
    /// The returned [`FunctionStatus`] has `is_temporary` set to `true` because built-in
    /// functions are session-scoped (Spark-compatible semantics).
    fn get_function(&self, name: &str) -> Option<FunctionStatus>;

    /// Returns the metadata for all built-in functions whose name matches the given pattern.
    /// If `pattern` is `None`, all built-in functions are returned.
    fn list_functions(&self, pattern: Option<&str>) -> Vec<FunctionStatus>;
}

pub struct PlanService {
    catalog_display: Box<dyn CatalogDisplay>,
    plan_formatter: Box<dyn PlanFormatter>,
    function_registry: Box<dyn FunctionRegistry>,
}

impl PlanService {
    pub fn new(
        catalog_display: Box<dyn CatalogDisplay>,
        plan_formatter: Box<dyn PlanFormatter>,
        function_registry: Box<dyn FunctionRegistry>,
    ) -> Self {
        Self {
            catalog_display,
            plan_formatter,
            function_registry,
        }
    }

    pub fn catalog_display(&self) -> &dyn CatalogDisplay {
        self.catalog_display.as_ref()
    }

    pub fn plan_formatter(&self) -> &dyn PlanFormatter {
        self.plan_formatter.as_ref()
    }

    pub fn function_registry(&self) -> &dyn FunctionRegistry {
        self.function_registry.as_ref()
    }
}

impl SessionExtension for PlanService {
    fn name() -> &'static str {
        "PlanService"
    }
}
