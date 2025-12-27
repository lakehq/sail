use std::fmt::Debug;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use sail_common::object::DynObject;

use crate::catalog::display::CatalogDisplay;
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

pub struct PlanService {
    catalog_display: Box<dyn CatalogDisplay>,
    plan_formatter: Box<dyn PlanFormatter>,
}

impl PlanService {
    pub fn new(
        catalog_display: Box<dyn CatalogDisplay>,
        plan_formatter: Box<dyn PlanFormatter>,
    ) -> Self {
        Self {
            catalog_display,
            plan_formatter,
        }
    }

    pub fn catalog_display(&self) -> &dyn CatalogDisplay {
        self.catalog_display.as_ref()
    }

    pub fn plan_formatter(&self) -> &dyn PlanFormatter {
        self.plan_formatter.as_ref()
    }
}

impl SessionExtension for PlanService {
    fn name() -> &'static str {
        "PlanService"
    }
}
