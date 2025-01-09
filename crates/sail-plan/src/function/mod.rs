use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableFunction;
use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};
use crate::function::common::Function;

mod aggregate;
pub(crate) mod common;
mod generator;
mod scalar;
mod table;
mod window;

pub(crate) use aggregate::get_built_in_aggregate_function;
pub(crate) use window::get_built_in_window_function;

lazy_static! {
    pub static ref BUILT_IN_SCALAR_FUNCTIONS: HashMap<&'static str, Function> =
        HashMap::from_iter(scalar::list_built_in_scalar_functions());
    pub static ref BUILT_IN_GENERATOR_FUNCTIONS: HashMap<&'static str, Function> =
        HashMap::from_iter(generator::list_built_in_generator_functions());
    pub static ref BUILT_IN_TABLE_FUNCTIONS: HashMap<&'static str, Arc<TableFunction>> =
        HashMap::from_iter(table::list_built_in_table_functions());
}

pub fn get_built_in_function(name: &str) -> PlanResult<Function> {
    Ok(BUILT_IN_SCALAR_FUNCTIONS
        .get(name)
        .or_else(|| BUILT_IN_GENERATOR_FUNCTIONS.get(name))
        .ok_or_else(|| PlanError::unsupported(format!("unknown function: {name}")))?
        .clone())
}

pub fn get_built_in_table_function(name: &str) -> PlanResult<Arc<TableFunction>> {
    Ok(BUILT_IN_TABLE_FUNCTIONS
        .get(name)
        .ok_or_else(|| PlanError::unsupported(format!("unknown table function: {name}")))?
        .clone())
}

pub fn is_built_in_generator_function(name: &str) -> bool {
    BUILT_IN_GENERATOR_FUNCTIONS.contains_key(name)
}

pub use generator::get_outer_built_in_generator_functions;
