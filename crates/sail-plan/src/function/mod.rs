use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};
use crate::function::common::Function;

mod aggregate;
pub(crate) mod common;
mod generator;
mod scalar;
mod window;

pub(crate) use aggregate::get_built_in_aggregate_function;
pub(crate) use window::get_built_in_window_function;

lazy_static! {
    pub static ref BUILT_IN_SCALAR_FUNCTIONS: HashMap<&'static str, Function> =
        HashMap::from_iter(scalar::list_built_in_scalar_functions());
    pub static ref BUILT_IN_GENERATOR_FUNCTIONS: HashMap<&'static str, Function> =
        HashMap::from_iter(generator::list_built_in_generator_functions());
}

pub(crate) fn get_built_in_function(name: &str) -> PlanResult<Function> {
    let name = name.to_lowercase();
    Ok(BUILT_IN_SCALAR_FUNCTIONS
        .get(name.as_str())
        .or_else(|| BUILT_IN_GENERATOR_FUNCTIONS.get(name.as_str()))
        .ok_or_else(|| PlanError::unsupported(format!("unknown function: {name}")))?
        .clone())
}
