use crate::error::{PlanError, PlanResult};
use crate::function::common::Function;
use lazy_static::lazy_static;
use std::collections::HashMap;

mod aggregate;
mod common;
mod generator;
mod scalar;
mod window;

pub(crate) use aggregate::get_built_in_aggregate_function;
pub(crate) use window::get_built_in_window_function;

lazy_static! {
    static ref BUILT_IN_FUNCTIONS: HashMap<&'static str, Function> = {
        let mut m = HashMap::new();
        for (name, func) in list_built_in_functions() {
            m.insert(name, func);
        }
        m
    };
}

pub(crate) fn get_built_in_function(name: &str) -> PlanResult<Function> {
    let name = name.to_lowercase();
    Ok(BUILT_IN_FUNCTIONS
        .get(name.as_str())
        .ok_or_else(|| PlanError::unsupported(format!("unknown function: {name}")))?
        .clone())
}

fn list_built_in_functions() -> Vec<(&'static str, Function)> {
    let mut output = Vec::new();
    output.extend(scalar::list_built_in_scalar_functions());
    output.extend(generator::list_built_in_generator_functions());
    output
}
