use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableFunction;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_expr::expr::Expr;
use lazy_static::lazy_static;
use sail_common_datafusion::catalog::FunctionStatus;

use crate::error::{PlanError, PlanResult};
use crate::function::common::ScalarFunction;

mod aggregate;
pub(crate) mod common;
mod generator;
mod metadata;
mod scalar;
mod table;
mod window;

pub(crate) use aggregate::get_built_in_aggregate_function;
pub(crate) use scalar::{get_lambda_parameters, is_higher_order_function};
pub(crate) use window::get_built_in_window_function;

lazy_static! {
    pub static ref BUILT_IN_SCALAR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(scalar::list_built_in_scalar_functions());
    pub static ref BUILT_IN_GENERATOR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(generator::list_built_in_generator_functions());
    pub static ref BUILT_IN_TABLE_FUNCTIONS: HashMap<&'static str, Arc<TableFunction>> =
        HashMap::from_iter(table::list_built_in_table_functions());
}

const BUILT_IN_OPERATOR_FUNCTION_NAMES: &[&str] = &["<>", "between", "||"];

pub fn get_built_in_function(name: &str) -> PlanResult<ScalarFunction> {
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

fn list_built_in_function_names_raw() -> Vec<&'static str> {
    let mut names = BUILT_IN_SCALAR_FUNCTIONS
        .keys()
        .chain(BUILT_IN_GENERATOR_FUNCTIONS.keys())
        .chain(BUILT_IN_TABLE_FUNCTIONS.keys())
        .copied()
        .collect::<Vec<_>>();
    names.extend(aggregate::list_built_in_aggregate_function_names());
    names.extend(window::list_built_in_window_function_names());
    names.extend(BUILT_IN_OPERATOR_FUNCTION_NAMES.iter().copied());
    names.sort_unstable();
    names.dedup();
    names
}

pub(crate) fn list_built_in_function_statuses() -> Vec<FunctionStatus> {
    list_built_in_function_names_raw()
        .into_iter()
        .filter_map(metadata::built_in_public_function_status)
        .collect()
}

pub use generator::get_outer_built_in_generator_functions;

/// This function is temporary and should ONLY be used for COUNT(*).
/// [`Expr::Wildcard`]
///
/// Only aware of this being applicable to [`datafusion::functions_aggregate::count`],
/// although it may be applicable elsewhere as well.
/// Similarly, this function may need to be adjusted if there are other possible pattern matches
/// that were not considered.
#[inline(always)]
pub(super) fn transform_count_star_wildcard_expr(arguments: Vec<Expr>) -> Vec<Expr> {
    match arguments.as_slice() {
        #[expect(deprecated)]
        [Expr::Wildcard {
            qualifier: None,
            options: _,
        }] => {
            vec![Expr::Literal(COUNT_STAR_EXPANSION, None)]
        }
        _ => arguments,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_function_metadata_names_match_registered_functions() {
        let registered_names = list_built_in_function_names_raw();
        let metadata_names = metadata::built_in_function_metadata_names().collect::<Vec<_>>();
        for name in metadata::built_in_function_metadata_names() {
            assert!(
                registered_names.contains(&name),
                "metadata for unregistered function: {name}"
            );
        }
        for name in registered_names {
            assert!(
                metadata_names.contains(&name),
                "registered function missing metadata: {name}"
            );
        }
    }

    #[test]
    fn generated_function_metadata_populates_status_description() {
        let status = metadata::built_in_function_status("to_date");
        assert_eq!(status.name, "to_date");
        assert_eq!(status.signatures, vec!["to_date(date_str[, fmt])"]);
        assert!(status
            .usage
            .as_deref()
            .is_some_and(|usage| usage.contains("to_date(date_str[, fmt]) - Parses")));
        assert!(status
            .arguments
            .as_deref()
            .is_some_and(|arguments| arguments.contains("Arguments:")));
        assert!(status
            .examples
            .as_deref()
            .is_some_and(|examples| examples.contains("SELECT to_date")));
        assert_eq!(status.since.as_deref(), Some("1.5.0"));
        assert_eq!(
            status.class_name,
            "org.apache.spark.sql.catalyst.expressions.ParseToDate"
        );
        assert!(status.is_temporary);
    }
}
