use std::collections::HashMap;
use std::sync::Arc;

use chrono::FixedOffset;
use datafusion::arrow::array::timezone::Tz;
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

fn list_built_in_function_names() -> Vec<&'static str> {
    let mut names = BUILT_IN_SCALAR_FUNCTIONS
        .keys()
        .chain(BUILT_IN_GENERATOR_FUNCTIONS.keys())
        .chain(BUILT_IN_TABLE_FUNCTIONS.keys())
        .copied()
        .chain(aggregate::list_built_in_aggregate_function_names())
        .chain(window::list_built_in_window_function_names())
        .chain(BUILT_IN_OPERATOR_FUNCTION_NAMES.iter().copied())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}

pub(crate) fn list_built_in_function_statuses() -> Vec<FunctionStatus> {
    list_built_in_function_names()
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
        [
            Expr::Wildcard {
                qualifier: None,
                options: _,
            },
        ] => {
            vec![Expr::Literal(COUNT_STAR_EXPANSION, None)]
        }
        _ => arguments,
    }
}

pub fn is_spark_compatible_arrow_fixed_offset(timezone: &str) -> bool {
    if !timezone.starts_with('+') && !timezone.starts_with('-') {
        return false;
    }
    if timezone.parse::<Tz>().is_err() {
        return false;
    }
    let normalized = (timezone.len() == 3).then(|| format!("{timezone}:00"));
    normalized
        .as_deref()
        .unwrap_or(timezone)
        .parse::<FixedOffset>()
        .is_ok_and(|offset| offset.local_minus_utc().unsigned_abs() <= 18 * 60 * 60)
}

#[cfg(test)]
mod tests {
    use super::is_spark_compatible_arrow_fixed_offset;

    #[test]
    fn spark_compatible_arrow_fixed_offset_matrix() {
        for timezone in [
            "+00", "+01", "+0130", "+01:30", "-00:00", "-01", "-0130", "-01:30", "+18", "+1800",
            "+18:00", "-18", "-1800", "-18:00",
        ] {
            assert!(
                is_spark_compatible_arrow_fixed_offset(timezone),
                "{timezone}"
            );
        }

        for timezone in [
            "",
            "Z",
            "UTC",
            "UTC+01:30",
            "GMT+01:30",
            "+1:30",
            "+01:3",
            "+01:30:15",
            "+0160",
            "+18:01",
            "+1801",
            "+23:59",
            "-18:01",
            "America/Los_Angeles",
        ] {
            assert!(
                !is_spark_compatible_arrow_fixed_offset(timezone),
                "{timezone}"
            );
        }
    }
}
