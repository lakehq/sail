use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableFunction;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::DFSchemaRef;
use datafusion_expr::expr::Expr;
use lazy_static::lazy_static;

use crate::error::{PlanError, PlanResult};
use crate::function::common::ScalarFunction;

mod aggregate;
pub(crate) mod common;
mod generator;
mod scalar;
mod table;
mod window;

pub(crate) use aggregate::get_built_in_aggregate_function;
pub(crate) use window::get_built_in_window_function;

lazy_static! {
    pub static ref BUILT_IN_SCALAR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(scalar::list_built_in_scalar_functions());
    pub static ref BUILT_IN_GENERATOR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(generator::list_built_in_generator_functions());
    pub static ref BUILT_IN_TABLE_FUNCTIONS: HashMap<&'static str, Arc<TableFunction>> =
        HashMap::from_iter(table::list_built_in_table_functions());
}

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

pub use generator::get_outer_built_in_generator_functions;

#[allow(deprecated)]
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
        #[allow(deprecated)]
        [Expr::Wildcard {
            qualifier: None,
            options: _,
        }] => {
            vec![Expr::Literal(COUNT_STAR_EXPANSION, None)]
        }
        _ => arguments,
    }
}

/// Expands a wildcard argument to individual column references for `COUNT(DISTINCT *)`.
///
/// Unlike [`transform_count_star_wildcard_expr`], which replaces `*` with a literal `1`,
/// this function expands `*` into one column reference per schema field so that
/// `COUNT(DISTINCT *)` correctly counts distinct rows across all columns.
#[inline(always)]
pub(super) fn expand_wildcard_to_columns(arguments: Vec<Expr>, schema: &DFSchemaRef) -> Vec<Expr> {
    match arguments.as_slice() {
        #[allow(deprecated)]
        [Expr::Wildcard {
            qualifier: None,
            options: _,
        }] => schema.columns().into_iter().map(Expr::Column).collect(),
        _ => arguments,
    }
}
