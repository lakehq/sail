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

// DataFusion's async extraction does not stage dependencies between nested calls.
// Check the optimized plan, since projection merging can introduce such nesting.
pub fn validate_jev_async_nesting(
    plan: &datafusion_expr::LogicalPlan,
) -> datafusion_common::Result<()> {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use sail_function::scalar::jev::JevKind;

    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::ScalarFunction(outer) = expr
                    && outer.func.as_async().is_some()
                {
                    for argument in &outer.args {
                        argument.apply(|nested| {
                            if let Expr::ScalarFunction(inner) = nested
                                && inner.func.as_async().is_some()
                                && (JevKind::from_name(outer.func.name()).is_some()
                                    || JevKind::from_name(inner.func.name()).is_some())
                            {
                                return datafusion_common::plan_err!(
                                    "Jev async calls cannot be nested inside another async call; materialize the inner result in a table before evaluating the outer call"
                                );
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })?;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

// Work around a DataFusion 55.1.0 bug in async aggregate planning.
// Each aggregate starts its async result columns at the same input-column
// offset. Different aggregates can therefore read the first Jev result,
// although their HTTP requests return different answers.
//
// Move the Jev calls into one projection before aggregation. Replace each
// call in the aggregates with a reference to its own result column.
// DataFusion's projection planner assigns these columns correctly.
// Keep the original input columns, output names and types, and grouping
// expressions. The extra projection processes record batches.
//
// TODO: Remove this workaround when Sail uses a DataFusion version with the
//  indexing fix and the Jev aggregate regression tests pass without it.
pub fn project_jev_aggregate_arguments(
    plan: datafusion_expr::LogicalPlan,
) -> datafusion_common::Result<datafusion_expr::LogicalPlan> {
    use datafusion_common::Column;
    use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion_expr::expr_rewriter::NamePreserver;
    use datafusion_expr::{Aggregate, LogicalPlan, Projection};
    use sail_function::scalar::jev::JevKind;

    plan.transform_up_with_subqueries(|plan| {
        let LogicalPlan::Aggregate(mut aggregate) = plan else {
            return Ok(Transformed::no(plan));
        };
        let mut projection = aggregate
            .input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let input_columns = projection.len();
        let mut next_alias = 0usize;
        let expressions = std::mem::take(&mut aggregate.aggr_expr)
            .into_iter()
            .map(|expr| {
                let name = NamePreserver::new_for_projection().save(&expr);
                let rewritten = expr
                    .transform_up(|expr| {
                        if let Expr::ScalarFunction(function) = &expr
                            && function.func.as_async().is_some()
                            && JevKind::from_name(function.func.name()).is_some()
                        {
                            let name = loop {
                                let name = format!("__sail_jev_aggregate_{next_alias}");
                                next_alias += 1;
                                if !aggregate
                                    .input
                                    .schema()
                                    .has_column_with_unqualified_name(&name)
                                {
                                    break name;
                                }
                            };
                            projection.push(expr.alias(name.clone()));
                            return Ok(Transformed::yes(Expr::Column(Column::from_name(name))));
                        }
                        Ok(Transformed::no(expr))
                    })
                    .data()?;
                Ok(name.restore(rewritten))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        aggregate.aggr_expr = expressions;
        if projection.len() == input_columns {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        }
        let input = Arc::new(LogicalPlan::Projection(Projection::try_new(
            projection,
            aggregate.input,
        )?));
        Ok(Transformed::yes(LogicalPlan::Aggregate(
            Aggregate::try_new_with_schema(
                input,
                aggregate.group_expr,
                aggregate.aggr_expr,
                aggregate.schema,
            )?,
        )))
    })
    .data()
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
