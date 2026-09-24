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

/// Prepare asynchronous functions in one logical-plan traversal.
/// The current checks and rewrites apply only to Jev calls.
pub fn prepare_async_functions(
    plan: datafusion_expr::LogicalPlan,
) -> datafusion_common::Result<datafusion_expr::LogicalPlan> {
    use datafusion_common::Column;
    use datafusion_common::tree_node::{
        Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
    };
    use datafusion_expr::expr_rewriter::NamePreserver;
    use datafusion_expr::utils::{conjunction, split_conjunction_owned};
    use datafusion_expr::{Aggregate, Filter, JoinType, LogicalPlan, Projection};
    use sail_function::scalar::jev::JevKind;

    let contains_jev = |expr: &Expr| {
        expr.exists(|expr| {
            Ok(matches!(expr, Expr::ScalarFunction(function)
                if function.func.as_async().is_some()
                    && JevKind::from_name(function.func.name()).is_some()))
        })
    };
    plan.transform_up_with_subqueries(|plan| {
        // Check each node before rewriting its expressions. Extracting an inner
        // async call first could hide unsupported nesting from this check.
        plan.apply_expressions(|expr| {
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
        match plan {
            LogicalPlan::Join(mut join) if join.join_type == JoinType::Inner => {
                let Some(predicate) = join.filter.take() else {
                    return Ok(Transformed::no(LogicalPlan::Join(join)));
                };
                if !contains_jev(&predicate)? {
                    join.filter = Some(predicate);
                    return Ok(Transformed::no(LogicalPlan::Join(join)));
                }
                let mut asynchronous = Vec::new();
                let mut synchronous = Vec::new();
                for predicate in split_conjunction_owned(predicate) {
                    if contains_jev(&predicate)? {
                        asynchronous.push(predicate);
                    } else {
                        synchronous.push(predicate);
                    }
                }
                join.filter = conjunction(synchronous);
                let input = Arc::new(LogicalPlan::Join(join));
                let Some(predicate) = conjunction(asynchronous) else {
                    return Ok(Transformed::no(Arc::unwrap_or_clone(input)));
                };
                Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                    predicate, input,
                )?)))
            }
            LogicalPlan::Sort(mut sort) => {
                let output = sort
                    .input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                let mut projection = output.clone();
                let mut next_alias = 0usize;
                for order in &mut sort.expr {
                    if !contains_jev(&order.expr)? {
                        continue;
                    }
                    let name = loop {
                        let name = format!("__sail_jev_sort_{next_alias}");
                        next_alias += 1;
                        if !sort.input.schema().has_column_with_unqualified_name(&name) {
                            break name;
                        }
                    };
                    projection.push(order.expr.clone().alias(name.clone()));
                    order.expr = Expr::Column(Column::from_name(name));
                }
                if projection.len() == output.len() {
                    return Ok(Transformed::no(LogicalPlan::Sort(sort)));
                }
                sort.input = Arc::new(LogicalPlan::Projection(Projection::try_new(
                    projection, sort.input,
                )?));
                Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(
                    output,
                    Arc::new(LogicalPlan::Sort(sort)),
                )?)))
            }
            LogicalPlan::Aggregate(mut aggregate) => {
                // DataFusion 55.1.0 assigns the same async result-column offset to
                // different aggregates. Project Jev calls first so each aggregate
                // reads its own result column.
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
            }
            plan => Ok(Transformed::no(plan)),
        }
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
