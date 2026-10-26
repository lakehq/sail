use std::collections::HashMap;
use std::sync::Arc;

use datafusion_expr::expr::Expr;
use sail_common_datafusion::catalog::FunctionStatus;
pub(crate) mod aggregate;
pub mod common;
pub(crate) mod generator;
pub(crate) mod metadata;
pub(crate) mod scalar;
pub(crate) mod table;
pub(crate) mod window;

static BUILT_IN_SCALAR_FUNCTIONS: std::sync::LazyLock<
    HashMap<&'static str, common::ScalarFunction>,
> = std::sync::LazyLock::new(|| HashMap::from_iter(scalar::list_built_in_scalar_functions()));

pub fn get_built_in_function(name: &str) -> Option<common::ScalarFunction> {
    BUILT_IN_SCALAR_FUNCTIONS.get(name).cloned()
}

// Prepare asynchronous functions for physical planning.
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
    use sail_common_datafusion::utils::items::ItemTaker;
    use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};

    let contains_async = |expr: &Expr| {
        expr.exists(|expr| {
            Ok(matches!(expr, Expr::ScalarFunction(function)
                if function.func.as_async().is_some()))
        })
    };
    // This scan avoids plan transformation for queries without async calls,
    // at the cost of another scan when they are present.
    let has_async = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            Ok(if contains_async(expr)? {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
    })? == TreeNodeRecursion::Stop;
    if !has_async {
        return Ok(plan);
    }
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
                            {
                                return datafusion_common::plan_err!(
                                    "Async calls cannot be nested inside another async call; materialize the inner result in a table before evaluating the outer call"
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
                if !contains_async(&predicate)? {
                    join.filter = Some(predicate);
                    return Ok(Transformed::no(LogicalPlan::Join(join)));
                }
                let mut asynchronous = Vec::new();
                let mut synchronous = Vec::new();
                for predicate in split_conjunction_owned(predicate) {
                    if contains_async(&predicate)? {
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
            plan if matches!(&plan, LogicalPlan::Sort(_))
                || matches!(&plan, LogicalPlan::Extension(extension)
                    if extension.node.as_any().is::<SortWithinPartitionsNode>()
                        || extension.node.as_any().is::<RequiredSortNode>()) =>
            {
                let input = Arc::new(plan.inputs().one()?.clone());
                let output = input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                let mut projection = output.clone();
                let mut expressions = plan.expressions();
                let mut next_alias = 0usize;
                for expression in &mut expressions {
                    if !contains_async(expression)? {
                        continue;
                    }
                    let name = loop {
                        let name = format!("__sail_async_sort_{next_alias}");
                        next_alias += 1;
                        if !input.schema().has_column_with_unqualified_name(&name) {
                            break name;
                        }
                    };
                    projection.push(expression.clone().alias(name.clone()));
                    *expression = Expr::Column(Column::from_name(name));
                }
                if projection.len() == output.len() {
                    return Ok(Transformed::no(plan));
                }
                let input = LogicalPlan::Projection(Projection::try_new(projection, input)?);
                let sort = plan.with_new_exprs(expressions, vec![input])?;
                Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(
                    output,
                    Arc::new(sort),
                )?)))
            }
            LogicalPlan::Aggregate(mut aggregate) => {
                // DataFusion 55.1.0 assigns the same async result-column offset to
                // different aggregates. Project async calls first so each aggregate
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
                let mut group_columns: HashMap<Expr, String> = HashMap::new();
                let mut project_group = |expr: Expr| -> datafusion_common::Result<Expr> {
                    if !contains_async(&expr)? {
                        return Ok(expr);
                    }
                    let saved_name = NamePreserver::new_for_projection().save(&expr);
                    let name = if let Some(name) = group_columns.get(&expr) {
                        name.clone()
                    } else {
                        let name = loop {
                            let name = format!("__sail_async_group_{next_alias}");
                            next_alias += 1;
                            if !aggregate.input.schema().has_column_with_unqualified_name(&name) {
                                break name;
                            }
                        };
                        projection.push(expr.clone().alias(name.clone()));
                        group_columns.insert(expr, name.clone());
                        name
                    };
                    Ok(saved_name.restore(Expr::Column(Column::from_name(name))))
                };
                aggregate.group_expr = std::mem::take(&mut aggregate.group_expr)
                    .into_iter()
                    .map(|expr| {
                        if matches!(&expr, Expr::GroupingSet(_)) {
                            expr.map_children(|expr| project_group(expr).map(Transformed::yes))
                                .data()
                        } else {
                            project_group(expr)
                        }
                    })
                    .collect::<datafusion_common::Result<Vec<_>>>()?;
                let expressions = std::mem::take(&mut aggregate.aggr_expr)
                    .into_iter()
                    .map(|expr| {
                        let name = NamePreserver::new_for_projection().save(&expr);
                        let rewritten = expr
                            .transform_up(|expr| {
                                if let Expr::ScalarFunction(function) = &expr
                                    && function.func.as_async().is_some()
                                {
                                    let name = loop {
                                        let name = format!("__sail_async_aggregate_{next_alias}");
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

pub(crate) fn list_built_in_function_statuses() -> Vec<FunctionStatus> {
    let mut names: std::collections::BTreeSet<&str> =
        BUILT_IN_SCALAR_FUNCTIONS.keys().copied().collect();
    names.extend(BUILT_IN_AGGREGATE_FUNCTIONS.keys().copied());
    names.extend(BUILT_IN_WINDOW_FUNCTIONS.keys().copied());
    names.extend(BUILT_IN_GENERATOR_FUNCTIONS.keys().copied());
    names.extend(BUILT_IN_TABLE_FUNCTIONS.keys().copied());
    names.extend(scalar::lambda::list_built_in_higher_order_functions());
    names
        .into_iter()
        .filter_map(metadata::built_in_public_function_status)
        .collect()
}
pub use crate::analyzer::typing::temporal::is_spark_compatible_arrow_fixed_offset;

static BUILT_IN_AGGREGATE_FUNCTIONS: std::sync::LazyLock<
    HashMap<&'static str, aggregate::AggregateFunction>,
> = std::sync::LazyLock::new(|| HashMap::from_iter(aggregate::list_built_in_aggregate_functions()));
static BUILT_IN_WINDOW_FUNCTIONS: std::sync::LazyLock<
    HashMap<&'static str, window::WindowFunction>,
> = std::sync::LazyLock::new(|| HashMap::from_iter(window::list_built_in_window_functions()));

pub(crate) fn get_built_in_aggregate_function(name: &str) -> Option<aggregate::AggregateFunction> {
    BUILT_IN_AGGREGATE_FUNCTIONS.get(name).cloned()
}
pub(crate) fn get_built_in_window_function(name: &str) -> Option<window::WindowFunction> {
    BUILT_IN_WINDOW_FUNCTIONS.get(name).cloned()
}

static BUILT_IN_GENERATOR_FUNCTIONS: std::sync::LazyLock<
    HashMap<&'static str, common::ScalarFunction>,
> = std::sync::LazyLock::new(|| HashMap::from_iter(generator::list_built_in_generator_functions()));
static BUILT_IN_TABLE_FUNCTIONS: std::sync::LazyLock<HashMap<&'static str, table::TableFunction>> =
    std::sync::LazyLock::new(|| HashMap::from_iter(table::list_built_in_table_functions()));

pub(crate) fn get_built_in_generator_function(name: &str) -> Option<common::ScalarFunction> {
    BUILT_IN_GENERATOR_FUNCTIONS.get(name).cloned()
}
pub(crate) fn get_built_in_table_function(name: &str) -> Option<table::TableFunction> {
    BUILT_IN_TABLE_FUNCTIONS.get(name).cloned()
}

/// A resolved function category. Lookup precedence is shared by all plan resolvers.
pub(crate) enum Function {
    Scalar(common::ScalarFunction),
    Aggregate(aggregate::AggregateFunction),
}

pub(crate) fn resolve_function(
    name: &str,
    registered: Option<datafusion_expr::ScalarUDF>,
    context: &common::FunctionContextInput<'_>,
) -> crate::error::PlanResult<Function> {
    use common::ScalarFunctionBuilder as F;
    registered
        .map(|function| {
            let function = Arc::new(function);
            Function::Scalar(F::scalar_udf(move || Arc::clone(&function)))
        })
        .or_else(|| get_built_in_function(name).map(Function::Scalar))
        .or_else(|| {
            let function = context.scalar_kernel(name).ok()?;
            Some(Function::Scalar(F::scalar_udf(move || {
                Arc::clone(&function)
            })))
        })
        .or_else(|| get_built_in_aggregate_function(name).map(Function::Aggregate))
        .or_else(|| {
            let function = context.aggregate_kernel(name).ok()?;
            Some(Function::Aggregate(
                aggregate::AggregateFunctionBuilder::aggregate_udf(name, move || {
                    Arc::clone(&function)
                }),
            ))
        })
        .ok_or_else(|| {
            crate::error::PlanError::unsupported(format!("native function resolution: {name}"))
        })
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
