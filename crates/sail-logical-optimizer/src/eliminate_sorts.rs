use std::sync::Arc;

use datafusion::functions_aggregate::{average, bit_and_or_xor, bool_and_or, count, sum};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::logical_plan::{Aggregate, Partitioning};
use datafusion_expr::{AggregateUDF, Expr, ExprSchemable, JoinType, LogicalPlan};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use sail_common_datafusion::min_max::min_max_agg::{Max, Min};
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::sort::SortWithinPartitionsNode;

/// Removes sorts whose order cannot affect the result.
///
/// Following Spark's `EliminateSorts`, sorts below a join with a deterministic condition,
/// and below an aggregate whose functions do not depend on the input order, are removed.
/// The removal looks through deterministic projections, filters and repartitions.
/// Following Spark's `OptimizeSubqueries`, the top-level sort of a subquery is removed.
/// A sort with a fetch selects rows, so it is never removed.
#[derive(Debug, Default)]
pub struct EliminateSorts;

impl OptimizerRule for EliminateSorts {
    fn name(&self) -> &str {
        "eliminate_sorts"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let plan = match plan {
            LogicalPlan::Join(mut join)
                if !join
                    .on
                    .iter()
                    .any(|(left, right)| left.is_volatile() || right.is_volatile())
                    && !join.filter.as_ref().is_some_and(Expr::is_volatile) =>
            {
                // A semi, anti or mark join returns the rows of one side in their order, as
                // Spark keeps the sort of that side by pushing the join below it.
                let (left, right) = match join.join_type {
                    JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => (false, true),
                    JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                        (true, false)
                    }
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                        (true, true)
                    }
                };
                let mut transformed = false;
                if left {
                    let input = remove_sorts(Arc::unwrap_or_clone(join.left))?;
                    transformed |= input.transformed;
                    join.left = Arc::new(input.data);
                }
                if right {
                    let input = remove_sorts(Arc::unwrap_or_clone(join.right))?;
                    transformed |= input.transformed;
                    join.right = Arc::new(input.data);
                }
                Transformed::new_transformed(LogicalPlan::Join(join), transformed)
            }
            LogicalPlan::Aggregate(ref aggregate) if is_order_irrelevant(aggregate)? => {
                plan.map_children(remove_sorts)?
            }
            plan => Transformed::no(plan),
        };
        plan.transform_data(|plan| {
            plan.map_subqueries(|subquery| {
                let LogicalPlan::Subquery(mut subquery) = subquery else {
                    return Ok(Transformed::no(subquery));
                };
                let body = remove_top_level_sort(subquery.subquery.as_ref().clone())?;
                if !body.transformed {
                    return Ok(Transformed::no(LogicalPlan::Subquery(subquery)));
                }
                subquery.subquery = Arc::new(body.data);
                Ok(Transformed::yes(LogicalPlan::Subquery(subquery)))
            })
        })
    }
}

/// Removes the sorts in the plan through the nodes that keep its rows unchanged,
/// as Spark's `EliminateSorts` does below a join or aggregate.
fn remove_sorts(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => Ok(Transformed::yes(
            remove_sorts(Arc::unwrap_or_clone(sort.input))?.data,
        )),
        LogicalPlan::Extension(ref extension) => {
            let node = extension.node.as_any();
            if let Some(sort) = node.downcast_ref::<SortWithinPartitionsNode>()
                && sort.fetch().is_none()
            {
                let input = sort.input().as_ref().clone();
                Ok(Transformed::yes(remove_sorts(input)?.data))
            } else if node
                .downcast_ref::<ExplicitRepartitionNode>()
                .is_some_and(|repartition| {
                    !repartition
                        .partitioning_expressions()
                        .iter()
                        .any(Expr::is_volatile)
                })
            {
                plan.map_children(remove_sorts)
            } else {
                Ok(Transformed::no(plan))
            }
        }
        LogicalPlan::Projection(ref projection)
            if !projection.expr.iter().any(Expr::is_volatile) =>
        {
            plan.map_children(remove_sorts)
        }
        LogicalPlan::Filter(ref filter) if !filter.predicate.is_volatile() => {
            plan.map_children(remove_sorts)
        }
        LogicalPlan::Repartition(ref repartition)
            if !matches!(
                &repartition.partitioning_scheme,
                Partitioning::Hash(exprs, _) | Partitioning::DistributeBy(exprs)
                    if exprs.iter().any(Expr::is_volatile)
            ) =>
        {
            plan.map_children(remove_sorts)
        }
        LogicalPlan::SubqueryAlias(_) => plan.map_children(remove_sorts),
        _ => Ok(Transformed::no(plan)),
    }
}

/// Removes the sort at the top of a subquery plan, whose order cannot affect the result
/// of the subquery expression.
fn remove_top_level_sort(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            Ok(Transformed::yes(Arc::unwrap_or_clone(sort.input)))
        }
        LogicalPlan::Extension(ref extension) => {
            match extension
                .node
                .as_any()
                .downcast_ref::<SortWithinPartitionsNode>()
            {
                Some(sort) if sort.fetch().is_none() => {
                    Ok(Transformed::yes(sort.input().as_ref().clone()))
                }
                _ => Ok(Transformed::no(plan)),
            }
        }
        LogicalPlan::Projection(ref projection)
            if !projection.expr.iter().any(Expr::is_volatile) =>
        {
            plan.map_children(remove_top_level_sort)
        }
        LogicalPlan::SubqueryAlias(_) => plan.map_children(remove_top_level_sort),
        _ => Ok(Transformed::no(plan)),
    }
}

/// Returns whether the result of the aggregate does not depend on the order of its input,
/// following Spark's `EliminateSorts`.
fn is_order_irrelevant(aggregate: &Aggregate) -> Result<bool> {
    let schema = aggregate.input.schema();
    let mut irrelevant = true;
    for expr in &aggregate.aggr_expr {
        expr.apply(|expr| {
            let Expr::AggregateFunction(function) = expr else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if is_order_irrelevant_aggregate(&function.func, &function.params.args, schema)? {
                Ok(TreeNodeRecursion::Jump)
            } else {
                irrelevant = false;
                Ok(TreeNodeRecursion::Stop)
            }
        })?;
        if !irrelevant {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Returns whether the result of the aggregate function does not depend on the order of
/// its input. The result of a floating-point sum or average depends on the order of
/// its input.
pub fn is_order_irrelevant_aggregate(
    udf: &AggregateUDF,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<bool> {
    if udf.inner().is::<Min>()
        || udf.inner().is::<Max>()
        || udf == count::count_udaf().as_ref()
        || udf == bit_and_or_xor::bit_and_udaf().as_ref()
        || udf == bit_and_or_xor::bit_or_udaf().as_ref()
        || udf == bit_and_or_xor::bit_xor_udaf().as_ref()
        || udf == bool_and_or::bool_and_udaf().as_ref()
        || udf == bool_and_or::bool_or_udaf().as_ref()
    {
        return Ok(true);
    }

    if udf == sum::sum_udaf().as_ref()
        || udf == average::avg_udaf().as_ref()
        || udf.inner().is::<SparkTrySum>()
        || udf.inner().is::<TryAvgFunction>()
    {
        let Some(arg) = args.first() else {
            return Ok(false);
        };
        return Ok(!matches!(
            arg.get_type(schema)?,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ));
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions::math::expr_fn::random;
    use datafusion::functions_aggregate::expr_fn::{first_value, sum};
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion_expr::{
        AggregateUDF, Extension, LogicalPlanBuilder, cast, col, exists, lit, scalar_subquery,
    };

    use super::*;

    fn max(expr: Expr) -> Expr {
        AggregateUDF::from(Max::new()).call(vec![expr])
    }

    fn scan(name: &str) -> Result<LogicalPlanBuilder> {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]);
        LogicalPlanBuilder::scan(
            name,
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema)))),
            None,
        )
    }

    fn sorted(name: &str) -> Result<LogicalPlanBuilder> {
        scan(name)?.sort(vec![col("v").sort(false, true)])
    }

    fn sort_within_partitions(name: &str) -> Result<LogicalPlanBuilder> {
        let input = scan(name)?.build()?;
        Ok(LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(SortWithinPartitionsNode::new(
                Arc::new(input),
                vec![col("v").sort(true, true)],
                None,
            )),
        })))
    }

    fn optimize(plan: LogicalPlanBuilder) -> Result<String> {
        let plan = Optimizer::with_rules(vec![Arc::new(EliminateSorts)]).optimize(
            plan.build()?,
            &OptimizerContext::new(),
            |_, _| {},
        )?;
        Ok(plan.display_indent().to_string())
    }

    fn sorts(plan: &str) -> usize {
        plan.matches("Sort").count()
    }

    #[test]
    fn sorts_below_order_irrelevant_aggregates_are_removed() -> Result<()> {
        let plan = optimize(
            sorted("t")?
                .project(vec![col("v")])?
                .filter(col("v").gt(lit(1_i64)))?
                .aggregate(vec![col("v")], vec![max(col("v"))])?,
        )?;
        assert_eq!(sorts(&plan), 0, "{plan}");
        let plan = optimize(sort_within_partitions("t")?.aggregate(
            Vec::<Expr>::new(),
            vec![max(col("v")), sum(col("v"))],
        )?)?;
        assert_eq!(sorts(&plan), 0, "{plan}");
        Ok(())
    }

    #[test]
    fn sorts_below_order_sensitive_aggregates_are_kept() -> Result<()> {
        let float = cast(col("v"), DataType::Float64);
        let plan = optimize(sorted("t")?.aggregate(Vec::<Expr>::new(), vec![sum(float)])?)?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        let plan = optimize(sorted("t")?.aggregate(
            Vec::<Expr>::new(),
            vec![max(col("v")), first_value(col("v"), vec![])],
        )?)?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        Ok(())
    }

    #[test]
    fn sorts_that_select_rows_or_feed_volatile_expressions_are_kept() -> Result<()> {
        let plan = optimize(
            scan("t")?
                .sort_with_limit(vec![col("v").sort(true, true)], Some(2))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?,
        )?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        let plan = optimize(
            sorted("t")?
                .limit(0, Some(2))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?,
        )?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        let plan = optimize(
            sorted("t")?
                .filter(random().lt(lit(0.5)))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?,
        )?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        Ok(())
    }

    #[test]
    fn sorts_below_joins_are_removed() -> Result<()> {
        let plan = optimize(sorted("l")?.join_on(
            sorted("r")?.build()?,
            JoinType::Inner,
            vec![col("l.k").eq(col("r.k"))],
        )?)?;
        assert_eq!(sorts(&plan), 0, "{plan}");
        Ok(())
    }

    #[test]
    fn sorts_of_the_returned_side_of_semi_joins_are_kept() -> Result<()> {
        let plan = optimize(sorted("l")?.join_on(
            sorted("r")?.build()?,
            JoinType::LeftSemi,
            vec![col("l.k").eq(col("r.k"))],
        )?)?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        assert!(plan.contains("Sort: l.v DESC"), "{plan}");
        Ok(())
    }

    #[test]
    fn top_level_sorts_of_subqueries_are_removed() -> Result<()> {
        let subquery = sorted("s")?.project(vec![col("s.k")])?.build()?;
        let plan = optimize(scan("t")?.filter(exists(Arc::new(subquery)))?)?;
        assert_eq!(sorts(&plan), 0, "{plan}");
        let subquery = scan("s")?
            .sort_with_limit(vec![col("v").sort(true, true)], Some(1))?
            .project(vec![col("s.v")])?
            .build()?;
        let plan = optimize(scan("t")?.project(vec![scalar_subquery(Arc::new(subquery))])?)?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        Ok(())
    }
}
