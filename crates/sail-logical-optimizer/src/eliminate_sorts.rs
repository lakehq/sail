use std::sync::Arc;

use datafusion::functions_aggregate::{average, bit_and_or_xor, bool_and_or, count, sum};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::expr::{Cast, TryCast};
use datafusion_expr::logical_plan::{Aggregate, Join, Partitioning};
use datafusion_expr::{
    AggregateUDF, BinaryExpr, Expr, ExprSchemable, JoinType, LogicalPlan, Operator,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use sail_common_datafusion::min_max::min_max_agg::{Max, Min};
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};

/// Removes sorts whose order cannot affect the result.
///
/// Following Spark's `EliminateSorts`, sorts below a join with a deterministic condition,
/// and below an aggregate whose functions do not depend on the input order, are removed.
/// The removal looks through deterministic projections, filters and repartitions.
/// Following Spark's `OptimizeSubqueries`, the top-level sort of a subquery is removed.
/// A sort with a fetch selects rows, so it is never removed.
/// Following Spark's `PushDownLeftSemiAntiJoin`, a left semi or anti join is first moved
/// below the sorts of its left input, so that its output keeps their order.
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
            LogicalPlan::Join(join) => {
                let plan = push_down_left_semi_anti_join(join)?;
                if plan.transformed {
                    // The moved join is visited next as a descendant of the returned plan.
                    return Ok(plan);
                }
                plan.data
            }
            plan => plan,
        };
        let plan = match plan {
            LogicalPlan::Join(mut join) if is_deterministic(&join) => {
                // A semi, anti or mark join returns the rows of one side in their order, so
                // the sorts of that side are kept.
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

/// Moves a left semi or anti join with a deterministic condition below the lowest sort of
/// its left input, as Spark's `PushDownLeftSemiAntiJoin` does, so that the join keeps the
/// order of the sort. The sort must be reached through sorts without a fetch, repartitions,
/// subquery aliases and projections without volatile expressions or subqueries, and the
/// condition is rewritten in terms of the input of the sort. The condition must not refer to
/// a column that a projection computes, which the join would compute again. Otherwise, the
/// join is returned unchanged.
///
/// Spark does not move the join below a projection that computes any column if the join has
/// a condition. The sort is then removed below the join, so keeping its order is also a
/// valid result.
pub fn push_down_left_semi_anti_join(join: Join) -> Result<Transformed<LogicalPlan>> {
    if !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        || !is_deterministic(&join)
    {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let left = Arc::clone(&join.left);
    let mut path = vec![];
    let mut length = 0;
    let mut node = left.as_ref();
    while let Some(is_sort) = semi_join_pass_through(node) {
        path.push(node);
        if is_sort {
            length = path.len();
        }
        node = node.inputs()[0];
    }
    path.truncate(length);
    let Some(&lowest) = path.last() else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };
    let right_schema = join.right.schema();
    if is_ambiguous(&join.on, join.filter.as_ref(), left.schema(), right_schema) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    // The condition refers to the columns of the left input, which become the columns
    // of the input of the lowest sort.
    let mut on = join.on.clone();
    let mut filter = join.filter.clone();
    let mut computed = false;
    for &node in &path {
        let mut rewrite = |expr: Expr| -> Result<Expr> {
            expr.transform(|expr| {
                let Expr::Column(column) = &expr else {
                    return Ok(Transformed::no(expr));
                };
                if right_schema.has_column(column) {
                    return Ok(Transformed::no(expr));
                }
                let Some(index) = node.schema().maybe_index_of_column(column) else {
                    return Ok(Transformed::no(expr));
                };
                Ok(match node {
                    LogicalPlan::Projection(projection) => {
                        match column_or_literal(&projection.expr[index], projection.input.schema())
                        {
                            Some(expr) => Transformed::yes(expr),
                            None => {
                                computed = true;
                                Transformed::no(expr)
                            }
                        }
                    }
                    LogicalPlan::SubqueryAlias(alias) => Transformed::yes(Expr::Column(
                        Column::from(alias.input.schema().qualified_field(index)),
                    )),
                    _ => Transformed::no(expr),
                })
            })
            .data()
        };
        on = on
            .into_iter()
            .map(|(left, right)| Ok((rewrite(left)?, right)))
            .collect::<Result<_>>()?;
        filter = filter.map(rewrite).transpose()?;
    }
    if computed {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let input = Arc::new(lowest.inputs()[0].clone());
    if is_ambiguous(&on, filter.as_ref(), input.schema(), right_schema) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let mut plan = LogicalPlan::Join(Join::try_new(
        input,
        Arc::clone(&join.right),
        on,
        filter,
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?);
    for node in path.into_iter().rev() {
        plan = node.with_new_exprs(node.expressions(), vec![plan])?;
    }
    Ok(Transformed::yes(plan))
}

/// Returns the column or literal that a projected expression computes, if any. Like Spark,
/// which removes the addition of zero and the multiplication by one of integers
/// (`ReorderAssociativeOperator`) before it moves a semi join below a projection, these
/// operations are looked through.
fn column_or_literal(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    match expr {
        Expr::Alias(alias) => column_or_literal(&alias.expr, schema),
        Expr::Column(_) | Expr::Literal(..) => Some(expr.clone()),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let identity = match op {
                Operator::Plus => 0,
                Operator::Multiply => 1,
                _ => return None,
            };
            let ((Expr::Literal(value, _), other) | (other, Expr::Literal(value, _))) =
                (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            let is_identity = value.data_type().is_integer()
                && value.cast_to(&DataType::Int64).ok() == Some(ScalarValue::Int64(Some(identity)));
            if is_identity && other.get_type(schema).is_ok_and(|t| t.is_integer()) {
                column_or_literal(other, schema)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Returns whether a column of the join condition names a column of both inputs.
fn is_ambiguous(
    on: &[(Expr, Expr)],
    filter: Option<&Expr>,
    left: &DFSchema,
    right: &DFSchema,
) -> bool {
    on.iter()
        .flat_map(|(l, r)| [l, r])
        .chain(filter)
        .flat_map(Expr::column_refs)
        .any(|column| left.has_column(column) && right.has_column(column))
}

/// Returns whether a left semi or anti join can be moved below the node, and if so,
/// whether the node is a sort.
fn semi_join_pass_through(plan: &LogicalPlan) -> Option<bool> {
    if plan.expressions().iter().any(Expr::is_volatile) {
        return None;
    }
    match plan {
        LogicalPlan::Sort(sort) => sort.fetch.is_none().then_some(true),
        LogicalPlan::Projection(projection) => {
            (!projection.expr.iter().any(contains_subquery)).then_some(false)
        }
        LogicalPlan::SubqueryAlias(_) | LogicalPlan::Repartition(_) => Some(false),
        LogicalPlan::Extension(extension) => {
            let node = extension.node.as_any();
            if let Some(sort) = node.downcast_ref::<SortWithinPartitionsNode>() {
                sort.fetch().is_none().then_some(true)
            } else if let Some(sort) = node.downcast_ref::<RequiredSortNode>() {
                sort.fetch().is_none().then_some(true)
            } else {
                node.is::<ExplicitRepartitionNode>().then_some(false)
            }
        }
        _ => None,
    }
}

/// Returns whether the expression contains a subquery or an outer reference, which must not
/// be moved into a join condition.
fn contains_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::OuterReferenceColumn(..)
        ))
    })
    .unwrap_or(true)
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
        // A semi or anti join returns the rows of one side, so Spark pushes it below the
        // sorts of that side, which are then removed as well.
        LogicalPlan::Join(mut join) if is_deterministic(&join) => match join.join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => {
                let input = remove_sorts(Arc::unwrap_or_clone(join.left))?;
                join.left = Arc::new(input.data);
                Ok(Transformed::new_transformed(
                    LogicalPlan::Join(join),
                    input.transformed,
                ))
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                let input = remove_sorts(Arc::unwrap_or_clone(join.right))?;
                join.right = Arc::new(input.data);
                Ok(Transformed::new_transformed(
                    LogicalPlan::Join(join),
                    input.transformed,
                ))
            }
            _ => Ok(Transformed::no(LogicalPlan::Join(join))),
        },
        _ => Ok(Transformed::no(plan)),
    }
}

/// Returns whether the join condition is deterministic.
fn is_deterministic(join: &Join) -> bool {
    !join
        .on
        .iter()
        .any(|(left, right)| left.is_volatile() || right.is_volatile())
        && !join.filter.as_ref().is_some_and(Expr::is_volatile)
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
    // Spark evaluates volatile grouping keys below the aggregate, which keeps the sort.
    if aggregate.group_expr.iter().any(Expr::is_volatile) {
        return Ok(false);
    }
    let schema = aggregate.input.schema();
    let mut irrelevant = true;
    for expr in &aggregate.aggr_expr {
        expr.apply(|expr| {
            let Expr::AggregateFunction(function) = expr else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let params = &function.params;
            irrelevant = !params.args.iter().any(Expr::is_volatile)
                && !params.filter.as_deref().is_some_and(Expr::is_volatile)
                && (is_order_irrelevant_aggregate(&function.func, &params.args, schema)?
                    || is_average_of_integers(&function.func, &params.args, schema)?);
            Ok(if irrelevant {
                TreeNodeRecursion::Jump
            } else {
                TreeNodeRecursion::Stop
            })
        })?;
        if !irrelevant {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Returns whether the aggregate function is an average of integers, whose argument type
/// coercion casts to a floating-point type. Spark treats the average of integers as
/// order-irrelevant, although it also adds them as floating-point numbers.
fn is_average_of_integers(udf: &AggregateUDF, args: &[Expr], schema: &DFSchema) -> Result<bool> {
    if !(udf == average::avg_udaf().as_ref() || udf.inner().is::<TryAvgFunction>()) {
        return Ok(false);
    }
    match args.first() {
        Some(Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. })) => {
            Ok(expr.get_type(schema)?.is_integer())
        }
        _ => Ok(false),
    }
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
        Ok(LogicalPlanBuilder::from(LogicalPlan::Extension(
            Extension {
                node: Arc::new(SortWithinPartitionsNode::new(
                    Arc::new(input),
                    vec![col("v").sort(true, true)],
                    None,
                )),
            },
        )))
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
        let plan = optimize(
            sort_within_partitions("t")?
                .aggregate(Vec::<Expr>::new(), vec![max(col("v")), sum(col("v"))])?,
        )?;
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
        let plan = optimize(
            sorted("l")?
                .join_on(
                    scan("r")?.build()?,
                    JoinType::LeftSemi,
                    vec![col("l.k").eq(col("r.k"))],
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("l.v"))])?,
        )?;
        assert_eq!(sorts(&plan), 0, "{plan}");
        Ok(())
    }

    fn sort_above_join(plan: &str) -> bool {
        matches!((plan.find("Sort:"), plan.find("Join:")), (Some(sort), Some(join)) if sort < join)
    }

    #[test]
    fn left_semi_and_anti_joins_are_moved_below_the_sorts_of_their_left_input() -> Result<()> {
        for join_type in [JoinType::LeftSemi, JoinType::LeftAnti] {
            let left = sorted("l")?
                .project(vec![
                    (col("k") + lit(0_i64)).alias("a"),
                    col("v"),
                    lit(1_i64).alias("c"),
                ])?
                .alias("s")?;
            let plan = optimize(left.join_on(
                sorted("r")?.build()?,
                join_type,
                vec![col("s.a").eq(col("r.k")), col("s.c").lt(col("r.v"))],
            )?)?;
            assert_eq!(sorts(&plan), 1, "{plan}");
            assert!(sort_above_join(&plan), "{plan}");
            assert!(plan.contains("l.k = r.k"), "{plan}");
            assert!(plan.contains("Int64(1) < r.v"), "{plan}");
        }
        let required = LogicalPlan::Extension(Extension {
            node: Arc::new(RequiredSortNode::new(
                Arc::new(scan("l")?.build()?),
                vec![col("v").sort(false, true)],
                None,
                false,
            )),
        });
        let plan = optimize(LogicalPlanBuilder::from(required).join_on(
            scan("r")?.build()?,
            JoinType::LeftSemi,
            vec![col("l.k").eq(col("r.k"))],
        )?)?;
        assert!(sort_above_join(&plan), "{plan}");
        Ok(())
    }

    #[test]
    fn left_semi_joins_are_moved_below_columns_that_their_condition_does_not_use() -> Result<()> {
        let left = sorted("l")?.project(vec![col("k"), (col("v") + lit(1_i64)).alias("b")])?;
        let plan = optimize(left.join_on(
            scan("r")?.build()?,
            JoinType::LeftSemi,
            vec![col("l.k").eq(col("r.k"))],
        )?)?;
        assert!(sort_above_join(&plan), "{plan}");
        Ok(())
    }

    #[test]
    fn left_semi_joins_are_not_moved_below_selecting_sorts_or_volatile_expressions() -> Result<()> {
        let inputs = [
            (
                sorted("l")?.project(vec![(col("k") + lit(1_i64)).alias("a")])?,
                col("a"),
            ),
            (
                scan("l")?.sort_with_limit(vec![col("v").sort(true, true)], Some(2))?,
                col("l.k"),
            ),
            (
                scan("l")?.sort(vec![random().sort(true, true)])?,
                col("l.k"),
            ),
            (
                sorted("l")?.project(vec![col("k"), random().alias("w")])?,
                col("l.k"),
            ),
        ];
        for (left, key) in inputs {
            let plan = optimize(left.join_on(
                scan("r")?.build()?,
                JoinType::LeftSemi,
                vec![key.eq(col("r.k"))],
            )?)?;
            assert_eq!(sorts(&plan), 1, "{plan}");
            assert!(!sort_above_join(&plan), "{plan}");
        }
        Ok(())
    }

    #[test]
    fn sorts_below_aggregates_with_volatile_inputs_are_kept() -> Result<()> {
        let plan =
            optimize(sorted("t")?.aggregate(vec![random().lt(lit(0.5))], vec![max(col("v"))])?)?;
        assert_eq!(sorts(&plan), 1, "{plan}");
        Ok(())
    }

    #[test]
    fn sorts_below_averages_of_integers_are_removed() -> Result<()> {
        let plan = optimize(sorted("t")?.aggregate(
            Vec::<Expr>::new(),
            vec![average::avg_udaf().call(vec![cast(col("v"), DataType::Float64)])],
        )?)?;
        assert_eq!(sorts(&plan), 0, "{plan}");
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
