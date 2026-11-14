//! Finds whether the plan of a shared CTE holds its whole output before it returns it.

use std::sync::Arc;

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode};
use datafusion_common::JoinType;

/// Returns whether the plan holds its whole output before it returns any row of it, in
/// memory that the plan itself takes, so that the rows that a shared CTE keeps for a
/// reference that reads it late are no more than what each evaluation of its plan holds
/// anyway. The plan is a hash aggregate, a sort, or a window over a sort that adds
/// fixed-width columns, below operators that only filter, select or move its rows. A
/// semi or anti join is followed to the side whose rows it returns, as for `INTERSECT`
/// and `EXCEPT`.
pub fn holds_output(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let plan = through(plan, true);
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        holds_groups(aggregate)
    } else if plan.is::<SortExec>() {
        true
    } else if let Some(window) = plan.downcast_ref::<BoundedWindowAggExec>() {
        holds_window(plan, window.window_expr())
    } else if let Some(window) = plan.downcast_ref::<WindowAggExec>() {
        holds_window(plan, window.window_expr())
    } else if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        holds_semi_join(join.join_type(), join.left(), join.right())
    } else if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        holds_semi_join(join.join_type(), join.left(), join.right())
    } else {
        false
    }
}

/// Whether the aggregate returns its groups only after its input ends. An aggregate over
/// ordered input returns each group once the next group starts.
fn holds_groups(aggregate: &AggregateExec) -> bool {
    matches!(
        aggregate.mode(),
        AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned
    ) && (aggregate.group_expr().is_empty()
        || matches!(aggregate.input_order_mode(), InputOrderMode::Linear))
}

/// Whether the window holds its input in a sort below it, and adds columns of a fixed
/// width to each row, unlike a list of the rows of a frame.
fn holds_window(plan: &Arc<dyn ExecutionPlan>, expressions: &[Arc<dyn WindowExpr>]) -> bool {
    expressions.iter().all(|expr| {
        expr.field()
            .is_ok_and(|field| field.data_type().primitive_width().is_some())
    }) && plan
        .children()
        .first()
        .is_some_and(|input| through(input, false).is::<SortExec>())
}

fn holds_semi_join(
    join_type: &JoinType,
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
) -> bool {
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => holds_output(left),
        JoinType::RightSemi | JoinType::RightAnti => holds_output(right),
        _ => false,
    }
}

/// Returns the input of the operators that only move the rows of their input or select
/// their columns, and of the operators that filter them as well if `filters` is set.
fn through(plan: &Arc<dyn ExecutionPlan>, filters: bool) -> &Arc<dyn ExecutionPlan> {
    let mut plan = plan;
    loop {
        let moves = plan.is::<RepartitionExec>()
            || plan.is::<CoalescePartitionsExec>()
            || plan.is::<SortPreservingMergeExec>()
            // The subqueries return one value each, which the input reads.
            || plan.is::<ScalarSubqueryExec>()
            || plan
                .downcast_ref::<ProjectionExec>()
                .is_some_and(|projection| {
                    // A computed column can be larger than what the plan holds.
                    projection
                        .expr()
                        .iter()
                        .all(|expr| expr.expr.is::<Column>())
                });
        match plan.children().first() {
            Some(input) if moves || (filters && plan.is::<FilterExec>()) => plan = input,
            _ => return plan,
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::dataframe::DataFrame;
    use datafusion::functions_aggregate::array_agg::array_agg_udaf;
    use datafusion::functions_aggregate::expr_fn::{count, sum};
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::expr::WindowFunction;
    use datafusion::logical_expr::{Expr, ExprFunctionExt, WindowFunctionDefinition, col, lit};
    use datafusion::physical_expr::expressions::col as physical_col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    fn table(ctx: &SessionContext, names: [&str; 2]) -> DataFrame {
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let column = Arc::new(Int64Array::from_iter_values(0..100));
        let batch = RecordBatch::try_new(schema, vec![column.clone(), column]).unwrap();
        ctx.read_batch(batch).unwrap()
    }

    async fn holds(df: DataFrame) -> bool {
        holds_output(&df.create_physical_plan().await.unwrap())
    }

    #[tokio::test]
    async fn plans_that_hold_their_output() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let t = || table(&ctx, ["k", "v"]);
        let u = || table(&ctx, ["k2", "v2"]);
        let grouped = t()
            .aggregate(vec![col("k")], vec![sum(col("v")).alias("s")])
            .unwrap();
        assert!(holds(grouped.clone()).await);
        let selected = grouped.clone().select(vec![col("s")]).unwrap();
        assert!(holds(selected).await);
        let filtered = grouped.filter(col("s").gt(lit(10))).unwrap();
        assert!(holds(filtered).await);
        let total = t().aggregate(vec![], vec![sum(col("v"))]).unwrap();
        assert!(holds(total).await);
        assert!(holds(t().distinct().unwrap()).await);
        let sorted = t().sort(vec![col("v").sort(true, false)]).unwrap();
        assert!(holds(sorted).await);
        let ranked = row_number()
            .partition_by(vec![col("k")])
            .order_by(vec![col("v").sort(true, false)])
            .build()
            .unwrap();
        assert!(holds(t().window(vec![ranked]).unwrap()).await);
        assert!(holds(t().intersect_distinct(u()).unwrap()).await);
        assert!(holds(t().except_distinct(u()).unwrap()).await);
        // `INTERSECT ALL` numbers the duplicate rows of each side.
        let numbered = |df: DataFrame, names: [&str; 2], alias: &str| {
            let number = row_number()
                .partition_by(vec![col(names[0]), col(names[1])])
                .build()
                .unwrap()
                .alias(alias);
            df.window(vec![number]).unwrap()
        };
        let all = numbered(t(), ["k", "v"], "n")
            .join(
                numbered(u(), ["k2", "v2"], "n2"),
                datafusion::logical_expr::JoinType::LeftSemi,
                &["k", "v", "n"],
                &["k2", "v2", "n2"],
                None,
            )
            .unwrap()
            .select(vec![col("k"), col("v")])
            .unwrap();
        assert!(holds(all).await);
    }

    #[tokio::test]
    async fn plans_that_return_rows_before_their_input_ends() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let t = || table(&ctx, ["k", "v"]);
        let u = || table(&ctx, ["k2", "v2"]);
        assert!(!holds(t()).await);
        assert!(!holds(t().filter(col("k").gt(lit(1))).unwrap()).await);
        let joined = t()
            .join(
                u(),
                datafusion::logical_expr::JoinType::Inner,
                &["k"],
                &["k2"],
                None,
            )
            .unwrap();
        assert!(!holds(joined.clone()).await);
        assert!(!holds(joined.select(vec![col("k")]).unwrap()).await);
        let left = t()
            .join(
                u(),
                datafusion::logical_expr::JoinType::Left,
                &["k"],
                &["k2"],
                None,
            )
            .unwrap();
        assert!(!holds(left).await);
        // A scan joined with groups by the join keys is not followed.
        let groups = u()
            .aggregate(vec![col("k2")], vec![count(col("v2")).alias("c")])
            .unwrap();
        let joined = t()
            .join(
                groups,
                datafusion::logical_expr::JoinType::Inner,
                &["k"],
                &["k2"],
                None,
            )
            .unwrap();
        assert!(!holds(joined).await);
        let grouped = t()
            .aggregate(vec![col("k")], vec![sum(col("v")).alias("s")])
            .unwrap();
        // A computed column can be larger than the groups.
        let computed = grouped
            .select(vec![(col("s") * lit(2)).alias("x")])
            .unwrap();
        assert!(!holds(computed).await);
        // A window that collects the rows of each frame returns more than it holds.
        let listed = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(array_agg_udaf()),
            vec![col("v")],
        ))
        .partition_by(vec![col("k")])
        .order_by(vec![col("v").sort(true, false)])
        .build()
        .unwrap();
        assert!(!holds(t().window(vec![listed]).unwrap()).await);
        let semi = t()
            .join(
                u(),
                datafusion::logical_expr::JoinType::LeftSemi,
                &["k"],
                &["k2"],
                None,
            )
            .unwrap();
        assert!(!holds(semi).await);
        assert!(!holds(t().union(u()).unwrap()).await);
    }

    #[tokio::test]
    async fn an_aggregate_over_ordered_input_returns_groups_early() {
        let ctx = SessionContext::new();
        let input = table(&ctx, ["k", "v"])
            .create_physical_plan()
            .await
            .unwrap();
        let schema = input.schema();
        let key = physical_col("k", &schema).unwrap();
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::clone(&key))]).unwrap();
        let sorted = Arc::new(SortExec::new(ordering, input));
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(key, "k".to_string())]),
            vec![],
            vec![],
            sorted,
            schema,
        )
        .unwrap();
        assert!(!matches!(
            aggregate.input_order_mode(),
            InputOrderMode::Linear
        ));
        assert!(!holds_output(
            &(Arc::new(aggregate) as Arc<dyn ExecutionPlan>)
        ));
    }
}
