// SPDX-License-Identifier: Apache-2.0

//! Finds whether the plan of a shared CTE holds its whole output before it returns it,
//! and whether the operators that read a shared CTE hold all the rows that they read.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, GroupClusteringMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion_common::JoinType;

use crate::shared_cte::SharedCteScanExec;

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
        || matches!(aggregate.group_clustering_mode(), GroupClusteringMode::None))
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

/// Returns the identifiers of the shared CTEs that the plan reads into an operator that
/// does not hold all the rows that it reads. The other shared CTEs are read only by the
/// build sides of hash joins, through operators that only move the rows or select their
/// columns. A build side holds all its rows until its join ends, so the rows that such a
/// shared CTE keeps for a reference that reads it late are no more than what the
/// reference that has read them holds, and than what each reference holds in the end,
/// whether the plan of the shared CTE holds its output or not. At worst, when a join
/// ends before the build side of another join starts to read, the shared CTE keeps one
/// more copy of its output, with the columns that any reference reads, than the memory
/// that the build sides take when each of them evaluates the CTE itself.
pub fn ctes_read_without_holding(plan: &Arc<dyn ExecutionPlan>) -> HashSet<usize> {
    fn visit(plan: &Arc<dyn ExecutionPlan>, held: bool, ids: &mut HashSet<usize>) {
        if let Some(scan) = plan.downcast_ref::<SharedCteScanExec>() {
            if !held {
                ids.insert(scan.cte().id());
            }
        } else if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            visit(join.left(), true, ids);
            visit(join.right(), false, ids);
        } else {
            // An operator that moves rows moves the rows of its first input.
            let held = held && moves(plan);
            for (index, input) in plan.children().into_iter().enumerate() {
                visit(input, held && index == 0, ids);
            }
        }
    }
    let mut ids = HashSet::new();
    visit(plan, false, &mut ids);
    ids
}

/// Returns whether the operator only moves the rows of its first input or selects their
/// columns.
fn moves(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<RepartitionExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<SortPreservingMergeExec>()
        // The subqueries return one value each, which the input reads.
        || plan.is::<ScalarSubqueryExec>()
        || plan
            .downcast_ref::<ProjectionExec>()
            .is_some_and(|projection| {
                // A computed column can be larger than the columns that it is computed from.
                projection
                    .expr()
                    .iter()
                    .all(|expr| expr.expr.is::<Column>())
            })
}

/// Returns the input of the operators that only move the rows of their input or select
/// their columns, and of the operators that filter them as well if `filters` is set.
fn through(plan: &Arc<dyn ExecutionPlan>, filters: bool) -> &Arc<dyn ExecutionPlan> {
    let mut plan = plan;
    loop {
        match plan.children().first() {
            Some(input) if moves(plan) || (filters && plan.is::<FilterExec>()) => plan = input,
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
    use datafusion::logical_expr::{
        Expr, ExprFunctionExt, Operator, WindowFunctionDefinition, col, lit,
    };
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal, col as physical_col};
    use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::{NullEquality, ScalarValue, Statistics};

    use super::*;
    use crate::shared_cte::SharedCte;

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
            aggregate.group_clustering_mode(),
            GroupClusteringMode::None
        ));
        assert!(!holds_output(
            &(Arc::new(aggregate) as Arc<dyn ExecutionPlan>)
        ));
    }

    #[tokio::test]
    async fn references_that_hold_the_rows_that_they_read() {
        let ctx = SessionContext::new();
        let table = table(&ctx, ["k", "v"])
            .create_physical_plan()
            .await
            .unwrap();
        let schema = table.schema();
        let key = physical_col("k", &schema).unwrap();
        let reference = |id: usize| {
            let cte = Arc::new(SharedCte::new(
                id,
                "c".to_string(),
                Partitioning::UnknownPartitioning(1),
            ));
            let statistics = Statistics::new_unknown(&schema);
            Arc::new(SharedCteScanExec::new(
                cte,
                Arc::clone(&schema),
                &statistics,
                None,
            )) as Arc<dyn ExecutionPlan>
        };
        let join = |build: Arc<dyn ExecutionPlan>, probe: Arc<dyn ExecutionPlan>| {
            let join = HashJoinExec::try_new(
                build,
                probe,
                vec![(Arc::clone(&key), Arc::clone(&key))],
                None,
                &JoinType::RightSemi,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap();
            Arc::new(join) as Arc<dyn ExecutionPlan>
        };
        let select = |expr: Arc<dyn PhysicalExpr>, input: Arc<dyn ExecutionPlan>| {
            let expr = vec![ProjectionExpr::new(expr, "k".to_string())];
            Arc::new(ProjectionExec::try_new(expr, input).unwrap()) as Arc<dyn ExecutionPlan>
        };
        let repartition = |input: Arc<dyn ExecutionPlan>| {
            let partitioning = Partitioning::Hash(vec![Arc::clone(&key)], 4);
            Arc::new(RepartitionExec::try_new(input, partitioning).unwrap())
                as Arc<dyn ExecutionPlan>
        };
        let positive = || {
            let zero = Arc::new(Literal::new(ScalarValue::Int64(Some(0))));
            Arc::new(BinaryExpr::new(Arc::clone(&key), Operator::Gt, zero)) as Arc<dyn PhysicalExpr>
        };
        let streamed = |plan: Arc<dyn ExecutionPlan>| {
            let mut ids = ctes_read_without_holding(&plan)
                .into_iter()
                .collect::<Vec<_>>();
            ids.sort();
            ids
        };

        // The build side of a hash join holds its rows, also when they are selected and
        // repartitioned on the way, and when the join is the build side of another join.
        let build = repartition(select(Arc::clone(&key), reference(0)));
        assert!(streamed(join(build, Arc::clone(&table))).is_empty());
        let inner = join(reference(0), Arc::clone(&table));
        assert!(streamed(join(inner, Arc::clone(&table))).is_empty());

        // The other readers return or drop rows as they read them.
        assert_eq!(streamed(reference(0)), vec![0]);
        assert_eq!(streamed(join(Arc::clone(&table), reference(0))), vec![0]);
        assert_eq!(streamed(join(reference(0), reference(0))), vec![0]);
        assert_eq!(streamed(join(reference(0), reference(1))), vec![1]);
        let inner = join(Arc::clone(&table), reference(0));
        assert_eq!(streamed(join(inner, Arc::clone(&table))), vec![0]);
        let filtered = Arc::new(FilterExec::try_new(positive(), reference(0)).unwrap());
        assert_eq!(streamed(join(filtered, Arc::clone(&table))), vec![0]);
        let computed = select(positive(), reference(0));
        assert_eq!(streamed(join(computed, Arc::clone(&table))), vec![0]);
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(Arc::clone(&key), "k".to_string())]),
            vec![],
            vec![],
            reference(0),
            Arc::clone(&schema),
        )
        .unwrap();
        assert_eq!(
            streamed(join(Arc::new(aggregate), Arc::clone(&table))),
            vec![0]
        );
    }
}
