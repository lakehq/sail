use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::equivalence::ProjectionMapping;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::enforce_distribution::{
    DistributionContext, ensure_distribution,
};
use datafusion::physical_optimizer::output_requirements::{
    OutputRequirementExec, OutputRequirements,
};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use sail_physical_plan::tail::TailExec;

/// Tail consumes its input order even when a projection hides the sort expressions.
/// Capture that order where its expressions are still available, using the same
/// requirements as a query output. The normal removal pass drops these temporary nodes.
#[derive(Debug, Default)]
pub struct PreserveTailInputOrder;

impl PhysicalOptimizerRule for PreserveTailInputOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(tail) = plan.downcast_ref::<TailExec>() else {
                return Ok(Transformed::no(plan));
            };
            let input =
                OutputRequirements::new_add_mode().optimize(Arc::clone(tail.input()), config)?;
            if Arc::ptr_eq(tail.input(), &input) {
                return Ok(Transformed::no(plan));
            }
            Ok(Transformed::yes(plan.replace_children(
                vec![input],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?))
        })
        .data()
    }

    fn name(&self) -> &str {
        "PreserveTailInputOrder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Prevent optional repartitioning from changing the sequence consumed by Tail.
/// The sort origin can still use the usual parallel planning; its transparent
/// consumers must retain that sequence when projections hide any of its keys.
pub(super) fn ensure_tail_distribution(
    context: DistributionContext,
    config: &ConfigOptions,
) -> Result<DistributionContext> {
    let mut ordered_config = config.clone();
    ordered_config.optimizer.enable_round_robin_repartition = false;
    ordered_config.optimizer.repartition_file_scans = false;
    // Downward data marks the sequence-sensitive path. On the way up that flag
    // is consumed before ensure_distribution resets it to distribution tracking.
    // Use TreeNode's protected traversal rather than adding unguarded recursion.
    context
        .transform_down_up(
            |mut context| {
                let tail = context.plan.downcast_ref::<TailExec>();
                let tail_needs_sequence = tail
                    .map(|tail| input_order(tail.input()))
                    .transpose()?
                    .is_some_and(|order| matches!(order, InputOrder::Hidden));
                let origin =
                    context.plan.is::<SortExec>() || context.plan.is::<SortPreservingMergeExec>();
                let protected = tail_needs_sequence || (context.data && !origin);
                // OffsetBoundary hides sort propagation but forwards the same rows.
                let offset = context.plan.is::<crate::offset::OffsetBoundary>();
                let maintains = context.plan.maintains_input_order();
                context.data = protected;
                for (index, child) in context.children.iter_mut().enumerate() {
                    child.data = tail_needs_sequence || (protected && (offset || maintains[index]));
                }
                Ok(Transformed::no(context))
            },
            |context| {
                let config = if context.data {
                    &ordered_config
                } else {
                    config
                };
                ensure_distribution(context, config)
            },
        )
        .data()
}

/// Whether the complete ordering origin is still expressible at this point.
/// A visible prefix is insufficient: a hidden secondary key selects different
/// rows within ties. Unordered input retains DataFusion's normal parallelism.
enum InputOrder {
    Unordered,
    Hidden,
    Visible(LexOrdering),
}

fn input_order(plan: &Arc<dyn ExecutionPlan>) -> Result<InputOrder> {
    let mut path = Vec::new();
    let mut current = Arc::clone(plan);
    let mut order = loop {
        // A nested Tail may itself hide part of the ordering it consumed. Keep
        // its sequence opaque and avoid repeatedly traversing nested Tail chains.
        if current.is::<TailExec>() {
            break InputOrder::Hidden;
        }
        let origin = if let Some(sort) = current.downcast_ref::<SortExec>() {
            Some(sort.expr())
        } else {
            current
                .downcast_ref::<SortPreservingMergeExec>()
                .map(|merge| merge.expr())
        };
        if let Some(order) = origin {
            break InputOrder::Visible(order.clone());
        }
        let children = current.children();
        let same_rows = current.is::<crate::offset::OffsetBoundary>();
        let maintained = current.maintains_input_order();
        let mut inputs = maintained
            .iter()
            .enumerate()
            .filter_map(|(index, maintained)| maintained.then_some(index));
        let input = if same_rows { Some(0) } else { inputs.next() };
        if !same_rows && inputs.next().is_some() {
            break InputOrder::Hidden;
        }
        let Some(index) = input.filter(|index| *index < children.len()) else {
            break current
                .output_ordering()
                .cloned()
                .map(InputOrder::Visible)
                .unwrap_or(InputOrder::Unordered);
        };
        let child = Arc::clone(children[index]);
        path.push((current, index));
        current = child;
    };
    // Check before mapping: a projection can replace a volatile expression with
    // a Column, which would otherwise erase the evidence that it is unsafe.
    if let InputOrder::Visible(ordering) = &order
        && ordering_is_volatile(ordering)?
    {
        order = InputOrder::Hidden;
    }
    for (plan, index) in path.into_iter().rev() {
        let InputOrder::Visible(mut ordering) = order else {
            continue;
        };
        let child = plan.children()[index];
        let mapping = if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            Some(
                projection
                    .projection_expr()
                    .projection_mapping(&child.schema())?,
            )
        } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            filter
                .projection()
                .as_ref()
                .map(|indices| ProjectionMapping::from_indices(indices, &child.schema()))
                .transpose()?
        } else if plan.is::<OutputRequirementExec>()
            || plan.is::<crate::offset::OffsetBoundary>()
            || plan.is::<GlobalLimitExec>()
            || plan.is::<LocalLimitExec>()
            || plan.is::<CooperativeExec>()
        {
            None
        } else {
            order = InputOrder::Hidden;
            continue;
        };
        if let Some(mapping) = mapping {
            let mapped = ordering
                .into_iter()
                .map(|sort| {
                    child
                        .equivalence_properties()
                        .project_expr(&sort.expr, &mapping)
                        .map(|expr| PhysicalSortExpr::new(expr, sort.options))
                })
                .collect::<Option<Vec<_>>>();
            let Some(mapped) = mapped.and_then(LexOrdering::new) else {
                order = InputOrder::Hidden;
                continue;
            };
            ordering = mapped;
        }
        let volatile = ordering_is_volatile(&ordering)?;
        order = if !volatile
            && plan
                .equivalence_properties()
                .ordering_satisfy(ordering.clone())?
        {
            InputOrder::Visible(ordering)
        } else {
            InputOrder::Hidden
        };
    }
    Ok(order)
}

fn ordering_is_volatile(ordering: &LexOrdering) -> Result<bool> {
    ordering.iter().try_fold(false, |found, sort| {
        Ok(found || sort.expr.exists(|expr| Ok(expr.is_volatile_node()))?)
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::common::internal_datafusion_err;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::execution::config::SessionConfig;
    use datafusion::logical_expr::{Volatility, create_udf};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{
        LexOrdering, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
    };
    use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{ExecutionPlanProperties, collect, displayable};

    use super::*;
    use crate::{PhysicalOptimizerOptions, get_physical_optimizers};

    fn source(rows: i64, partitions: usize) -> Result<Arc<dyn ExecutionPlan>> {
        source_with_ordering(rows, partitions, false)
    }

    fn source_with_ordering(
        rows: i64,
        partitions: usize,
        ordered: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batches = (0..partitions)
            .map(|partition| {
                let start = rows * partition as i64 / partitions as i64;
                let end = rows * (partition + 1) as i64 / partitions as i64;
                (start..end)
                    .step_by(128)
                    .map(|batch_start| {
                        let batch_end = (batch_start + 128).min(end);
                        Ok(RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![
                                Arc::new(Int64Array::from_iter_values(batch_start..batch_end)),
                                Arc::new(Int64Array::from_iter_values(
                                    (batch_start..batch_end).map(|key| 100_000 - key),
                                )),
                            ],
                        )?)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut source = MemorySourceConfig::try_new(&batches, schema, None)?;
        if ordered {
            source = source.try_with_sort_information(vec![ordering("key", 0, false)?])?;
        }
        Ok(datafusion_datasource::source::DataSourceExec::from_data_source(source))
    }

    fn ordering(name: &str, index: usize, descending: bool) -> Result<LexOrdering> {
        LexOrdering::new([PhysicalSortExpr::new(
            Arc::new(Column::new(name, index)),
            datafusion::arrow::compute::SortOptions {
                descending,
                nulls_first: false,
            },
        )])
        .ok_or_else(|| internal_datafusion_err!("nonempty ordering"))
    }

    fn hidden_sort(rows: i64, partitions: usize, computed: bool) -> Result<Arc<dyn ExecutionPlan>> {
        let sorted = Arc::new(SortExec::new(
            ordering("key", 0, true)?,
            source(rows, partitions)?,
        ));
        project_hidden(sorted, computed)
    }

    fn project_hidden(
        sorted: Arc<dyn ExecutionPlan>,
        computed: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let column = Arc::new(Column::new("value", 1)) as Arc<dyn PhysicalExpr>;
        let value = if computed {
            let udf = create_udf(
                "identity_projection",
                vec![DataType::Int64],
                DataType::Int64,
                Volatility::Volatile,
                Arc::new(|args| Ok(args[0].clone())),
            );
            Arc::new(ScalarFunctionExpr::try_new(
                Arc::new(udf),
                vec![column],
                &sorted.schema(),
                Arc::new(ConfigOptions::default()),
            )?) as Arc<dyn PhysicalExpr>
        } else {
            column
        };
        Ok(Arc::new(ProjectionExec::try_new(
            vec![(value, "payload".to_string())],
            sorted,
        )?))
    }

    fn optimize(mut plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 4;
        config.execution.batch_size = datafusion::common::config::ConfigNonZeroUsize::try_new(128)?;
        for rule in get_physical_optimizers(PhysicalOptimizerOptions::default()) {
            plan = rule.optimize(plan, &config)?;
        }
        assert!(!plan.exists(|plan| Ok(plan.is::<OutputRequirementExec>()))?);
        plan.apply(|node| {
            if let Some(tail) = node.downcast_ref::<TailExec>()
                && matches!(input_order(tail.input())?, InputOrder::Hidden) {
                let mut input = Arc::clone(tail.input());
                loop {
                    assert_eq!(input.output_partitioning().partition_count(), 1,
                        "Tail's ordered suffix must not fan out: {}", displayable(plan.as_ref()).indent(true));
                    if input.is::<SortExec>()
                        || input.is::<datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec>()
                        || input.children().len() != 1 {
                        break;
                    }
                    input = Arc::clone(input.children()[0]);
                }
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    async fn values(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<i64>> {
        let context = TaskContext::default().with_session_config(
            SessionConfig::new()
                .with_target_partitions(4)
                .with_batch_size(128),
        );
        let batches = collect(plan, Arc::new(context)).await?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect())
    }

    #[tokio::test]
    async fn hidden_sort_keys_survive_tail_and_parent_sort() -> Result<()> {
        for partitions in [1, 3] {
            for computed in [false, true] {
                for fetch in [3, 9_000] {
                    let input = hidden_sort(20_000, partitions, computed)?;
                    let tail = Arc::new(TailExec::new(input, fetch));
                    let schema = tail.schema();
                    let parent = Arc::new(SortExec::new(ordering("payload", 0, true)?, tail));
                    let mut plan = optimize(parent)?;
                    for _ in 0..2 {
                        assert_eq!(plan.schema(), schema);
                        assert_eq!(
                            values(Arc::clone(&plan)).await?,
                            (0..fetch as i64)
                                .map(|key| 100_000 - key)
                                .collect::<Vec<_>>(),
                            "partitions={partitions}, computed={computed}, fetch={fetch}: {}",
                            displayable(plan.as_ref()).indent(true)
                        );
                        plan = optimize(plan)?;
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn hidden_intrinsic_order_survives_repeated_optimization() -> Result<()> {
        for computed in [false, true] {
            for fetch in [3, 9_000] {
                let source = source_with_ordering(20_000, 1, true)?;
                // The optimizer can remove this redundant sort in the first pass. The
                // second pass must still preserve the source order hidden by projection.
                let sorted = Arc::new(SortExec::new(ordering("key", 0, false)?, source));
                let input = project_hidden(sorted, computed)?;
                let mut plan = optimize(Arc::new(TailExec::new(input, fetch)))?;
                for _ in 0..2 {
                    assert_eq!(
                        values(Arc::clone(&plan)).await?,
                        ((20_000 - fetch as i64)..20_000)
                            .map(|key| 100_000 - key)
                            .collect::<Vec<_>>(),
                        "computed={computed}, fetch={fetch}: {}",
                        displayable(plan.as_ref()).indent(true)
                    );
                    plan = optimize(plan)?;
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn nested_tail_keeps_hidden_sort_order() -> Result<()> {
        let inner = Arc::new(TailExec::new(hidden_sort(20_000, 3, true)?, 10));
        let outer = Arc::new(TailExec::new(inner, 3));
        let plan = optimize(outer)?;
        assert_eq!(
            values(Arc::clone(&plan)).await?,
            vec![99_998, 99_999, 100_000]
        );
        assert_eq!(
            values(optimize(plan)?).await?,
            vec![99_998, 99_999, 100_000]
        );
        Ok(())
    }

    #[tokio::test]
    async fn visible_keys_and_unordered_inputs_keep_projection_parallelism() -> Result<()> {
        for ordered in [false, true] {
            let input = source(20_000, 1)?;
            let input: Arc<dyn ExecutionPlan> = if ordered {
                Arc::new(SortExec::new(ordering("key", 0, true)?, input))
            } else {
                input
            };
            let projected = project_hidden(input, true)?;
            let projection = projected
                .downcast_ref::<ProjectionExec>()
                .ok_or_else(|| internal_datafusion_err!("expected projection"))?;
            let mut expressions = projection.expr().to_vec();
            expressions.push(
                (
                    Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
                    "renamed_key".to_string(),
                )
                    .into(),
            );
            let input = Arc::new(ProjectionExec::try_new(
                expressions,
                Arc::clone(projection.input()),
            )?);
            let mut plan = optimize(Arc::new(TailExec::new(input, 9_000)))?;
            for _ in 0..2 {
                assert!(
                    plan.exists(|node| Ok(node.downcast_ref::<ProjectionExec>().is_some_and(
                        |projection| {
                            projection.benefits_from_input_partitioning()[0]
                                && projection.input().output_partitioning().partition_count() > 1
                        }
                    )))?,
                    "visible={ordered}: {}",
                    displayable(plan.as_ref()).indent(true)
                );
                let output = values(Arc::clone(&plan)).await?;
                if ordered {
                    assert_eq!(
                        output,
                        (0..9_000)
                            .rev()
                            .map(|key| 100_000 - key)
                            .collect::<Vec<_>>()
                    );
                } else {
                    assert_eq!(output.len(), 9_000);
                }
                plan = optimize(plan)?;
            }
        }
        Ok(())
    }

    #[test]
    fn a_visible_ordering_prefix_does_not_expose_hidden_tie_breakers() -> Result<()> {
        let sort_order = LexOrdering::new([
            ordering("key", 0, false)?.first().clone(),
            ordering("value", 1, true)?.first().clone(),
        ])
        .ok_or_else(|| internal_datafusion_err!("nonempty ordering"))?;
        let sorted = Arc::new(SortExec::new(sort_order, source(20, 1)?));
        let input: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
                "visible_key".to_string(),
            )],
            sorted,
        )?);
        assert!(input.output_ordering().is_some());
        assert!(matches!(input_order(&input)?, InputOrder::Hidden));
        Ok(())
    }

    #[tokio::test]
    async fn offset_does_not_break_hidden_tail_order() -> Result<()> {
        let offset = Arc::new(GlobalLimitExec::new(hidden_sort(20_000, 3, true)?, 7, None));
        let mut plan = optimize(Arc::new(TailExec::new(offset, 9_000)))?;
        for _ in 0..2 {
            assert_eq!(
                values(Arc::clone(&plan)).await?,
                (0..9_000)
                    .rev()
                    .map(|key| 100_000 - key)
                    .collect::<Vec<_>>()
            );
            plan = optimize(plan)?;
        }
        Ok(())
    }

    #[test]
    fn tail_requirements_capture_is_idempotent_and_ignores_other_plans() -> Result<()> {
        let input = hidden_sort(10, 1, false)?;
        let config = ConfigOptions::default();
        let unchanged = PreserveTailInputOrder.optimize(Arc::clone(&input), &config)?;
        assert!(Arc::ptr_eq(&input, &unchanged));
        let tail = Arc::new(TailExec::new(input, 3));
        let once = PreserveTailInputOrder.optimize(tail, &config)?;
        let twice = PreserveTailInputOrder.optimize(Arc::clone(&once), &config)?;
        assert_eq!(
            displayable(once.as_ref()).indent(true).to_string(),
            displayable(twice.as_ref()).indent(true).to_string()
        );
        Ok(())
    }
}
