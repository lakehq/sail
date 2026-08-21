use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_expr::{
    Distribution, LexOrdering, LexRequirement, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr, PhysicalSortRequirement, ScalarFunctionExpr,
};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{
    HashJoinExec, HashJoinExecBuilder, NestedLoopJoinExec, NestedLoopJoinExecBuilder,
    PiecewiseMergeJoinExec, SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr::projection::ProjectionExpr;
use sail_catalog_system::physical_plan::SystemTableExec;
use sail_common_datafusion::physical_expr::lazy_scalar::LazyScalarExpr;
use sail_delta_lake::physical_plan::DeletionVectorWriterExec;
use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use sail_physical_plan::streaming::filter::StreamFilterExec;

type PhysicalJoinKey = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);
type NamedPhysicalExpression = (Arc<dyn PhysicalExpr>, String);

/// Lowers every lazy scalar marker owned by a DataFusion 54 physical expression host.
///
/// DataFusion 54 has no generic `ExecutionPlan` expression rewrite hook, so each host is rebuilt
/// here at the logical-to-physical planning boundary. Keeping the cases in one rule prevents
/// projection-only behavior. The logical marker is deliberately not eagerly callable, so an
/// unsupported host fails closed instead of silently changing evaluation semantics.
pub(super) fn lower_lazy_scalars(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| lower_plan_node(plan, target_partitions))
        .data()
}

fn lower_plan_node(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return lower_projection(Arc::clone(&plan), projection);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        return lower_filter(Arc::clone(&plan), filter);
    }
    if let Some(sort) = plan.downcast_ref::<SortExec>() {
        return lower_sort(Arc::clone(&plan), sort);
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        return lower_repartition(Arc::clone(&plan), repartition);
    }
    if let Some(repartition) = plan.downcast_ref::<ExplicitRepartitionExec>() {
        return lower_explicit_repartition(Arc::clone(&plan), repartition);
    }
    if let Some(requirement) = plan.downcast_ref::<OutputRequirementExec>() {
        return lower_output_requirement(Arc::clone(&plan), requirement);
    }
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        return lower_aggregate(Arc::clone(&plan), aggregate);
    }
    if let Some(window) = plan.downcast_ref::<WindowAggExec>() {
        return lower_window(Arc::clone(&plan), window);
    }
    if let Some(window) = plan.downcast_ref::<BoundedWindowAggExec>() {
        return lower_bounded_window(Arc::clone(&plan), window);
    }
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        return lower_hash_join(Arc::clone(&plan), join);
    }
    if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        return lower_nested_loop_join(Arc::clone(&plan), join);
    }
    if let Some(join) = plan.downcast_ref::<PiecewiseMergeJoinExec>() {
        return lower_piecewise_merge_join(Arc::clone(&plan), join, target_partitions);
    }
    if let Some(join) = plan.downcast_ref::<SortMergeJoinExec>() {
        return lower_sort_merge_join(Arc::clone(&plan), join);
    }
    if let Some(join) = plan.downcast_ref::<SymmetricHashJoinExec>() {
        return lower_symmetric_hash_join(Arc::clone(&plan), join);
    }
    if let Some(filter) = plan.downcast_ref::<StreamFilterExec>() {
        return lower_stream_filter(Arc::clone(&plan), filter);
    }
    if let Some(scan) = plan.downcast_ref::<SystemTableExec>() {
        return lower_system_table(Arc::clone(&plan), scan);
    }
    if let Some(writer) = plan.downcast_ref::<DeletionVectorWriterExec>() {
        return lower_deletion_vector_writer(Arc::clone(&plan), writer);
    }
    if let Some(checkpoint) = plan.downcast_ref::<RemoteCheckpointCommitExec>() {
        return lower_remote_checkpoint(Arc::clone(&plan), checkpoint);
    }
    Ok(Transformed::no(plan))
}

fn lower_projection(
    plan: Arc<dyn ExecutionPlan>,
    projection: &ProjectionExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = projection.input().schema();
    let mut changed = false;
    let expressions = projection
        .expr()
        .iter()
        .map(|projection_expr| {
            let transformed =
                lower_expression(Arc::clone(&projection_expr.expr), input_schema.as_ref())?;
            changed |= transformed.transformed;
            Ok(ProjectionExpr::new(
                transformed.data,
                projection_expr.alias.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    if changed {
        Ok(Transformed::yes(Arc::new(ProjectionExec::try_new(
            expressions,
            Arc::clone(projection.input()),
        )?) as Arc<dyn ExecutionPlan>))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn lower_filter(
    plan: Arc<dyn ExecutionPlan>,
    filter: &FilterExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let transformed = lower_expression(
        Arc::clone(filter.predicate()),
        filter.input().schema().as_ref(),
    )?;
    if transformed.transformed {
        let filter = FilterExecBuilder::from(filter)
            .with_predicate(transformed.data)
            .build()?;
        Ok(Transformed::yes(Arc::new(filter) as Arc<dyn ExecutionPlan>))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn lower_sort(
    plan: Arc<dyn ExecutionPlan>,
    sort: &SortExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (ordering, changed) = lower_ordering(sort.expr(), sort.input().schema().as_ref())?;
    if !changed {
        return Ok(Transformed::no(plan));
    }

    let sort = SortExec::new(ordering, Arc::clone(sort.input()))
        .with_preserve_partitioning(sort.preserve_partitioning())
        .with_fetch(sort.fetch());
    Ok(Transformed::yes(Arc::new(sort) as Arc<dyn ExecutionPlan>))
}

fn lower_repartition(
    plan: Arc<dyn ExecutionPlan>,
    repartition: &RepartitionExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Partitioning::Hash(expressions, partition_count) = repartition.partitioning() else {
        return Ok(Transformed::no(plan));
    };
    let (expressions, changed) =
        lower_expressions(expressions, repartition.input().schema().as_ref())?;
    if !changed {
        return Ok(Transformed::no(plan));
    }

    let preserve_order = repartition.preserve_order();
    let mut repartition = RepartitionExec::try_new(
        Arc::clone(repartition.input()),
        Partitioning::Hash(expressions, *partition_count),
    )?;
    if preserve_order {
        repartition = repartition.with_preserve_order();
    }
    Ok(Transformed::yes(
        Arc::new(repartition) as Arc<dyn ExecutionPlan>
    ))
}

fn lower_explicit_repartition(
    plan: Arc<dyn ExecutionPlan>,
    repartition: &ExplicitRepartitionExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Partitioning::Hash(expressions, partition_count) =
        repartition.properties().output_partitioning()
    else {
        return Ok(Transformed::no(plan));
    };
    let (expressions, changed) =
        lower_expressions(expressions, repartition.input().schema().as_ref())?;
    if !changed {
        return Ok(Transformed::no(plan));
    }

    Ok(Transformed::yes(Arc::new(ExplicitRepartitionExec::new(
        Arc::clone(repartition.input()),
        Partitioning::Hash(expressions, *partition_count),
    )) as Arc<dyn ExecutionPlan>))
}

fn lower_output_requirement(
    plan: Arc<dyn ExecutionPlan>,
    requirement: &OutputRequirementExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input = requirement.input();
    let input_schema = input.schema();
    let order_requirement = requirement
        .required_input_ordering()
        .into_iter()
        .next()
        .flatten();
    let (order_requirement, order_changed) =
        lower_ordering_requirements(order_requirement, input_schema.as_ref())?;
    let distribution = requirement
        .required_input_distribution()
        .into_iter()
        .next()
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "OutputRequirementExec is missing its input distribution"
            )
        })?;
    let (distribution, distribution_changed) =
        lower_distribution(distribution, input_schema.as_ref())?;
    if !(order_changed || distribution_changed) {
        return Ok(Transformed::no(plan));
    }

    Ok(Transformed::yes(Arc::new(OutputRequirementExec::new(
        input,
        order_requirement,
        distribution,
        requirement.fetch(),
    )) as Arc<dyn ExecutionPlan>))
}

fn lower_aggregate(
    plan: Arc<dyn ExecutionPlan>,
    aggregate: &AggregateExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = aggregate.input().schema();
    let (group_expressions, group_changed) =
        lower_named_expressions(aggregate.group_expr().expr(), input_schema.as_ref())?;
    let (null_expressions, null_changed) =
        lower_named_expressions(aggregate.group_expr().null_expr(), input_schema.as_ref())?;
    let group_by = PhysicalGroupBy::new(
        group_expressions,
        null_expressions,
        aggregate.group_expr().groups().to_vec(),
        aggregate.group_expr().has_grouping_set(),
    );

    let mut aggregate_changed = false;
    let aggregate_expressions = aggregate
        .aggr_expr()
        .iter()
        .map(|expression| {
            let (expression, changed) =
                lower_aggregate_expression(expression, input_schema.as_ref())?;
            aggregate_changed |= changed;
            Ok(expression)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut filter_changed = false;
    let filters = aggregate
        .filter_expr()
        .iter()
        .map(|filter| {
            let Some(filter) = filter else {
                return Ok(None);
            };
            let transformed = lower_expression(Arc::clone(filter), input_schema.as_ref())?;
            filter_changed |= transformed.transformed;
            Ok(Some(transformed.data))
        })
        .collect::<Result<Vec<_>>>()?;

    if !(group_changed || null_changed || aggregate_changed || filter_changed) {
        return Ok(Transformed::no(plan));
    }

    let aggregate = AggregateExec::try_new(
        *aggregate.mode(),
        group_by,
        aggregate_expressions,
        filters,
        Arc::clone(aggregate.input()),
        Arc::clone(&aggregate.input_schema),
    )?
    .with_limit_options(aggregate.limit_options());
    Ok(Transformed::yes(
        Arc::new(aggregate) as Arc<dyn ExecutionPlan>
    ))
}

fn lower_window(
    plan: Arc<dyn ExecutionPlan>,
    window: &WindowAggExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (expressions, changed) =
        lower_window_expressions(window.window_expr(), window.input().schema().as_ref())?;
    if !changed {
        return Ok(Transformed::no(plan));
    }
    let can_repartition = !window.partition_keys().is_empty();
    let window = WindowAggExec::try_new(expressions, Arc::clone(window.input()), can_repartition)?;
    Ok(Transformed::yes(Arc::new(window) as Arc<dyn ExecutionPlan>))
}

fn lower_bounded_window(
    plan: Arc<dyn ExecutionPlan>,
    window: &BoundedWindowAggExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (expressions, changed) =
        lower_window_expressions(window.window_expr(), window.input().schema().as_ref())?;
    if !changed {
        return Ok(Transformed::no(plan));
    }
    let can_repartition = !window.partition_keys().is_empty();
    let window = BoundedWindowAggExec::try_new(
        expressions,
        Arc::clone(window.input()),
        window.input_order_mode.clone(),
        can_repartition,
    )?;
    Ok(Transformed::yes(Arc::new(window) as Arc<dyn ExecutionPlan>))
}

fn lower_window_expressions(
    expressions: &[Arc<dyn WindowExpr>],
    input_schema: &Schema,
) -> Result<(Vec<Arc<dyn WindowExpr>>, bool)> {
    let mut changed = false;
    let expressions = expressions
        .iter()
        .map(|expression| {
            let all = expression.all_expressions();
            let (arguments, arguments_changed) = lower_expressions(&all.args, input_schema)?;
            let (partition_by, partition_changed) =
                lower_expressions(&all.partition_by_exprs, input_schema)?;
            let (order_by, order_changed) = lower_expressions(&all.order_by_exprs, input_schema)?;
            let expression_changed = arguments_changed || partition_changed || order_changed;
            changed |= expression_changed;
            if !expression_changed {
                return Ok(Arc::clone(expression));
            }
            expression
                .with_new_expressions(arguments, partition_by, order_by)
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!(
                        "window expression {} does not support lazy scalar lowering",
                        expression.name()
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((expressions, changed))
}

fn lower_hash_join(
    plan: Arc<dyn ExecutionPlan>,
    join: &HashJoinExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (on, on_changed) = lower_join_on(
        join.on(),
        join.left().schema().as_ref(),
        join.right().schema().as_ref(),
    )?;
    let (filter, filter_changed) = lower_join_filter(join.filter())?;
    if !(on_changed || filter_changed) {
        return Ok(Transformed::no(plan));
    }
    let join = HashJoinExecBuilder::from(join)
        .with_on(on)
        .with_filter(filter)
        .recompute_properties()
        .build()?;
    Ok(Transformed::yes(Arc::new(join) as Arc<dyn ExecutionPlan>))
}

fn lower_nested_loop_join(
    plan: Arc<dyn ExecutionPlan>,
    join: &NestedLoopJoinExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (filter, changed) = lower_join_filter(join.filter())?;
    if !changed {
        return Ok(Transformed::no(plan));
    }
    let join = NestedLoopJoinExecBuilder::from(join)
        .with_filter(filter)
        .build()?;
    Ok(Transformed::yes(Arc::new(join) as Arc<dyn ExecutionPlan>))
}

fn lower_piecewise_merge_join(
    plan: Arc<dyn ExecutionPlan>,
    join: &PiecewiseMergeJoinExec,
    target_partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let left = lower_expression(Arc::clone(&join.on.0), join.buffered().schema().as_ref())?;
    let right = lower_expression(Arc::clone(&join.on.1), join.streamed().schema().as_ref())?;
    if !(left.transformed || right.transformed) {
        return Ok(Transformed::no(plan));
    }

    Ok(Transformed::yes(Arc::new(PiecewiseMergeJoinExec::try_new(
        Arc::clone(join.buffered()),
        Arc::clone(join.streamed()),
        (left.data, right.data),
        join.operator,
        join.join_type(),
        target_partitions,
    )?) as Arc<dyn ExecutionPlan>))
}

fn lower_sort_merge_join(
    plan: Arc<dyn ExecutionPlan>,
    join: &SortMergeJoinExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (on, on_changed) = lower_join_on(
        join.on(),
        join.left().schema().as_ref(),
        join.right().schema().as_ref(),
    )?;
    let (filter, filter_changed) = lower_join_filter(join.filter().as_ref())?;
    if !(on_changed || filter_changed) {
        return Ok(Transformed::no(plan));
    }
    let join = SortMergeJoinExec::try_new(
        Arc::clone(join.left()),
        Arc::clone(join.right()),
        on,
        filter,
        join.join_type(),
        join.sort_options().to_vec(),
        join.null_equality(),
    )?;
    Ok(Transformed::yes(Arc::new(join) as Arc<dyn ExecutionPlan>))
}

fn lower_symmetric_hash_join(
    plan: Arc<dyn ExecutionPlan>,
    join: &SymmetricHashJoinExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (on, on_changed) = lower_join_on(
        join.on(),
        join.left().schema().as_ref(),
        join.right().schema().as_ref(),
    )?;
    let (filter, filter_changed) = lower_join_filter(join.filter())?;
    let (left_sort, left_sort_changed) =
        lower_optional_ordering(join.left_sort_exprs(), join.left().schema().as_ref())?;
    let (right_sort, right_sort_changed) =
        lower_optional_ordering(join.right_sort_exprs(), join.right().schema().as_ref())?;
    if !(on_changed || filter_changed || left_sort_changed || right_sort_changed) {
        return Ok(Transformed::no(plan));
    }
    let join = SymmetricHashJoinExec::try_new(
        Arc::clone(join.left()),
        Arc::clone(join.right()),
        on,
        filter,
        join.join_type(),
        join.null_equality(),
        left_sort,
        right_sort,
        join.partition_mode(),
    )?;
    Ok(Transformed::yes(Arc::new(join) as Arc<dyn ExecutionPlan>))
}

fn lower_stream_filter(
    plan: Arc<dyn ExecutionPlan>,
    filter: &StreamFilterExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let transformed = lower_expression(
        Arc::clone(filter.predicate()),
        filter.input().schema().as_ref(),
    )?;
    if !transformed.transformed {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(Arc::new(StreamFilterExec::try_new(
        Arc::clone(filter.input()),
        transformed.data,
    )?) as Arc<dyn ExecutionPlan>))
}

fn lower_system_table(
    plan: Arc<dyn ExecutionPlan>,
    scan: &SystemTableExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let table = scan.table();
    let (filters, changed) = lower_expressions(scan.filters(), table.schema().as_ref())?;
    if !changed {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(Arc::new(SystemTableExec::try_new(
        table,
        scan.projection().map(|projection| projection.to_vec()),
        filters,
        scan.fetch(),
    )?) as Arc<dyn ExecutionPlan>))
}

fn lower_deletion_vector_writer(
    plan: Arc<dyn ExecutionPlan>,
    writer: &DeletionVectorWriterExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let transformed = lower_expression(
        Arc::clone(writer.condition()),
        writer.table_schema().as_ref(),
    )?;
    if !transformed.transformed {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(Arc::new(DeletionVectorWriterExec::new(
        Arc::clone(writer.input()),
        writer.table_url().clone(),
        transformed.data,
        Arc::clone(writer.table_schema()),
        writer.version(),
        writer
            .partition_value_columns()
            .map(|columns| columns.to_vec()),
        writer.operation().cloned(),
    )?) as Arc<dyn ExecutionPlan>))
}

fn lower_remote_checkpoint(
    plan: Arc<dyn ExecutionPlan>,
    checkpoint: &RemoteCheckpointCommitExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let storage_schema = checkpoint.storage_schema();
    let (partitioning, partitioning_changed) = lower_partitioning(
        checkpoint.checkpoint_partitioning().clone(),
        storage_schema.as_ref(),
    )?;
    let (ordering, ordering_changed) =
        lower_optional_ordering(checkpoint.checkpoint_ordering(), storage_schema.as_ref())?;
    if !(partitioning_changed || ordering_changed) {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(Arc::new(RemoteCheckpointCommitExec::new(
        Arc::clone(checkpoint.input()),
        checkpoint.relation_id().to_string(),
        checkpoint.object_store_url().clone(),
        checkpoint.prefix().clone(),
        Arc::clone(checkpoint.logical_schema()),
        Arc::clone(storage_schema),
        partitioning,
        ordering,
    )) as Arc<dyn ExecutionPlan>))
}

fn lower_join_on(
    on: &[PhysicalJoinKey],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(Vec<PhysicalJoinKey>, bool)> {
    let mut changed = false;
    let on = on
        .iter()
        .map(|(left, right)| {
            let left = lower_expression(Arc::clone(left), left_schema)?;
            let right = lower_expression(Arc::clone(right), right_schema)?;
            changed |= left.transformed || right.transformed;
            Ok((left.data, right.data))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((on, changed))
}

fn lower_join_filter(filter: Option<&JoinFilter>) -> Result<(Option<JoinFilter>, bool)> {
    let Some(filter) = filter else {
        return Ok((None, false));
    };
    let transformed = lower_expression(Arc::clone(filter.expression()), filter.schema().as_ref())?;
    Ok((
        Some(JoinFilter::new(
            transformed.data,
            filter.column_indices().to_vec(),
            Arc::clone(filter.schema()),
        )),
        transformed.transformed,
    ))
}

fn lower_aggregate_expression(
    expression: &Arc<AggregateFunctionExpr>,
    input_schema: &Schema,
) -> Result<(Arc<AggregateFunctionExpr>, bool)> {
    let all = expression.all_expressions();
    let (arguments, arguments_changed) = lower_expressions(&all.args, input_schema)?;
    let (order_by, order_changed) = lower_expressions(&all.order_by_exprs, input_schema)?;
    if !(arguments_changed || order_changed) {
        return Ok((Arc::clone(expression), false));
    }
    let expression = expression
        .with_new_expressions(arguments, order_by)
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "aggregate expression {} does not support lazy scalar lowering",
                expression.name()
            )
        })?;
    Ok((Arc::new(expression), true))
}

fn lower_named_expressions(
    expressions: &[NamedPhysicalExpression],
    input_schema: &Schema,
) -> Result<(Vec<NamedPhysicalExpression>, bool)> {
    let mut changed = false;
    let expressions = expressions
        .iter()
        .map(|(expression, name)| {
            let transformed = lower_expression(Arc::clone(expression), input_schema)?;
            changed |= transformed.transformed;
            Ok((transformed.data, name.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((expressions, changed))
}

fn lower_expressions(
    expressions: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<(Vec<Arc<dyn PhysicalExpr>>, bool)> {
    let mut changed = false;
    let expressions = expressions
        .iter()
        .map(|expression| {
            let transformed = lower_expression(Arc::clone(expression), input_schema)?;
            changed |= transformed.transformed;
            Ok(transformed.data)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((expressions, changed))
}

fn lower_optional_ordering(
    ordering: Option<&LexOrdering>,
    input_schema: &Schema,
) -> Result<(Option<LexOrdering>, bool)> {
    let Some(ordering) = ordering else {
        return Ok((None, false));
    };
    let (ordering, changed) = lower_ordering(ordering, input_schema)?;
    Ok((Some(ordering), changed))
}

fn lower_ordering_requirements(
    requirements: Option<OrderingRequirements>,
    input_schema: &Schema,
) -> Result<(Option<OrderingRequirements>, bool)> {
    let Some(requirements) = requirements else {
        return Ok((None, false));
    };
    let (alternatives, soft) = match requirements {
        OrderingRequirements::Hard(alternatives) => (alternatives, false),
        OrderingRequirements::Soft(alternatives) => (alternatives, true),
    };
    let mut changed = false;
    let alternatives = alternatives
        .into_iter()
        .map(|alternative| {
            let requirements = alternative
                .into_iter()
                .map(|requirement| {
                    let transformed = lower_expression(requirement.expr, input_schema)?;
                    changed |= transformed.transformed;
                    Ok(PhysicalSortRequirement::new(
                        transformed.data,
                        requirement.options,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            LexRequirement::new(requirements).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "lazy scalar lowering produced an empty ordering requirement"
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let requirements =
        OrderingRequirements::new_alternatives(alternatives, soft).ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "lazy scalar lowering produced no ordering alternatives"
            )
        })?;
    Ok((Some(requirements), changed))
}

fn lower_distribution(
    distribution: Distribution,
    input_schema: &Schema,
) -> Result<(Distribution, bool)> {
    let Distribution::HashPartitioned(expressions) = distribution else {
        return Ok((distribution, false));
    };
    let (expressions, changed) = lower_expressions(&expressions, input_schema)?;
    Ok((Distribution::HashPartitioned(expressions), changed))
}

fn lower_partitioning(
    partitioning: Partitioning,
    input_schema: &Schema,
) -> Result<(Partitioning, bool)> {
    let Partitioning::Hash(expressions, partition_count) = partitioning else {
        return Ok((partitioning, false));
    };
    let (expressions, changed) = lower_expressions(&expressions, input_schema)?;
    Ok((Partitioning::Hash(expressions, partition_count), changed))
}

fn lower_ordering(ordering: &LexOrdering, input_schema: &Schema) -> Result<(LexOrdering, bool)> {
    let mut changed = false;
    let expressions = ordering
        .iter()
        .map(|sort_expression| {
            let transformed = lower_expression(Arc::clone(&sort_expression.expr), input_schema)?;
            changed |= transformed.transformed;
            Ok(PhysicalSortExpr::new(
                transformed.data,
                sort_expression.options,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let ordering = LexOrdering::new(expressions).ok_or_else(|| {
        datafusion_common::internal_datafusion_err!(
            "lazy scalar lowering produced an empty ordering"
        )
    })?;
    Ok((ordering, changed))
}

fn lower_expression(
    expression: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    expression.transform_up(|expression| {
        if let Some(scalar) = expression.downcast_ref::<ScalarSubqueryExpr>() {
            if scalar.nullable() {
                return Ok(Transformed::no(expression));
            }
            return Ok(Transformed::yes(Arc::new(ScalarSubqueryExpr::new(
                scalar.data_type().clone(),
                true,
                scalar.index(),
                scalar.results().clone(),
            )) as Arc<dyn PhysicalExpr>));
        }

        let Some(scalar) = expression.downcast_ref::<ScalarFunctionExpr>() else {
            return Ok(Transformed::no(expression));
        };
        let Some(lazy) = LazyScalarExpr::try_from_scalar_function(scalar, input_schema)? else {
            return Ok(Transformed::no(expression));
        };
        Ok(Transformed::yes(Arc::new(lazy) as Arc<dyn PhysicalExpr>))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::{
        Distribution, LexOrdering, LexRequirement, OrderingRequirements, PhysicalExpr,
        PhysicalSortExpr, PhysicalSortRequirement, ScalarFunctionExpr,
    };
    use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::PiecewiseMergeJoinExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue, internal_datafusion_err};
    use datafusion_expr::{JoinType, Operator, ScalarUDF};
    use datafusion_physical_expr::Partitioning;
    use object_store::path::Path;
    use sail_catalog_system::physical_plan::SystemTableExec;
    use sail_common_datafusion::logical_expr::lazy_scalar::{
        LazyScalarEvaluationPolicy, LazyScalarUDF,
    };
    use sail_common_datafusion::physical_expr::lazy_scalar::LazyScalarExpr;
    use sail_common_datafusion::system::catalog::SystemTable;
    use sail_delta_lake::physical_plan::DeletionVectorWriterExec;
    use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
    use sail_physical_plan::repartition::ExplicitRepartitionExec;
    use sail_physical_plan::streaming::filter::StreamFilterExec;

    use super::lower_lazy_scalars;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]))
    }

    fn marked_abs(schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        let function = Arc::new(ScalarUDF::new_from_impl(
            datafusion::functions::math::abs::AbsFunc::new(),
        ));
        let marker = Arc::new(ScalarUDF::from(LazyScalarUDF::new(
            function,
            LazyScalarEvaluationPolicy::TryActiveRows,
        )));
        Ok(Arc::new(ScalarFunctionExpr::try_new(
            marker,
            vec![Arc::new(Literal::new(ScalarValue::Int64(Some(1))))],
            schema,
            Arc::new(ConfigOptions::default()),
        )?))
    }

    #[test]
    fn lowers_lazy_scalar_in_sort_expression() -> Result<()> {
        let schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(marked_abs(
            schema.as_ref(),
        )?)])
        .ok_or_else(|| internal_datafusion_err!("expected sort ordering"))?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));

        let lowered = lower_lazy_scalars(plan, 1)?;
        let sort = lowered
            .downcast_ref::<SortExec>()
            .ok_or_else(|| internal_datafusion_err!("expected SortExec"))?;
        let lazy = sort.expr()[0]
            .expr
            .downcast_ref::<LazyScalarExpr>()
            .ok_or_else(|| internal_datafusion_err!("sort expression was not lowered"))?;
        assert_eq!(lazy.policy(), LazyScalarEvaluationPolicy::TryActiveRows);
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_hash_partitioning() -> Result<()> {
        let schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![marked_abs(schema.as_ref())?], 2),
        )?);

        let lowered = lower_lazy_scalars(plan, 1)?;
        let repartition = lowered
            .downcast_ref::<RepartitionExec>()
            .ok_or_else(|| internal_datafusion_err!("expected RepartitionExec"))?;
        let Partitioning::Hash(expressions, _) = repartition.partitioning() else {
            return Err(internal_datafusion_err!("expected hash partitioning"));
        };
        assert!(expressions[0].downcast_ref::<LazyScalarExpr>().is_some());
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_explicit_hash_partitioning() -> Result<()> {
        let schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ExplicitRepartitionExec::new(
            input,
            Partitioning::Hash(vec![marked_abs(schema.as_ref())?], 2),
        ));

        let lowered = lower_lazy_scalars(plan, 1)?;
        let repartition = lowered
            .downcast_ref::<ExplicitRepartitionExec>()
            .ok_or_else(|| internal_datafusion_err!("expected ExplicitRepartitionExec"))?;
        let Partitioning::Hash(expressions, _) = repartition.properties().output_partitioning()
        else {
            return Err(internal_datafusion_err!("expected hash partitioning"));
        };
        assert!(expressions[0].downcast_ref::<LazyScalarExpr>().is_some());
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_output_requirements() -> Result<()> {
        let schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let ordering = LexRequirement::new([PhysicalSortRequirement::new(
            marked_abs(schema.as_ref())?,
            None,
        )])
        .ok_or_else(|| internal_datafusion_err!("expected ordering requirement"))?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
            input,
            Some(OrderingRequirements::Hard(vec![ordering])),
            Distribution::HashPartitioned(vec![marked_abs(schema.as_ref())?]),
            None,
        ));

        let lowered = lower_lazy_scalars(plan, 1)?;
        let requirement = lowered
            .downcast_ref::<OutputRequirementExec>()
            .ok_or_else(|| internal_datafusion_err!("expected OutputRequirementExec"))?;
        let ordering = requirement
            .required_input_ordering()
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| internal_datafusion_err!("expected ordering requirement"))?;
        assert!(
            ordering[0][0]
                .expr
                .downcast_ref::<LazyScalarExpr>()
                .is_some()
        );
        let Distribution::HashPartitioned(expressions) =
            requirement.required_input_distribution().swap_remove(0)
        else {
            return Err(internal_datafusion_err!("expected hash distribution"));
        };
        assert!(expressions[0].downcast_ref::<LazyScalarExpr>().is_some());
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_piecewise_merge_join() -> Result<()> {
        let schema = schema();
        let left: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let right: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(PiecewiseMergeJoinExec::try_new(
            left,
            right,
            (marked_abs(schema.as_ref())?, marked_abs(schema.as_ref())?),
            Operator::Gt,
            JoinType::Inner,
            1,
        )?);

        let lowered = lower_lazy_scalars(plan, 1)?;
        let join = lowered
            .downcast_ref::<PiecewiseMergeJoinExec>()
            .ok_or_else(|| internal_datafusion_err!("expected PiecewiseMergeJoinExec"))?;
        assert!(join.on.0.downcast_ref::<LazyScalarExpr>().is_some());
        assert!(join.on.1.downcast_ref::<LazyScalarExpr>().is_some());
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_stream_filter() -> Result<()> {
        let schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StreamFilterExec::try_new(
            input,
            marked_abs(schema.as_ref())?,
        )?);

        let lowered = lower_lazy_scalars(plan, 1)?;
        let filter = lowered
            .downcast_ref::<StreamFilterExec>()
            .ok_or_else(|| internal_datafusion_err!("expected StreamFilterExec"))?;
        assert!(
            filter
                .predicate()
                .downcast_ref::<LazyScalarExpr>()
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_system_table_filter() -> Result<()> {
        let table = SystemTable::Options;
        let schema = table.schema();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SystemTableExec::try_new(
            table,
            None,
            vec![marked_abs(schema.as_ref())?],
            None,
        )?);

        let lowered = lower_lazy_scalars(plan, 1)?;
        let scan = lowered
            .downcast_ref::<SystemTableExec>()
            .ok_or_else(|| internal_datafusion_err!("expected SystemTableExec"))?;
        assert!(scan.filters()[0].downcast_ref::<LazyScalarExpr>().is_some());
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_checkpoint_properties() -> Result<()> {
        let schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(marked_abs(schema.as_ref())?)])
                .ok_or_else(|| internal_datafusion_err!("expected checkpoint ordering"))?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(RemoteCheckpointCommitExec::new(
            input,
            "checkpoint".to_string(),
            ObjectStoreUrl::parse("memory://")?,
            Path::from("checkpoint"),
            Arc::clone(&schema),
            Arc::clone(&schema),
            Partitioning::Hash(vec![marked_abs(schema.as_ref())?], 2),
            Some(ordering),
        ));

        let lowered = lower_lazy_scalars(plan, 1)?;
        let checkpoint = lowered
            .downcast_ref::<RemoteCheckpointCommitExec>()
            .ok_or_else(|| internal_datafusion_err!("expected RemoteCheckpointCommitExec"))?;
        let Partitioning::Hash(expressions, _) = checkpoint.checkpoint_partitioning() else {
            return Err(internal_datafusion_err!("expected hash partitioning"));
        };
        assert!(expressions[0].downcast_ref::<LazyScalarExpr>().is_some());
        let ordering = checkpoint
            .checkpoint_ordering()
            .ok_or_else(|| internal_datafusion_err!("expected checkpoint ordering"))?;
        assert!(ordering[0].expr.downcast_ref::<LazyScalarExpr>().is_some());
        Ok(())
    }

    #[test]
    fn lowers_lazy_scalar_in_deletion_vector_condition() -> Result<()> {
        let schema = schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let table_url = "memory://"
            .parse()
            .map_err(|error| internal_datafusion_err!("invalid test URL: {error}"))?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(DeletionVectorWriterExec::new(
            input,
            table_url,
            marked_abs(schema.as_ref())?,
            Arc::clone(&schema),
            1,
            None,
            None,
        )?);

        let lowered = lower_lazy_scalars(plan, 1)?;
        let writer = lowered
            .downcast_ref::<DeletionVectorWriterExec>()
            .ok_or_else(|| internal_datafusion_err!("expected DeletionVectorWriterExec"))?;
        assert!(
            writer
                .condition()
                .downcast_ref::<LazyScalarExpr>()
                .is_some()
        );
        Ok(())
    }
}
