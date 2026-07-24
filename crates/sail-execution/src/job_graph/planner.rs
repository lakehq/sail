use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{JoinType, Result, plan_datafusion_err};
use datafusion::logical_expr::execution_props::ScalarSubqueryResults;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, PlanProperties, with_new_children_if_necessary,
};
use sail_catalog_system::physical_plan::SystemTableExec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_data_source::listing::delete::FileDeleteExec;
use sail_delta_lake::physical_plan::DeltaCommitExec;
use sail_iceberg::physical_plan::IcebergCommitExec;
use sail_physical_plan::barrier::BarrierExec;
use sail_physical_plan::catalog_command::CatalogCommandExec;
use sail_physical_plan::coalesce::CoalesceExec;
use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;

use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
};
use crate::plan::{ShuffleConsumption, StageInputExec};

impl JobGraph {
    pub fn try_new(plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Self> {
        let plan = ensure_single_input_partition_for_global_limit(plan)?;
        let plan = ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows(plan)?;
        let mut graph = Self {
            stages: vec![],
            schema: plan.schema(),
        };
        let last = build_job_graph(plan, PartitionUsage::Once, &mut graph)?.plan;
        let (last, inputs) = rewrite_inputs(last)?;
        graph.stages.push(Stage {
            inputs,
            plan: last,
            group: String::new(),
            mode: OutputMode::Pipelined,
            distribution: OutputDistribution::RoundRobin { channels: 1 },
            placement: TaskPlacement::Worker,
        });
        Ok(graph)
    }
}

fn ensure_single_input_partition_for_global_limit(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    // Rewrite *all* `GlobalLimitExec` nodes in the tree to ensure their input is single-partition.
    let result = plan.transform(|node| {
        if let Some(gl) = node.downcast_ref::<GlobalLimitExec>() {
            let skip = gl.skip();
            let fetch = gl.fetch();
            let input = gl.input();
            if fetch.is_none() && skip == 0 {
                // If there is neither LIMIT nor OFFSET, return the node as is.
                Ok(Transformed::no(node))
            } else if input.output_partitioning().partition_count() > 1 {
                // Keep `LocalLimitExec` (if any) to preserve the per-partition top-k optimization,
                // but make sure the input to `GlobalLimitExec` is single-partition.
                let input = Arc::new(CoalescePartitionsExec::new(input.clone()));
                Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                    input, skip, fetch,
                ))))
            } else {
                Ok(Transformed::no(node))
            }
        } else {
            Ok(Transformed::no(node))
        }
    });
    Ok(result.data()?)
}

fn ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    fn repartition(
        plan: Arc<dyn ExecutionPlan>,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // We have to remove unnecessary repartitioning explicitly here
        // since no physical optimizer will run afterward.
        let plan = if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
            Arc::clone(coalesce.input())
        } else if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            Arc::clone(repartition.input())
        } else {
            plan
        };
        Ok(Arc::new(RepartitionExec::try_new(
            plan,
            Partitioning::Hash(exprs, count),
        )?))
    }

    let result = plan.transform_up(|plan| {
        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(plan));
        };

        if join.mode != PartitionMode::CollectLeft {
            return Ok(Transformed::no(plan));
        }

        if !matches!(
            join.join_type,
            JoinType::Left
                | JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::Full
        ) {
            // `LEFT` or `FULL` joins need to emit unmatched rows from the build side.
            // This is not yet possible in distributed execution since the bitmap for
            // row matching is not shared across partitions.
            // So we need to turn the join into a partitioned hash join for now.
            return Ok(Transformed::no(plan));
        }

        // Convert the join to a partitioned hash join with explicit repartitioning on both sides,
        // so each output partition can be executed independently in the distributed engine.
        let partition_count = join.right.output_partitioning().partition_count();

        let (left_exprs, right_exprs): (Vec<_>, Vec<_>) = join
            .on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();

        Ok(Transformed::yes(Arc::new(HashJoinExec::try_new(
            repartition(Arc::clone(&join.left), left_exprs, partition_count)?,
            repartition(Arc::clone(&join.right), right_exprs, partition_count)?,
            join.on.clone(),
            join.filter.clone(),
            &join.join_type,
            join.projection.as_deref().map(|p| p.to_vec()),
            PartitionMode::Partitioned,
            join.null_equality,
            false,
        )?)))
    })?;

    Ok(result.data)
}

/// A flag to indicate how the partitions from physical plan execution are used.
#[derive(Clone, Copy)]
enum PartitionUsage {
    /// Each partition of the plan is only used once.
    Once,
    /// The same partition may be used multiple times when producing partitions
    /// for the parent physical plan.
    ///
    /// This is typically needed for an optimized join operation where
    /// the build-side data (small) of only one partition is gathered via `plan.execute(0)`
    /// for each partition of the probe-side data.
    /// For single-host execution, DataFusion uses `OnceAsync` to ensure the
    /// build-side is only evaluated once. In the distributed setting, we use this
    /// usage information to create materialized shuffle data that can be
    /// consumed multiple times.
    Shared,
}

#[derive(Clone, Copy)]
enum DriverStageHandling {
    CreateStage,
    PreserveRoot,
}

#[derive(Clone, Copy)]
struct ScalarSubqueryContext<'a> {
    links: &'a [ScalarSubqueryLink],
    results: &'a ScalarSubqueryResults,
}

struct PlannedSubtree {
    plan: Arc<dyn ExecutionPlan>,
    // TODO: Track pending SubqueryIndex values if wrapper placement must
    // distinguish multiple or nested scalar subqueries.
    has_pending_scalar_subquery_expr: bool,
}

impl PlannedSubtree {
    fn new(plan: Arc<dyn ExecutionPlan>, has_pending_scalar_subquery_expr: bool) -> Self {
        Self {
            plan,
            has_pending_scalar_subquery_expr,
        }
    }

    fn without_pending_scalar_subquery_expr(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self::new(plan, false)
    }
}

struct RebuiltSubtree {
    plan: Arc<dyn ExecutionPlan>,
    // TODO: Extend this with other stage-boundary metadata if materialization
    // needs to preserve more than pending scalar subquery state.
    child_has_pending_scalar_subquery_expr: Vec<bool>,
    node_has_scalar_subquery_expr: bool,
    subtree_has_pending_scalar_subquery_expr: bool,
}

impl RebuiltSubtree {
    fn only_child(&self) -> ExecutionResult<PlannedSubtree> {
        let [has_pending_scalar_subquery_expr] =
            self.child_has_pending_scalar_subquery_expr.as_slice()
        else {
            return Err(ExecutionError::InternalError(format!(
                "expected exactly one planned child, got {}",
                self.child_has_pending_scalar_subquery_expr.len()
            )));
        };
        let child = self.plan.children().one()?;
        Ok(PlannedSubtree::new(
            child.clone(),
            *has_pending_scalar_subquery_expr,
        ))
    }

    fn into_planned_subtree(self) -> PlannedSubtree {
        PlannedSubtree::new(self.plan, self.subtree_has_pending_scalar_subquery_expr)
    }
}

/// Recursively splits an execution plan into stages at shuffle boundaries and adds them to the job graph.
fn build_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
) -> ExecutionResult<PlannedSubtree> {
    plan_job_graph_stages(plan, usage, graph, DriverStageHandling::CreateStage, None)
}

fn plan_job_graph_stages(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    driver_stage_handling: DriverStageHandling,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<PlannedSubtree> {
    if let Some(scalar) = plan.downcast_ref::<ScalarSubqueryExec>() {
        return build_scalar_subquery_job_graph(scalar, usage, graph, driver_stage_handling);
    }

    if let Some(barrier) = plan.downcast_ref::<BarrierExec>() {
        return build_barrier_job_graph(barrier, usage, graph, scalar_context);
    }

    // Recursively build the job graph for the children first
    // and propagate partition usage information.
    let children = if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        let (left, right) = join.children().two()?;
        match join.mode {
            PartitionMode::Partitioned => {
                vec![
                    plan_job_graph_stages(
                        left.clone(),
                        usage,
                        graph,
                        DriverStageHandling::CreateStage,
                        scalar_context,
                    )?,
                    plan_job_graph_stages(
                        right.clone(),
                        usage,
                        graph,
                        DriverStageHandling::CreateStage,
                        scalar_context,
                    )?,
                ]
            }
            PartitionMode::CollectLeft => {
                vec![
                    plan_job_graph_stages(
                        left.clone(),
                        PartitionUsage::Shared,
                        graph,
                        DriverStageHandling::CreateStage,
                        scalar_context,
                    )?,
                    plan_job_graph_stages(
                        right.clone(),
                        usage,
                        graph,
                        DriverStageHandling::CreateStage,
                        scalar_context,
                    )?,
                ]
            }
            PartitionMode::Auto => {
                return Err(ExecutionError::DataFusionError(plan_datafusion_err!(
                    "unresolved auto partition mode in hash join"
                )));
            }
        }
    } else if plan.is::<NestedLoopJoinExec>()
        || plan.is::<CrossJoinExec>()
        || plan.is::<PiecewiseMergeJoinExec>()
    {
        let (left, right) = plan.children().two()?;
        vec![
            plan_job_graph_stages(
                left.clone(),
                PartitionUsage::Shared,
                graph,
                DriverStageHandling::CreateStage,
                scalar_context,
            )?,
            plan_job_graph_stages(
                right.clone(),
                usage,
                graph,
                DriverStageHandling::CreateStage,
                scalar_context,
            )?,
        ]
    } else if plan.is::<RepartitionExec>()
        || plan.is::<ExplicitRepartitionExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<SortPreservingMergeExec>()
        || plan.is::<CoalesceExec>()
    {
        let child = plan.children().one()?;
        // At the stage boundary, we only expect to use the child partition once
        // since the shuffle writer can materialize the data for multiple consumption.
        vec![plan_job_graph_stages(
            child.clone(),
            PartitionUsage::Once,
            graph,
            DriverStageHandling::CreateStage,
            scalar_context,
        )?]
    } else if matches!(driver_stage_handling, DriverStageHandling::PreserveRoot)
        && plan.is::<CooperativeExec>()
        && is_driver_stage_plan(&plan)
    {
        let child = plan.children().one()?;
        vec![plan_job_graph_stages(
            child.clone(),
            usage,
            graph,
            DriverStageHandling::PreserveRoot,
            scalar_context,
        )?]
    } else {
        plan.children()
            .into_iter()
            .map(|x| {
                plan_job_graph_stages(
                    x.clone(),
                    usage,
                    graph,
                    DriverStageHandling::CreateStage,
                    scalar_context,
                )
            })
            .collect::<ExecutionResult<Vec<_>>>()?
    };
    let subtree = rebuild_subtree(plan, children)?;

    let consumption = match usage {
        PartitionUsage::Once => ShuffleConsumption::Single,
        PartitionUsage::Shared => ShuffleConsumption::Multiple,
    };
    let plan = if let Some(repartition) = subtree.plan.downcast_ref::<RepartitionExec>() {
        if repartition.preserve_order() {
            // We haven't found a case when order-preserving repartition can be constructed,
            // so it's fine to return an error for now.
            // TODO: support order-preserving repartition
            return Err(ExecutionError::InternalError(
                "repartition is order-preserving and would result in incorrect results in distributed execution".to_string()
            ));
        }
        let properties = repartition.properties().clone();
        let child = subtree.only_child()?;
        let plan = match &properties.partitioning {
            Partitioning::UnknownPartitioning(n) => {
                let n = *n;
                let properties = Arc::new(
                    properties
                        .as_ref()
                        .clone()
                        .with_partitioning(Partitioning::RoundRobinBatch(n)),
                );
                create_shuffle(child, graph, properties, consumption, scalar_context)?
            }
            Partitioning::RoundRobinBatch(_) | Partitioning::Hash(_, _) => {
                create_shuffle(child, graph, properties, consumption, scalar_context)?
            }
        };
        PlannedSubtree::without_pending_scalar_subquery_expr(plan)
    } else if let Some(repartition) = subtree.plan.downcast_ref::<ExplicitRepartitionExec>() {
        let properties = repartition.properties().clone();
        let child = subtree.only_child()?;
        let plan = match &properties.partitioning {
            Partitioning::RoundRobinBatch(channels) => create_row_shuffle(
                child,
                *channels,
                graph,
                properties,
                consumption,
                scalar_context,
            )?,
            other => {
                return Err(ExecutionError::DataFusionError(plan_datafusion_err!(
                    "unexpected explicit repartition partitioning in distributed planning: {other:?}"
                )));
            }
        };
        PlannedSubtree::without_pending_scalar_subquery_expr(plan)
    } else if let Some(coalesce) = subtree.plan.downcast_ref::<CoalescePartitionsExec>() {
        let properties = coalesce.properties().clone();
        let fetch = coalesce.fetch();
        let child = subtree.only_child()?;
        let shuffled = create_shuffle(child, graph, properties, consumption, scalar_context)?;
        if let Some(f) = fetch {
            PlannedSubtree::without_pending_scalar_subquery_expr(Arc::new(GlobalLimitExec::new(
                shuffled,
                0,
                Some(f),
            ))
                as Arc<dyn ExecutionPlan>)
        } else {
            PlannedSubtree::without_pending_scalar_subquery_expr(shuffled)
        }
    } else if subtree.plan.is::<SortPreservingMergeExec>() {
        let child = subtree.only_child()?;
        let plan = subtree
            .plan
            .clone()
            .with_new_children(vec![create_merge_input(child, graph, scalar_context)?])?;
        PlannedSubtree::new(plan, subtree.node_has_scalar_subquery_expr)
    } else if let Some(coalesce) = subtree.plan.downcast_ref::<CoalesceExec>() {
        let child = subtree.only_child()?;
        let plan =
            create_rescale_input(child, coalesce.output_partitions(), graph, scalar_context)?;
        PlannedSubtree::without_pending_scalar_subquery_expr(plan)
    } else if subtree.plan.is::<SystemTableExec>()
        || subtree.plan.is::<CatalogCommandExec>()
        || subtree.plan.is::<FileDeleteExec>()
        || subtree.plan.is::<DeltaCommitExec>()
        || subtree.plan.is::<IcebergCommitExec>()
        || subtree.plan.is::<RemoteCheckpointCommitExec>()
    {
        if matches!(driver_stage_handling, DriverStageHandling::PreserveRoot) {
            subtree.into_planned_subtree()
        } else {
            let plan = create_driver_stage(subtree.into_planned_subtree(), graph, scalar_context)?;
            PlannedSubtree::without_pending_scalar_subquery_expr(plan)
        }
    } else {
        subtree.into_planned_subtree()
    };
    Ok(plan)
}

fn rebuild_subtree(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<PlannedSubtree>,
) -> ExecutionResult<RebuiltSubtree> {
    let child_has_pending_scalar_subquery_expr = children
        .iter()
        .map(|child| child.has_pending_scalar_subquery_expr)
        .collect::<Vec<_>>();
    let children = children
        .into_iter()
        .map(|child| child.plan)
        .collect::<Vec<_>>();
    let plan = with_new_children_if_necessary(plan, children)?;
    let node_has_scalar_subquery_expr = plan_node_has_scalar_subquery_expr(&plan);
    let subtree_has_pending_scalar_subquery_expr =
        node_has_scalar_subquery_expr || child_has_pending_scalar_subquery_expr.iter().any(|x| *x);
    Ok(RebuiltSubtree {
        plan,
        child_has_pending_scalar_subquery_expr,
        node_has_scalar_subquery_expr,
        subtree_has_pending_scalar_subquery_expr,
    })
}

fn build_scalar_subquery_job_graph(
    scalar: &ScalarSubqueryExec,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    driver_stage_handling: DriverStageHandling,
) -> ExecutionResult<PlannedSubtree> {
    let subqueries = scalar
        .subqueries()
        .iter()
        .map(|link| {
            // The link plan is materialized once as its own stage. Reuse happens
            // at the outer broadcast input consumed by ScalarSubqueryExec.
            let plan = plan_job_graph_stages(
                link.plan.clone(),
                PartitionUsage::Once,
                graph,
                DriverStageHandling::CreateStage,
                None,
            )?;
            let plan = create_scalar_subquery_input(&plan.plan, graph)?;
            Ok(ScalarSubqueryLink {
                plan,
                index: link.index,
            })
        })
        .collect::<ExecutionResult<Vec<_>>>()?;
    let input = plan_job_graph_stages(
        scalar.input().clone(),
        usage,
        graph,
        driver_stage_handling,
        Some(ScalarSubqueryContext {
            links: subqueries.as_slice(),
            results: scalar.results(),
        }),
    )?;
    let plan = wrap_pending_scalar_subqueries(
        input,
        Some(ScalarSubqueryContext {
            links: subqueries.as_slice(),
            results: scalar.results(),
        }),
    );
    Ok(PlannedSubtree::without_pending_scalar_subquery_expr(plan))
}

fn build_barrier_job_graph(
    barrier: &BarrierExec,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<PlannedSubtree> {
    let preconditions = barrier
        .preconditions()
        .iter()
        .map(|precondition| {
            plan_job_graph_stages(
                precondition.clone(),
                usage,
                graph,
                DriverStageHandling::CreateStage,
                scalar_context,
            )
        })
        .collect::<ExecutionResult<Vec<_>>>()?;
    let preconditions_have_pending_scalar_subquery_expr = preconditions
        .iter()
        .any(|precondition| precondition.has_pending_scalar_subquery_expr);
    let preconditions = preconditions
        .into_iter()
        .map(|precondition| precondition.plan)
        .collect::<Vec<_>>();
    let plan_is_driver_stage = is_driver_stage_plan(barrier.plan());
    let plan = if plan_is_driver_stage {
        plan_job_graph_stages(
            barrier.plan().clone(),
            usage,
            graph,
            DriverStageHandling::PreserveRoot,
            scalar_context,
        )?
    } else {
        plan_job_graph_stages(
            barrier.plan().clone(),
            usage,
            graph,
            DriverStageHandling::CreateStage,
            scalar_context,
        )?
    };
    let barrier_has_pending_scalar_subquery_expr =
        preconditions_have_pending_scalar_subquery_expr || plan.has_pending_scalar_subquery_expr;
    let barrier = Arc::new(BarrierExec::new(preconditions, plan.plan)) as Arc<dyn ExecutionPlan>;
    let barrier = PlannedSubtree::new(barrier, barrier_has_pending_scalar_subquery_expr);
    if plan_is_driver_stage {
        Ok(PlannedSubtree::without_pending_scalar_subquery_expr(
            create_driver_stage(barrier, graph, scalar_context)?,
        ))
    } else {
        Ok(barrier)
    }
}

fn is_driver_stage_plan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(cooperative) = plan.downcast_ref::<CooperativeExec>() {
        return is_driver_stage_plan(cooperative.input());
    }

    plan.is::<SystemTableExec>()
        || plan.is::<CatalogCommandExec>()
        || plan.is::<FileDeleteExec>()
        || plan.is::<DeltaCommitExec>()
        || plan.is::<IcebergCommitExec>()
        || plan.is::<RemoteCheckpointCommitExec>()
}

fn wrap_pending_scalar_subqueries(
    plan: PlannedSubtree,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> Arc<dyn ExecutionPlan> {
    let Some(scalar_context) = scalar_context else {
        return plan.plan;
    };
    if scalar_context.links.is_empty() || !plan.has_pending_scalar_subquery_expr {
        return plan.plan;
    }
    Arc::new(ScalarSubqueryExec::new(
        plan.plan,
        scalar_context.links.to_vec(),
        scalar_context.results.clone(),
    ))
}

fn plan_node_has_scalar_subquery_expr(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        return physical_expr_has_scalar_subquery(filter.predicate());
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return projection
            .expr()
            .iter()
            .any(|expr| physical_expr_has_scalar_subquery(&expr.expr));
    }
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        return aggregate_has_scalar_subquery_expr(aggregate);
    }
    if let Some(sort) = plan.downcast_ref::<SortExec>() {
        return sort
            .expr()
            .iter()
            .any(|sort| physical_expr_has_scalar_subquery(&sort.expr));
    }
    if let Some(sort) = plan.downcast_ref::<SortPreservingMergeExec>() {
        return sort
            .expr()
            .iter()
            .any(|sort| physical_expr_has_scalar_subquery(&sort.expr));
    }
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        return hash_join_has_scalar_subquery_expr(join);
    }
    if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        return join
            .filter()
            .is_some_and(|filter| physical_expr_has_scalar_subquery(filter.expression()));
    }
    if let Some(join) = plan.downcast_ref::<PiecewiseMergeJoinExec>() {
        return physical_expr_has_scalar_subquery(&join.on.0)
            || physical_expr_has_scalar_subquery(&join.on.1);
    }
    if let Some(window) = plan.downcast_ref::<WindowAggExec>() {
        return window
            .window_expr()
            .iter()
            .any(window_expr_has_scalar_subquery);
    }
    if let Some(window) = plan.downcast_ref::<BoundedWindowAggExec>() {
        return window
            .window_expr()
            .iter()
            .any(window_expr_has_scalar_subquery);
    }
    false
}

fn aggregate_has_scalar_subquery_expr(aggregate: &AggregateExec) -> bool {
    aggregate
        .group_expr()
        .expr()
        .iter()
        .any(|(expr, _)| physical_expr_has_scalar_subquery(expr))
        || aggregate
            .group_expr()
            .null_expr()
            .iter()
            .any(|(expr, _)| physical_expr_has_scalar_subquery(expr))
        || aggregate.aggr_expr().iter().any(|expr| {
            expr.expressions()
                .iter()
                .any(physical_expr_has_scalar_subquery)
                || expr
                    .order_bys()
                    .iter()
                    .any(|sort| physical_expr_has_scalar_subquery(&sort.expr))
        })
        || aggregate
            .filter_expr()
            .iter()
            .flatten()
            .any(physical_expr_has_scalar_subquery)
}

fn hash_join_has_scalar_subquery_expr(join: &HashJoinExec) -> bool {
    join.on().iter().any(|(left, right)| {
        physical_expr_has_scalar_subquery(left) || physical_expr_has_scalar_subquery(right)
    }) || join
        .filter()
        .is_some_and(|filter| physical_expr_has_scalar_subquery(filter.expression()))
}

fn window_expr_has_scalar_subquery(
    expr: &Arc<dyn datafusion::physical_expr::window::WindowExpr>,
) -> bool {
    let expressions = expr.all_expressions();
    expressions
        .args
        .iter()
        .chain(expressions.partition_by_exprs.iter())
        .chain(expressions.order_by_exprs.iter())
        .any(physical_expr_has_scalar_subquery)
}

fn physical_expr_has_scalar_subquery(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<ScalarSubqueryExpr>().is_some()
        || expr
            .children()
            .iter()
            .any(|child| physical_expr_has_scalar_subquery(child))
}

fn create_merge_input(
    plan: PlannedSubtree,
    graph: &mut JobGraph,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let properties = plan.plan.properties().clone();
    let plan = wrap_pending_scalar_subqueries(plan, scalar_context);
    let stage = push_stage(
        plan,
        graph,
        OutputDistribution::RoundRobin { channels: 1 },
        TaskPlacement::Worker,
    )?;
    Ok(stage_input_exec(stage, InputMode::Merge, properties))
}

fn create_scalar_subquery_input(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let stage = push_stage(
        Arc::clone(plan),
        graph,
        OutputDistribution::RoundRobin { channels: 1 },
        TaskPlacement::Worker,
    )?;
    // ScalarSubqueryExec reads the link as a scalar value on every output
    // partition, so the materialized stage is exposed as one broadcast input.
    let properties = stage_properties_with_unknown_partitioning(graph, stage, 1);
    Ok(stage_input_exec(stage, InputMode::Broadcast, properties))
}

fn create_rescale_input(
    plan: PlannedSubtree,
    output_partitions: usize,
    graph: &mut JobGraph,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let plan = wrap_pending_scalar_subqueries(plan, scalar_context);
    let stage = push_stage(
        plan,
        graph,
        OutputDistribution::RoundRobin { channels: 1 },
        TaskPlacement::Worker,
    )?;
    let properties = stage_properties_with_unknown_partitioning(graph, stage, output_partitions);
    Ok(stage_input_exec(stage, InputMode::Rescale, properties))
}

fn create_shuffle(
    plan: PlannedSubtree,
    graph: &mut JobGraph,
    // These are the properties after repartition/coalesce,
    // which are different from the properties of the input plan.
    properties: Arc<PlanProperties>,
    consumption: ShuffleConsumption,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let distribution = match properties.partitioning.clone() {
        Partitioning::RoundRobinBatch(channels) | Partitioning::UnknownPartitioning(channels) => {
            OutputDistribution::RoundRobin { channels }
        }
        Partitioning::Hash(keys, channels) => OutputDistribution::Hash { keys, channels },
    };
    let plan = wrap_pending_scalar_subqueries(plan, scalar_context);
    let stage = push_stage(plan, graph, distribution, TaskPlacement::Worker)?;
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    Ok(stage_input_exec(stage, mode, properties))
}

/// Creates a shuffle stage with row-level round-robin distribution.
/// Used for explicit user-requested repartition calls where individual rows
/// must be distributed evenly across output partitions.
fn create_row_shuffle(
    plan: PlannedSubtree,
    channels: usize,
    graph: &mut JobGraph,
    properties: Arc<PlanProperties>,
    consumption: ShuffleConsumption,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let distribution = OutputDistribution::RoundRobinRow { channels };
    let plan = wrap_pending_scalar_subqueries(plan, scalar_context);
    let stage = push_stage(plan, graph, distribution, TaskPlacement::Worker)?;
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    Ok(stage_input_exec(stage, mode, properties))
}

fn rewrite_inputs(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<(Arc<dyn ExecutionPlan>, Vec<StageInput>)> {
    let mut inputs = vec![];
    let result = plan.transform(|node| {
        if let Some(placeholder) = node.downcast_ref::<StageInputExec<StageInput>>() {
            let index = inputs.len();
            inputs.push(placeholder.input().clone());
            let placeholder = StageInputExec::new(index, placeholder.properties().clone());
            Ok(Transformed::yes(Arc::new(placeholder)))
        } else {
            Ok(Transformed::no(node))
        }
    });
    Ok((result.data()?, inputs))
}

fn push_stage(
    plan: Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    distribution: OutputDistribution,
    placement: TaskPlacement,
) -> ExecutionResult<usize> {
    let (plan, inputs) = rewrite_inputs(plan)?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution,
        placement,
    };
    let index = graph.stages.len();
    graph.stages.push(stage);
    Ok(index)
}

fn stage_input_exec(
    stage: usize,
    mode: InputMode,
    properties: Arc<PlanProperties>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(StageInputExec::new(StageInput { stage, mode }, properties))
}

fn stage_properties_with_unknown_partitioning(
    graph: &JobGraph,
    stage: usize,
    partitions: usize,
) -> Arc<PlanProperties> {
    let plan = &graph.stages[stage].plan;
    Arc::new(PlanProperties::new(
        datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
        Partitioning::UnknownPartitioning(partitions),
        plan.pipeline_behavior(),
        plan.boundedness(),
    ))
}

fn create_driver_stage(
    plan: PlannedSubtree,
    graph: &mut JobGraph,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let plan = wrap_pending_scalar_subqueries(plan, scalar_context);
    let stage = push_stage(
        plan,
        graph,
        OutputDistribution::RoundRobin { channels: 1 },
        TaskPlacement::Driver,
    )?;
    let properties = graph.stages[stage].plan.properties().clone();
    Ok(stage_input_exec(stage, InputMode::Forward, properties))
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::execution_props::{ScalarSubqueryResults, SubqueryIndex};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{binary, col};
    use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion::physical_expr::{Partitioning, PhysicalExpr};
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
    use sail_catalog::command::CatalogCommand;
    use sail_physical_plan::barrier::BarrierExec;
    use sail_physical_plan::catalog_command::CatalogCommandExec;
    use sail_physical_plan::coalesce::CoalesceExec;
    use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
    use sail_physical_plan::repartition::ExplicitRepartitionExec;

    use super::JobGraph;
    use crate::job_graph::{InputMode, OutputDistribution, StageInput, TaskPlacement};
    use crate::plan::StageInputExec;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn empty_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    #[test]
    fn test_job_graph_distinguishes_batch_and_explicit_round_robin_shuffle() {
        let generic_graph = JobGraph::try_new(Arc::new(
            RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(4)).unwrap(),
        ))
        .unwrap();
        let explicit_graph = JobGraph::try_new(Arc::new(ExplicitRepartitionExec::new(
            empty_plan(),
            Partitioning::RoundRobinBatch(4),
        )))
        .unwrap();

        assert_eq!(generic_graph.stages().len(), 2);
        assert!(matches!(
            &generic_graph.stages()[0].distribution,
            OutputDistribution::RoundRobin { channels: 4 }
        ));
        assert!(matches!(
            generic_graph.stages()[1].inputs.as_slice(),
            [StageInput {
                stage: 0,
                mode: InputMode::Shuffle,
            }]
        ));

        assert_eq!(explicit_graph.stages().len(), 2);
        assert!(matches!(
            &explicit_graph.stages()[0].distribution,
            OutputDistribution::RoundRobinRow { channels: 4 }
        ));
        assert!(matches!(
            explicit_graph.stages()[1].inputs.as_slice(),
            [StageInput {
                stage: 0,
                mode: InputMode::Shuffle,
            }]
        ));
    }

    #[test]
    fn test_job_graph_uses_rescale_input_for_coalesce_exec() {
        let input =
            UnionExec::try_new(vec![empty_plan(), empty_plan(), empty_plan(), empty_plan()])
                .unwrap();
        let graph = JobGraph::try_new(Arc::new(CoalesceExec::new(input, 2))).unwrap();

        assert_eq!(graph.stages().len(), 2);
        assert_eq!(
            graph.stages()[1]
                .plan
                .output_partitioning()
                .partition_count(),
            2
        );
        assert!(matches!(
            graph.stages()[1].inputs.as_slice(),
            [StageInput {
                stage: 0,
                mode: InputMode::Rescale,
            }]
        ));
    }

    #[test]
    fn test_job_graph_places_remote_checkpoint_commit_on_driver() {
        let schema = schema();
        let commit = Arc::new(RemoteCheckpointCommitExec::new(
            empty_plan(),
            "relation".to_string(),
            ObjectStoreUrl::parse("memory://").unwrap(),
            object_store::path::Path::from("checkpoint/session/relation"),
            Arc::clone(&schema),
            schema,
            Partitioning::UnknownPartitioning(1),
            None,
        ));

        let graph = JobGraph::try_new(commit).unwrap();

        assert_eq!(graph.stages().len(), 2);
        assert_eq!(graph.stages()[0].placement, TaskPlacement::Driver);
        assert!(graph.stages()[0].plan.is::<RemoteCheckpointCommitExec>());
        assert!(matches!(
            graph.stages()[1].inputs.as_slice(),
            [StageInput {
                stage: 0,
                mode: InputMode::Forward,
            }]
        ));
    }

    #[test]
    fn test_job_graph_keeps_driver_actual_plan_inside_barrier() {
        let precondition = Arc::new(
            RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(1)).unwrap(),
        );
        let command = Arc::new(CatalogCommandExec::new(
            CatalogCommand::CurrentCatalog,
            schema(),
        ));
        let command = Arc::new(CooperativeExec::new(command));
        let graph =
            JobGraph::try_new(Arc::new(BarrierExec::new(vec![precondition], command))).unwrap();

        assert_eq!(graph.stages().len(), 3);
        assert_eq!(graph.stages()[1].placement, TaskPlacement::Driver);
        #[expect(clippy::expect_used)]
        let barrier = graph.stages()[1]
            .plan
            .downcast_ref::<BarrierExec>()
            .expect("driver stage should contain BarrierExec");
        #[expect(clippy::expect_used)]
        let cooperative = barrier
            .plan()
            .downcast_ref::<CooperativeExec>()
            .expect("barrier actual plan should preserve CooperativeExec");
        assert!(cooperative.input().is::<CatalogCommandExec>());
        assert!(matches!(
            graph.stages()[1].inputs.as_slice(),
            [StageInput {
                stage: 0,
                mode: InputMode::Shuffle,
            }]
        ));
        assert!(matches!(
            graph.stages()[2].inputs.as_slice(),
            [StageInput {
                stage: 1,
                mode: InputMode::Forward,
            }]
        ));
    }

    #[test]
    fn test_job_graph_wraps_scalar_subquery_stage_after_shuffle_split() {
        let results = ScalarSubqueryResults::new(1);
        let predicate = Arc::new(ScalarSubqueryExpr::new(
            DataType::Boolean,
            false,
            SubqueryIndex::new(0),
            results.clone(),
        ));
        let filtered = Arc::new(FilterExec::try_new(predicate, empty_plan()).unwrap());
        let repartitioned =
            Arc::new(RepartitionExec::try_new(filtered, Partitioning::RoundRobinBatch(4)).unwrap());
        let plan = Arc::new(ScalarSubqueryExec::new(
            repartitioned,
            vec![ScalarSubqueryLink {
                plan: empty_plan(),
                index: SubqueryIndex::new(0),
            }],
            results,
        ));

        let graph = JobGraph::try_new(plan).unwrap();

        assert!(graph.stages().len() >= 3);
        let stage = graph
            .stages()
            .iter()
            .find_map(|stage| stage.plan.downcast_ref::<ScalarSubqueryExec>())
            .expect("stage containing ScalarSubqueryExpr should keep a ScalarSubqueryExec wrapper");
        assert_eq!(stage.subqueries().len(), 1);
        let filter = stage
            .input()
            .downcast_ref::<FilterExec>()
            .expect("ScalarSubqueryExec input should be the filtered stage");
        let scalar_expr = filter
            .predicate()
            .downcast_ref::<ScalarSubqueryExpr>()
            .expect("filter predicate should contain a scalar subquery expression");
        assert!(ScalarSubqueryResults::ptr_eq(
            stage.results(),
            scalar_expr.results()
        ));
    }

    #[test]
    fn test_job_graph_wraps_aggregate_scalar_subquery_stage_after_shuffle_split() {
        let input_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let results = ScalarSubqueryResults::new(1);
        let scalar_subquery = Arc::new(ScalarSubqueryExpr::new(
            DataType::Int64,
            false,
            SubqueryIndex::new(0),
            results.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let aggregate_arg = binary(
            col("v", input_schema.as_ref()).unwrap(),
            Operator::Plus,
            scalar_subquery,
            input_schema.as_ref(),
        )
        .unwrap();
        let aggregate_expr = Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![aggregate_arg])
                .schema(input_schema.clone())
                .alias("total")
                .human_display("#4")
                .build()
                .unwrap(),
        );
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new_single(vec![]),
                vec![aggregate_expr],
                vec![None],
                Arc::new(EmptyExec::new(input_schema.clone())),
                input_schema,
            )
            .unwrap(),
        );
        assert!(
            !displayable(aggregate.as_ref())
                .indent(false)
                .to_string()
                .contains("scalar_subquery("),
            "aggregate display should hide the scalar subquery expression"
        );
        let repartitioned = Arc::new(
            RepartitionExec::try_new(aggregate, Partitioning::RoundRobinBatch(4)).unwrap(),
        );
        let plan = Arc::new(ScalarSubqueryExec::new(
            repartitioned,
            vec![ScalarSubqueryLink {
                plan: empty_plan(),
                index: SubqueryIndex::new(0),
            }],
            results,
        ));

        let graph = JobGraph::try_new(plan).unwrap();

        let stage = graph
            .stages()
            .iter()
            .find_map(|stage| stage.plan.downcast_ref::<ScalarSubqueryExec>())
            .filter(|scalar| scalar.input().is::<AggregateExec>())
            .expect("aggregate stage containing ScalarSubqueryExpr should keep a ScalarSubqueryExec wrapper");
        assert_eq!(stage.subqueries().len(), 1);
    }

    #[test]
    fn test_job_graph_broadcasts_scalar_subquery_stage_inputs() {
        let results = ScalarSubqueryResults::new(1);
        let predicate = Arc::new(ScalarSubqueryExpr::new(
            DataType::Boolean,
            false,
            SubqueryIndex::new(0),
            results.clone(),
        ));
        let filtered = Arc::new(FilterExec::try_new(predicate, empty_plan()).unwrap());
        let repartitioned =
            Arc::new(RepartitionExec::try_new(filtered, Partitioning::RoundRobinBatch(4)).unwrap());
        let subquery = Arc::new(
            RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(4)).unwrap(),
        );
        let plan = Arc::new(ScalarSubqueryExec::new(
            repartitioned,
            vec![ScalarSubqueryLink {
                plan: subquery,
                index: SubqueryIndex::new(0),
            }],
            results,
        ));

        let graph = JobGraph::try_new(plan).unwrap();
        let stage = graph
            .stages()
            .iter()
            .find(|stage| stage.plan.is::<ScalarSubqueryExec>())
            .expect("stage containing ScalarSubqueryExpr should keep a ScalarSubqueryExec wrapper");
        let scalar = stage.plan.downcast_ref::<ScalarSubqueryExec>().unwrap();
        let subquery_input = scalar.subqueries()[0]
            .plan
            .downcast_ref::<StageInputExec<usize>>()
            .expect("scalar subquery link should read from a stage input");

        assert_eq!(
            subquery_input
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        assert!(
            stage
                .inputs
                .iter()
                .any(|input| matches!(input.mode, InputMode::Broadcast))
        );
    }
}
