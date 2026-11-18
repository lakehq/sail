use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinType, Result, internal_err, plan_datafusion_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr};
use datafusion::physical_expr_common::physical_expr::is_volatile;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, PlanProperties, replace_children_if_necessary,
};
use sail_catalog_system::physical_plan::SystemTableExec;
use sail_common_datafusion::lakeprocedure::LakeProcedureRootPlacement;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_data_source::listing::delete::FileDeleteExec;
use sail_delta_lake::physical_plan::DeltaCommitExec;
use sail_iceberg::physical_plan::{IcebergCommitExec, IcebergProcedureExec};
use sail_physical_plan::barrier::BarrierExec;
use sail_physical_plan::catalog_command::CatalogCommandExec;
use sail_physical_plan::coalesce::CoalesceExec;
use sail_physical_plan::lake_procedure::LakeProcedureExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use sail_physical_plan::shared_cte::{SharedCte, SharedCteExec, SharedCteScanExec};

use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{
    InputMode, JobGraph, JobGraphOptions, OutputDistribution, OutputMode, Stage, StageInput,
    TaskPlacement,
};
use crate::plan::{ShuffleConsumption, StageInputExec};
use crate::shuffle::ShuffleBackendKind;

impl JobGraph {
    pub fn try_new(
        plan: Arc<dyn ExecutionPlan>,
        options: JobGraphOptions,
    ) -> ExecutionResult<Self> {
        let plan = ensure_single_input_partition_for_global_limit(plan)?;
        let plan = ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows(plan)?;
        let plan = ensure_single_probe_partition_for_nested_loop_join(plan)?;
        let plan = ensure_worker_input_for_lake_procedure(plan)?;
        let mut graph = Self {
            stages: vec![],
            schema: plan.schema(),
            options,
        };
        let last = build_job_graph(plan, PartitionUsage::Once, &mut graph)?.plan;
        let (last, inputs) = rewrite_inputs(last)?;
        graph.stages.push(Stage {
            inputs,
            plan: last,
            group: String::new(),
            mode: OutputMode::Pipelined,
            distribution: OutputDistribution::RoundRobinBatch { channels: 1 },
            placement: TaskPlacement::Worker,
        });
        Ok(graph)
    }
}

fn ensure_worker_input_for_lake_procedure(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    fn has_exchange(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if let Some(cooperative) = plan.downcast_ref::<CooperativeExec>() {
            return has_exchange(cooperative.input());
        }
        plan.is::<CoalescePartitionsExec>() || plan.is::<SortPreservingMergeExec>()
    }

    Ok(plan
        .transform_up(|plan| {
            let input = if let Some(procedure) = plan.downcast_ref::<IcebergProcedureExec>() {
                procedure.input()
            } else if let Some(procedure) =
                plan.downcast_ref::<sail_delta_lake::physical_plan::DeltaMaintenanceExec>()
            {
                procedure.input()
            } else {
                None
            };
            let Some(input) = input else {
                return Ok(Transformed::no(plan));
            };
            if has_exchange(input) {
                return Ok(Transformed::no(plan));
            }
            // A single writer partition still belongs on a worker. Insert the exchange after
            // physical optimization so redundant-coalesce removal cannot erase this boundary.
            let input = Arc::new(CoalescePartitionsExec::new(Arc::clone(input)));
            Ok(Transformed::yes(replace_children_if_necessary(
                plan,
                vec![input],
            )?))
        })?
        .data)
}

fn ensure_single_probe_partition_for_nested_loop_join(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let result = plan.transform_up(|plan| {
        let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() else {
            return Ok(Transformed::no(plan));
        };
        if !matches!(
            join.join_type(),
            JoinType::Left
                | JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::Full
        ) || join.right().output_partitioning().partition_count() == 1
        {
            return Ok(Transformed::no(plan));
        }

        // Build-side output needs a match bitmap shared by all probe partitions.
        // Workers cannot share that state, so one task must consume the whole probe side.
        let children = vec![
            Arc::clone(join.left()),
            Arc::new(CoalescePartitionsExec::new(Arc::clone(join.right()))),
        ];
        Ok(Transformed::yes(replace_children_if_necessary(
            plan, children,
        )?))
    })?;
    Ok(result.data)
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
        let plan = if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>()
            && coalesce.fetch().is_none()
        {
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

        if join.null_aware {
            // NULL and empty-input decisions require the entire probe side, and workers
            // cannot share DataFusion's probe flags or build-side match bitmap.
            let children = join
                .children()
                .into_iter()
                .map(|input| {
                    if input.output_partitioning().partition_count() > 1 {
                        Arc::new(CoalescePartitionsExec::new(Arc::clone(input)))
                            as Arc<dyn ExecutionPlan>
                    } else {
                        Arc::clone(input)
                    }
                })
                .collect();
            return Ok(Transformed::yes(replace_children_if_necessary(
                plan, children,
            )?));
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

        let left = repartition(Arc::clone(&join.left), left_exprs, partition_count)?;
        let right = repartition(Arc::clone(&join.right), right_exprs, partition_count)?;
        let join = join
            .builder()
            .with_new_children(vec![left, right])?
            .with_partition_mode(PartitionMode::Partitioned)
            .build_exec()?;
        Ok(Transformed::yes(join))
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

// Plain exchanges finish at a reusable input that every reading task receives in full,
// with no row selection, computation, or added sort above the exchange. A merge without
// a fetch merges the sorted input streams directly, while an order-preserving
// repartition needs a sort above the shuffle.
fn is_plain_exchange(plan: &dyn ExecutionPlan) -> bool {
    plan.downcast_ref::<CoalescePartitionsExec>()
        .is_some_and(|coalesce| coalesce.fetch().is_none())
        || plan
            .downcast_ref::<RepartitionExec>()
            .is_some_and(|repartition| repartition_ordering_to_restore(repartition).is_none())
        || plan.is::<ExplicitRepartitionExec>()
        || plan.is::<CoalesceExec>()
        || plan
            .downcast_ref::<SortPreservingMergeExec>()
            .is_some_and(|merge| merge.fetch().is_none())
}

fn repartition_ordering_to_restore(repartition: &RepartitionExec) -> Option<&LexOrdering> {
    if repartition.preserve_order() {
        repartition.properties().output_ordering()
    } else {
        None
    }
}

/// Returns whether every join task that computes this build itself gets the same rows.
/// This holds for projections and filters without volatile expressions over in-memory, range,
/// empty, or placeholder sources, or over plain exchanges, whose complete output every reading
/// task receives.
/// Other operators, such as limits, aggregates, or windows over rows read from a shuffle,
/// may give different rows in different tasks.
fn is_reproducible_build(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    if is_plain_exchange(plan.as_ref()) {
        return Ok(true);
    }
    let source = plan.downcast_ref::<DataSourceExec>().is_some_and(|source| {
        source
            .data_source()
            .downcast_ref::<MemorySourceConfig>()
            .is_some()
    }) || plan.is::<RangeExec>()
        || plan.is::<EmptyExec>()
        || plan.is::<PlaceholderRowExec>();
    let row_wise =
        plan.is::<ProjectionExec>() || plan.is::<FilterExec>() || plan.is::<CooperativeExec>();
    if !(source || row_wise) || plan.fetch().is_some() {
        return Ok(false);
    }
    let mut volatile = false;
    plan.apply_expressions(&mut |expr| {
        volatile = is_volatile(expr);
        Ok(if volatile {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    if volatile {
        return Ok(false);
    }
    for child in plan.children() {
        if !is_reproducible_build(child)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn contains_file_scan(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    plan.exists(|node| {
        Ok(node.downcast_ref::<DataSourceExec>().is_some_and(|source| {
            source
                .data_source()
                .downcast_ref::<FileScanConfig>()
                .is_some()
        }))
    })
}

fn plan_job_graph_stages(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    driver_stage_handling: DriverStageHandling,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<PlannedSubtree> {
    if let Some(shared) = plan.downcast_ref::<SharedCteExec>() {
        return build_shared_cte_job_graph(
            shared,
            usage,
            graph,
            driver_stage_handling,
            scalar_context,
        );
    }

    // A reference reads the stage of its shared CTE, which every reading task can receive
    // in full, so it is not materialized again as a shared build relation below.
    if let Some(reference) = plan.downcast_ref::<StageInputExec<SharedCteStage>>() {
        return create_shared_cte_input(reference, usage, graph, scalar_context);
    }

    if matches!(usage, PartitionUsage::Shared)
        && plan.output_partitioning().partition_count() == 1
        && !is_plain_exchange(plan.as_ref())
        && (!is_reproducible_build(&plan)? || contains_file_scan(&plan)?)
    {
        // The complete build relation is shared, including limits, TopK, pushed-down
        // fetches, and volatile expressions. Sharing only its inputs can select different
        // rows in different join tasks. This also materializes single-partition file scans.
        // With Flight shuffle, the broadcast puts the producer and all join partitions in one
        // task region that is scheduled as a whole, so a join whose partitions could run as
        // independent single-slot regions now needs one worker slot per partition at once.
        // This is acceptable since Flight shuffle usually assumes all nodes are interconnected
        // to form a single task region.
        let properties = plan.properties().clone();
        let build = plan_job_graph_stages(
            plan,
            PartitionUsage::Once,
            graph,
            driver_stage_handling,
            scalar_context,
        )?;
        return Ok(PlannedSubtree::without_pending_scalar_subquery_expr(
            create_shared_input(build, graph, properties, scalar_context)?,
        ));
    }

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
                        if right.output_partitioning().partition_count() > 1 {
                            PartitionUsage::Shared
                        } else {
                            usage
                        },
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
                if right.output_partitioning().partition_count() > 1 {
                    PartitionUsage::Shared
                } else {
                    usage
                },
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
        let ordering = repartition_ordering_to_restore(repartition).cloned();
        if ordering.is_some() && repartition.properties().boundedness.is_unbounded() {
            return Err(ExecutionError::InternalError(
                "order-preserving repartition of unbounded input is not supported in distributed execution".to_string()
            ));
        }
        // Shuffle readers interleave producer streams. Restore the promised ordering
        // in each receiving partition before downstream operators consume the rows.
        let mut properties = repartition.properties().as_ref().clone();
        if ordering.is_some() {
            let mut equivalence = properties.eq_properties.clone();
            equivalence.clear_orderings();
            properties.set_eq_properties(equivalence);
        }
        let properties = Arc::new(properties);
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
            Partitioning::RoundRobinBatch(_)
            | Partitioning::Hash(_, _)
            | Partitioning::Range(_) => {
                create_shuffle(child, graph, properties, consumption, scalar_context)?
            }
        };
        let plan = match ordering {
            Some(ordering) => {
                Arc::new(SortExec::new(ordering, plan).with_preserve_partitioning(true))
                    as Arc<dyn ExecutionPlan>
            }
            None => plan,
        };
        let has_pending_scalar_subquery_expr = plan_node_has_scalar_subquery_expr(&plan)?;
        PlannedSubtree::new(plan, has_pending_scalar_subquery_expr)
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
        let plan = replace_children_if_necessary(
            subtree.plan.clone(),
            vec![create_merge_input(child, graph, scalar_context)?],
        )?;
        PlannedSubtree::new(plan, subtree.node_has_scalar_subquery_expr)
    } else if let Some(coalesce) = subtree.plan.downcast_ref::<CoalesceExec>() {
        let child = subtree.only_child()?;
        let plan = if matches!(usage, PartitionUsage::Shared) && coalesce.output_partitions() == 1 {
            // Rescale reads this same set of streams but reserves only one replica.
            // Broadcast makes the single coalesced partition reusable by every reader.
            create_shuffle(
                child,
                graph,
                coalesce.properties().clone(),
                ShuffleConsumption::Multiple,
                scalar_context,
            )?
        } else {
            create_rescale_input(child, coalesce.output_partitions(), graph, scalar_context)?
        };
        PlannedSubtree::without_pending_scalar_subquery_expr(plan)
    } else if subtree.plan.is::<SystemTableExec>()
        || subtree.plan.is::<CatalogCommandExec>()
        || is_coordinator_lake_procedure(&subtree.plan)
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
    let plan = replace_children_if_necessary(plan, children)?;
    let node_has_scalar_subquery_expr = plan_node_has_scalar_subquery_expr(&plan)?;
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

/// The stage of a shared CTE, as the placeholder of a reference to the CTE.
#[derive(Debug, Clone)]
struct SharedCteStage {
    stage: usize,
    /// The columns of the shared CTE that the reference reads, or `None` for all columns.
    projection: Option<Vec<usize>>,
}

impl std::fmt::Display for SharedCteStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SharedCteStage(stage={})", self.stage)
    }
}

/// Plans each shared CTE as its own stage, which is read by the stages of its references.
fn build_shared_cte_job_graph(
    shared: &SharedCteExec,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    driver_stage_handling: DriverStageHandling,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<PlannedSubtree> {
    // The stage of each shared CTE planned so far.
    let mut stages: HashMap<*const SharedCte, usize> = HashMap::new();
    let locate = |plan: &Arc<dyn ExecutionPlan>, stages: &HashMap<*const SharedCte, usize>| {
        Arc::clone(plan)
            .transform_up(|plan| {
                let Some(scan) = plan.downcast_ref::<SharedCteScanExec>() else {
                    return Ok(Transformed::no(plan));
                };
                let Some(&stage) = stages.get(&Arc::as_ptr(scan.cte())) else {
                    return internal_err!("shared CTE {} is not planned", scan.cte().id());
                };
                let input = SharedCteStage {
                    stage,
                    projection: scan.projection().map(|x| x.to_vec()),
                };
                Ok(Transformed::yes(Arc::new(StageInputExec::new(
                    input,
                    Arc::clone(plan.properties()),
                ))
                    as Arc<dyn ExecutionPlan>))
            })
            .data()
    };
    // The output of the stage is read in full by each reference, like the output of a
    // scalar subquery stage is read by each task that uses it.
    let mode = scalar_subquery_output_mode(graph);
    for (cte, plan) in shared.ctes() {
        let plan = plan_job_graph_stages(
            locate(plan, &stages)?,
            PartitionUsage::Once,
            graph,
            DriverStageHandling::CreateStage,
            None,
        )?;
        let distribution = match graph.options.shuffle_backend {
            // The shuffle service serves each partition of the CTE as a channel, which
            // keeps the partitioning of the CTE.
            ShuffleBackendKind::Celeborn { .. } => {
                let partitioning = plan.plan.output_partitioning();
                let channels = partitioning.partition_count();
                match partitioning {
                    Partitioning::Hash(keys, _) => OutputDistribution::Hash {
                        keys: keys.clone(),
                        channels,
                    },
                    _ => OutputDistribution::RoundRobinBatch { channels },
                }
            }
            ShuffleBackendKind::Flight { .. } | ShuffleBackendKind::Storage { .. } => {
                OutputDistribution::RoundRobinBatch { channels: 1 }
            }
        };
        let stage = push_stage(plan.plan, graph, distribution, TaskPlacement::Worker, mode)?;
        stages.insert(Arc::as_ptr(cte), stage);
    }
    plan_job_graph_stages(
        locate(shared.input(), &stages)?,
        usage,
        graph,
        driver_stage_handling,
        scalar_context,
    )
}

/// Reads the stage of a shared CTE for one of its references.
///
/// Each partition of the reference reads the same partition of the stage, so the
/// reference keeps the partitioning that it reported when the query was planned. If the
/// plan of the stage does not have this partitioning anymore, its output is
/// repartitioned for the reference.
fn create_shared_cte_input(
    reference: &StageInputExec<SharedCteStage>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<PlannedSubtree> {
    let SharedCteStage { stage, projection } = reference.input();
    let partitioning = reference.properties().output_partitioning();
    let schema = graph.stages[*stage].plan.schema();
    let stage_partitioning = graph.stages[*stage].plan.output_partitioning().clone();
    // The partitioning of the reference in terms of the columns of the stage.
    let expected = match partitioning {
        Partitioning::Hash(keys, partitions) => keys
            .iter()
            .map(|key| {
                let position = key.downcast_ref::<Column>()?.index();
                let index = match projection {
                    Some(projection) => *projection.get(position)?,
                    None => position,
                };
                let name = schema.fields().get(index)?.name();
                Some(Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>)
            })
            .collect::<Option<Vec<_>>>()
            .map(|keys| Partitioning::Hash(keys, *partitions)),
        partitioning => Some(partitioning.clone()),
    };
    let preserved = match &expected {
        Some(Partitioning::Hash(_, _)) => expected.as_ref() == Some(&stage_partitioning),
        _ => partitioning.partition_count() == stage_partitioning.partition_count(),
    };
    let partitions = stage_partitioning.partition_count();
    let mode = match (usage, &graph.options.shuffle_backend) {
        // The only partition of the reference is read by every task of its stage.
        (PartitionUsage::Shared, _) if preserved && partitions == 1 => InputMode::Broadcast,
        // Each partition of the shared CTE is a channel of the shuffle service.
        (_, ShuffleBackendKind::Celeborn { .. }) => InputMode::Shuffle,
        _ => InputMode::Rescale { partitions },
    };
    let mut properties = stage_properties_with_unknown_partitioning(graph, *stage, partitions);
    if preserved && let Some(expected) = expected {
        properties = Arc::new(properties.as_ref().clone().with_partitioning(expected));
    }
    let input = stage_input_exec(*stage, mode, properties);
    let input = match projection {
        Some(projection) => {
            let schema = input.schema();
            let expressions = projection
                .iter()
                .map(|&index| {
                    let name = schema.field(index).name();
                    ProjectionExpr::new(Arc::new(Column::new(name, index)), name.clone())
                })
                .collect::<Vec<_>>();
            Arc::new(ProjectionExec::try_new(expressions, input)?) as Arc<dyn ExecutionPlan>
        }
        None => input,
    };
    if preserved {
        return Ok(PlannedSubtree::without_pending_scalar_subquery_expr(input));
    }
    let partitioning = match partitioning {
        Partitioning::UnknownPartitioning(partitions) => Partitioning::RoundRobinBatch(*partitions),
        partitioning => partitioning.clone(),
    };
    plan_job_graph_stages(
        Arc::new(RepartitionExec::try_new(input, partitioning)?),
        usage,
        graph,
        DriverStageHandling::CreateStage,
        scalar_context,
    )
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
        || is_coordinator_lake_procedure(plan)
        || plan.is::<FileDeleteExec>()
        || plan.is::<DeltaCommitExec>()
        || plan.is::<IcebergCommitExec>()
        || plan.is::<RemoteCheckpointCommitExec>()
}

fn is_coordinator_lake_procedure(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.downcast_ref::<LakeProcedureExec>()
        .is_some_and(|procedure| {
            matches!(
                procedure.root_placement(),
                LakeProcedureRootPlacement::Coordinator
            )
        })
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

fn plan_node_has_scalar_subquery_expr(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    // Include expressions pushed into data sources, not just expressions on
    // filters and projections. Every stage using a scalar result needs its own
    // ScalarSubqueryExec, even if predicate pushdown removed the FilterExec.
    let mut found = false;
    plan.apply_expressions(&mut |expr| {
        if physical_expr_has_scalar_subquery(expr) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}

fn physical_expr_has_scalar_subquery(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<ScalarSubqueryExpr>().is_some()
        || expr
            .children()
            .iter()
            .any(|child| physical_expr_has_scalar_subquery(child))
}

/// Materializes a complete single-partition build relation for all of its consumers.
fn create_shared_input(
    plan: PlannedSubtree,
    graph: &mut JobGraph,
    properties: Arc<PlanProperties>,
    scalar_context: Option<ScalarSubqueryContext<'_>>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    create_shuffle(
        plan,
        graph,
        properties,
        ShuffleConsumption::Multiple,
        scalar_context,
    )
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
        OutputDistribution::RoundRobinBatch { channels: 1 },
        TaskPlacement::Worker,
        OutputMode::Pipelined,
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
        OutputDistribution::RoundRobinBatch { channels: 1 },
        TaskPlacement::Worker,
        scalar_subquery_output_mode(graph),
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
        OutputDistribution::RoundRobinBatch { channels: 1 },
        TaskPlacement::Worker,
        OutputMode::Pipelined,
    )?;
    let properties = stage_properties_with_unknown_partitioning(graph, stage, output_partitions);
    Ok(stage_input_exec(
        stage,
        InputMode::Rescale {
            partitions: output_partitions,
        },
        properties,
    ))
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
            OutputDistribution::RoundRobinBatch { channels }
        }
        Partitioning::Hash(keys, channels) => OutputDistribution::Hash { keys, channels },
        Partitioning::Range(partitioning) => OutputDistribution::Range { partitioning },
    };
    let plan = wrap_pending_scalar_subqueries(plan, scalar_context);
    let stage = push_stage(
        plan,
        graph,
        distribution,
        TaskPlacement::Worker,
        shuffle_output_mode(graph),
    )?;
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    create_shuffle_input(stage, mode, properties, graph)
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
    let stage = push_stage(
        plan,
        graph,
        distribution,
        TaskPlacement::Worker,
        shuffle_output_mode(graph),
    )?;
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    create_shuffle_input(stage, mode, properties, graph)
}

fn shuffle_output_mode(graph: &JobGraph) -> OutputMode {
    match graph.options.shuffle_backend {
        ShuffleBackendKind::Storage { .. } | ShuffleBackendKind::Flight { .. } => {
            OutputMode::Pipelined
        }
        ShuffleBackendKind::Celeborn { .. } => OutputMode::Blocking,
    }
}

fn scalar_subquery_output_mode(graph: &JobGraph) -> OutputMode {
    match graph.options.shuffle_backend {
        ShuffleBackendKind::Flight { .. } => OutputMode::Pipelined,
        ShuffleBackendKind::Storage { .. } | ShuffleBackendKind::Celeborn { .. } => {
            OutputMode::Blocking
        }
    }
}

/// In storage shuffle mode, collect each logical shuffle partition in a separate
/// blocking stage. This turns the many task/channel streams produced by the upstream
/// stage into one (or a few size-bounded) object-storage files per output partition.
fn create_shuffle_input(
    stage: usize,
    mode: InputMode,
    properties: Arc<PlanProperties>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    if !matches!(
        graph.options.shuffle_backend,
        ShuffleBackendKind::Storage { .. }
    ) {
        return Ok(stage_input_exec(stage, mode, properties));
    }

    let merge = stage_input_exec(stage, mode, properties.clone());
    let stage = push_stage(
        merge,
        graph,
        OutputDistribution::RoundRobinBatch { channels: 1 },
        TaskPlacement::Worker,
        OutputMode::Blocking,
    )?;
    Ok(stage_input_exec(stage, InputMode::Forward, properties))
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
    mode: OutputMode,
) -> ExecutionResult<usize> {
    let (plan, inputs) = rewrite_inputs(plan)?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode,
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
        EquivalenceProperties::new(plan.schema()),
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
        OutputDistribution::RoundRobinBatch { channels: 1 },
        TaskPlacement::Driver,
        OutputMode::Pipelined,
    )?;
    let properties = graph.stages[stage].plan.properties().clone();
    Ok(stage_input_exec(stage, InputMode::Forward, properties))
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::{JoinType, NullEquality, ScalarValue};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion::physical_expr::{
        LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
    };
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::{
        HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
    };
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
    use sail_catalog::command::CatalogCommand;
    use sail_celeborn::common::PartitionSplitMode;
    use sail_physical_plan::barrier::BarrierExec;
    use sail_physical_plan::catalog_command::CatalogCommandExec;
    use sail_physical_plan::coalesce::CoalesceExec;
    use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
    use sail_physical_plan::repartition::ExplicitRepartitionExec;

    use super::{JobGraph, JobGraphOptions, create_scalar_subquery_input};
    use crate::job_graph::{
        InputMode, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
        reads_inputs_in_fixed_order,
    };
    use crate::plan::StageInputExec;
    use crate::shuffle::{ShuffleBackendKind, ShuffleCompression};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn empty_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    fn flight_shuffle_options() -> JobGraphOptions {
        JobGraphOptions {
            shuffle_backend: ShuffleBackendKind::Flight {
                compression: ShuffleCompression::None,
                connection_count: std::num::NonZeroUsize::MIN,
                initial_stream_window_size: None,
                initial_connection_window_size: None,
            },
        }
    }

    fn blocking_shuffle_options() -> JobGraphOptions {
        JobGraphOptions {
            shuffle_backend: ShuffleBackendKind::Storage {
                path: None,
                max_file_size: 1,
                compression: ShuffleCompression::None,
            },
        }
    }

    fn celeborn_shuffle_options() -> JobGraphOptions {
        JobGraphOptions {
            shuffle_backend: ShuffleBackendKind::Celeborn {
                master_endpoints: vec!["localhost:1".to_string()],
                compression: sail_celeborn::common::CompressionCodec::Lz4,
                endpoint_overrides: vec![],
                heartbeat_interval_secs: 10,
                partition_split_threshold: 1_i64 << 30,
                partition_split_mode: PartitionSplitMode::Soft,
            },
        }
    }

    fn shared_build_producer<'a>(graph: &'a JobGraph, consumer: &Stage) -> &'a Stage {
        let join = consumer.plan.downcast_ref::<HashJoinExec>().unwrap();
        let input = join.children()[0]
            .downcast_ref::<StageInputExec<usize>>()
            .unwrap();
        let mut input = &consumer.inputs[*input.input()];
        if matches!(
            graph.options.shuffle_backend,
            ShuffleBackendKind::Storage { .. }
        ) {
            assert!(matches!(input.mode, InputMode::Forward));
            let collector = &graph.stages[input.stage];
            assert!(matches!(collector.mode, OutputMode::Blocking));
            let source = collector
                .plan
                .downcast_ref::<StageInputExec<usize>>()
                .unwrap();
            input = &collector.inputs[*source.input()];
        }
        assert!(matches!(input.mode, InputMode::Broadcast));
        let producer = &graph.stages[input.stage];
        assert_eq!(producer.placement, TaskPlacement::Worker);
        assert_eq!(producer.plan.output_partitioning().partition_count(), 1);
        producer
    }

    #[test]
    fn lake_procedure_root_placement_is_preserved() {
        use datafusion::common::tree_node::TreeNode;
        use sail_common_datafusion::lakeprocedure::{
            LakeProcedure, LakeProcedureAccess, LakeProcedureCall, LakeProcedureInvocation,
            LakeProcedureInvocationId, LakeProcedureRootPlacement, LakeProcedureTarget,
        };
        use sail_physical_plan::lake_procedure::LakeProcedureExec;

        for (placement, expected) in [
            (
                LakeProcedureRootPlacement::Coordinator,
                TaskPlacement::Driver,
            ),
            (
                LakeProcedureRootPlacement::Distributed,
                TaskPlacement::Worker,
            ),
        ] {
            let procedure = LakeProcedure {
                name: "test".to_string(),
                parameters: vec![],
                output: vec![],
                access: LakeProcedureAccess::MetadataCommit,
                target: LakeProcedureTarget::Catalog,
            };
            let implementation = Arc::new(EmptyExec::new(procedure.schema()));
            let call = LakeProcedureCall {
                invocation_id: LakeProcedureInvocationId("test".to_string()),
                catalog: "test".to_string(),
                namespace: vec!["system".to_string()],
                lake_source: "iceberg".to_string(),
                target: None,
                invocation: LakeProcedureInvocation {
                    procedure,
                    arguments: vec![],
                },
            };
            let plan =
                Arc::new(LakeProcedureExec::try_new(call, implementation, placement).unwrap());
            let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();
            let stages = graph
                .stages
                .iter()
                .filter(|stage| {
                    stage
                        .plan
                        .exists(|plan| Ok(plan.is::<LakeProcedureExec>()))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(stages.len(), 1);
            assert_eq!(stages[0].placement, expected);
        }
    }

    #[test]
    fn hash_join_repartition_preserves_input_fetch() {
        for join_type in [
            JoinType::Left,
            JoinType::LeftAnti,
            JoinType::LeftSemi,
            JoinType::LeftMark,
            JoinType::Full,
        ] {
            for limited_side in [0, 1] {
                for fetch in [0, 3] {
                    let inputs = [0, 1].map(|side| {
                        let input = UnionExec::try_new(vec![empty_plan(); 4]).unwrap();
                        if side == limited_side {
                            Arc::new(CoalescePartitionsExec::new(input).with_fetch(Some(fetch)))
                                as Arc<dyn ExecutionPlan>
                        } else {
                            input
                        }
                    });
                    let key = col("id", &schema()).unwrap();
                    let join = HashJoinExec::try_new(
                        Arc::clone(&inputs[0]),
                        Arc::clone(&inputs[1]),
                        vec![(Arc::clone(&key), key)],
                        None,
                        &join_type,
                        None,
                        PartitionMode::CollectLeft,
                        NullEquality::NullEqualsNothing,
                        false,
                    )
                    .unwrap();
                    let graph =
                        JobGraph::try_new(Arc::new(join), flight_shuffle_options()).unwrap();
                    let stage = graph.stages.last().unwrap();
                    let join = stage.plan.downcast_ref::<HashJoinExec>().unwrap();
                    assert_eq!(*join.partition_mode(), PartitionMode::Partitioned);
                    let input = join.children()[limited_side]
                        .downcast_ref::<StageInputExec<usize>>()
                        .unwrap();
                    let producer = &graph.stages[stage.inputs[*input.input()].stage];
                    let limit = producer.plan.downcast_ref::<GlobalLimitExec>().unwrap();
                    assert_eq!(limit.skip(), 0);
                    assert_eq!(limit.fetch(), Some(fetch));
                    assert_eq!(limit.input().output_partitioning().partition_count(), 1);
                }
            }
        }
    }

    #[test]
    fn broadcast_limit_is_applied_once() {
        use datafusion::common::tree_node::TreeNode;

        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let limited =
                CoalescePartitionsExec::new(UnionExec::try_new(vec![empty_plan(); 4]).unwrap())
                    .with_fetch(Some(3));
            let key = col("id", &schema()).unwrap();
            let join = HashJoinExec::try_new(
                Arc::new(limited),
                UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                vec![(Arc::clone(&key), key)],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap();
            let graph = JobGraph::try_new(Arc::new(join), options).unwrap();
            let limits = graph
                .stages
                .iter()
                .filter(|stage| {
                    stage
                        .plan
                        .exists(|plan| Ok(plan.is::<GlobalLimitExec>()))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            // The limit is applied by one task, whose output every task of the join reads.
            assert_eq!(limits.len(), 1);
            assert_eq!(limits[0].plan.output_partitioning().partition_count(), 1);
            let join = graph
                .stages
                .iter()
                .find(|stage| {
                    stage
                        .plan
                        .exists(|plan| Ok(plan.is::<HashJoinExec>()))
                        .unwrap()
                })
                .unwrap();
            assert_eq!(join.plan.output_partitioning().partition_count(), 4);
        }
    }

    #[test]
    fn broadcast_global_limit_preserves_skip_and_fetch() {
        use datafusion::common::tree_node::TreeNode;

        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            for explicit_coalesce in [false, true] {
                for (skip, fetch) in [(0, Some(3)), (2, Some(3)), (2, None), (0, Some(0))] {
                    let input = UnionExec::try_new(vec![empty_plan(); 4]).unwrap();
                    let input = if explicit_coalesce {
                        Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
                    } else {
                        // Exercise the no-fetch coalesce inserted by JobGraph::try_new too.
                        input
                    };
                    let limited = Arc::new(GlobalLimitExec::new(input, skip, fetch));
                    let key = col("id", &schema()).unwrap();
                    let join = HashJoinExec::try_new(
                        limited,
                        UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                        vec![(Arc::clone(&key), key)],
                        None,
                        &JoinType::Inner,
                        None,
                        PartitionMode::CollectLeft,
                        NullEquality::NullEqualsNothing,
                        false,
                    )
                    .unwrap();
                    let graph = JobGraph::try_new(Arc::new(join), options.clone()).unwrap();
                    let stage = graph.stages.last().unwrap();
                    assert_eq!(stage.plan.output_partitioning().partition_count(), 4);
                    assert!(
                        !stage
                            .plan
                            .exists(|plan| Ok(plan.is::<GlobalLimitExec>()))
                            .unwrap()
                    );
                    let producer = shared_build_producer(&graph, stage);
                    let limit = producer.plan.downcast_ref::<GlobalLimitExec>().unwrap();
                    assert_eq!((limit.skip(), limit.fetch()), (skip, fetch));
                    assert_eq!(producer.plan.output_partitioning().partition_count(), 1);
                    assert_eq!(limit.input().output_partitioning().partition_count(), 1);
                }
            }
        }
    }

    #[test]
    fn shared_plain_exchanges_broadcast_without_an_extra_producer() {
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let input = UnionExec::try_new(vec![empty_plan(); 4]).unwrap();
            let builds: Vec<Arc<dyn ExecutionPlan>> = vec![
                Arc::new(CoalescePartitionsExec::new(input.clone())),
                Arc::new(
                    RepartitionExec::try_new(input.clone(), Partitioning::RoundRobinBatch(1))
                        .unwrap(),
                ),
                Arc::new(ExplicitRepartitionExec::new(
                    input.clone(),
                    Partitioning::RoundRobinBatch(1),
                )),
                Arc::new(CoalesceExec::new(input, 1)),
            ];
            for build in builds {
                let explicit = build.is::<ExplicitRepartitionExec>();
                let key = col("id", &schema()).unwrap();
                let join = HashJoinExec::try_new(
                    build,
                    UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                    vec![(key.clone(), key)],
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap();
                let graph = JobGraph::try_new(Arc::new(join), options.clone()).unwrap();
                let storage = matches!(options.shuffle_backend, ShuffleBackendKind::Storage { .. });
                assert_eq!(graph.stages.len(), if storage { 3 } else { 2 });
                let producer = &graph.stages[0];
                assert!(producer.plan.is::<UnionExec>());
                assert_eq!(producer.plan.output_partitioning().partition_count(), 4);
                assert_eq!(producer.placement, TaskPlacement::Worker);
                if explicit {
                    assert!(matches!(
                        producer.distribution,
                        OutputDistribution::RoundRobinRow { channels: 1 }
                    ));
                } else {
                    assert!(matches!(
                        producer.distribution,
                        OutputDistribution::RoundRobinBatch { channels: 1 }
                    ));
                }
                let consumer = graph.stages.last().unwrap();
                assert_eq!(consumer.plan.output_partitioning().partition_count(), 4);
                let join = consumer.plan.downcast_ref::<HashJoinExec>().unwrap();
                let input = join.children()[0]
                    .downcast_ref::<StageInputExec<usize>>()
                    .unwrap();
                assert_eq!(
                    input.properties().output_partitioning().partition_count(),
                    1
                );
                let mut input = &consumer.inputs[*input.input()];
                if storage {
                    assert!(matches!(input.mode, InputMode::Forward));
                    let collector = &graph.stages[input.stage];
                    assert!(matches!(collector.mode, OutputMode::Blocking));
                    let source = collector
                        .plan
                        .downcast_ref::<StageInputExec<usize>>()
                        .unwrap();
                    input = &collector.inputs[*source.input()];
                }
                assert_eq!(input.stage, 0);
                assert!(matches!(input.mode, InputMode::Broadcast));
                // In Storage mode the sole collector reads the producer once;
                // otherwise each probe needs its own broadcast stream replica.
                assert_eq!(graph.replicas(0), if storage { 1 } else { 4 });
            }
        }
    }

    #[test]
    fn shared_ordered_repartition_materializes_the_restored_sort() {
        let key = col("id", &schema()).unwrap();
        let ordering: LexOrdering = [PhysicalSortExpr::new_default(key.clone())].into();
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let source = Arc::new(
                SortExec::new(
                    ordering.clone(),
                    UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                )
                .with_preserve_partitioning(true),
            );
            let build = RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(1))
                .unwrap()
                .with_preserve_order();
            assert!(build.preserve_order());
            let join = HashJoinExec::try_new(
                Arc::new(build),
                UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                vec![(key.clone(), key.clone())],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap();
            let graph = JobGraph::try_new(Arc::new(join), options).unwrap();
            let consumer = graph.stages.last().unwrap();
            let producer = shared_build_producer(&graph, consumer);
            let sort = producer.plan.downcast_ref::<SortExec>().unwrap();
            assert!(sort.preserve_partitioning());
            assert_eq!(sort.expr(), &ordering);
            assert!(sort.input().properties().output_ordering().is_none());
            let join = consumer.plan.downcast_ref::<HashJoinExec>().unwrap();
            assert_eq!(join.children()[0].output_ordering(), Some(&ordering));
        }
    }

    #[test]
    fn shared_selection_roots_preserve_the_complete_plan() {
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            for fetch in [None, Some(0), Some(3)] {
                let input = UnionExec::try_new(vec![empty_plan(); 4]).unwrap();
                let coalesced =
                    Arc::new(CoalescePartitionsExec::new(input.clone())) as Arc<dyn ExecutionPlan>;
                let key = col("id", &schema()).unwrap();
                let ordering: LexOrdering = [PhysicalSortExpr::new_default(key.clone())].into();
                let filter = binary(key.clone(), Operator::Eq, key.clone(), &schema()).unwrap();
                let roots: Vec<Arc<dyn ExecutionPlan>> = vec![
                    Arc::new(SortExec::new(ordering.clone(), coalesced.clone()).with_fetch(fetch)),
                    Arc::new(
                        SortExec::new(
                            ordering.clone(),
                            Arc::new(ExplicitRepartitionExec::new(
                                input.clone(),
                                Partitioning::RoundRobinBatch(1),
                            )),
                        )
                        .with_fetch(fetch),
                    ),
                    FilterExec::try_new(filter, coalesced)
                        .unwrap()
                        .with_fetch(fetch)
                        .unwrap(),
                    Arc::new(
                        SortPreservingMergeExec::new(
                            ordering.clone(),
                            Arc::new(
                                SortExec::new(ordering, input).with_preserve_partitioning(true),
                            ),
                        )
                        .with_fetch(fetch),
                    ),
                ];
                for build in roots {
                    if fetch.is_none() && !build.is::<SortExec>() {
                        // Every join task computes a filter or merge without a fetch itself.
                        continue;
                    }
                    let name = build.name().to_string();
                    let ordering = build.output_ordering().cloned();
                    let join = HashJoinExec::try_new(
                        build,
                        UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                        vec![(key.clone(), key.clone())],
                        None,
                        &JoinType::Inner,
                        None,
                        PartitionMode::CollectLeft,
                        NullEquality::NullEqualsNothing,
                        false,
                    )
                    .unwrap();
                    let graph = JobGraph::try_new(Arc::new(join), options.clone()).unwrap();
                    let consumer = graph.stages.last().unwrap();
                    assert_eq!(consumer.plan.output_partitioning().partition_count(), 4);
                    let producer = shared_build_producer(&graph, consumer);
                    assert_eq!(producer.plan.name(), name);
                    assert_eq!(producer.plan.fetch(), fetch);
                    assert_eq!(producer.plan.output_ordering().cloned(), ordering);
                }
            }
        }
    }

    fn shared_join(build: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = build.schema();
        let key = col("id", &schema).unwrap();
        Arc::new(
            HashJoinExec::try_new(
                build,
                UnionExec::try_new(
                    (0..4)
                        .map(|_| Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>)
                        .collect(),
                )
                .unwrap(),
                vec![(key.clone(), key)],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn file_scan(file_groups: usize) -> Arc<dyn ExecutionPlan> {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::{
            FileGroup, FileScanConfigBuilder, ParquetSource,
        };
        use datafusion::datasource::source::DataSourceExec;

        let groups = (0..file_groups)
            .map(|i| FileGroup::new(vec![PartitionedFile::new(format!("build-{i}.parquet"), 1)]))
            .collect();
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(schema())),
        )
        .with_file_groups(groups)
        .build();
        DataSourceExec::from_data_source(config)
    }

    #[test]
    fn reproducible_shared_builds_stay_inline() {
        use datafusion::common::tree_node::TreeNode;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
        use datafusion::physical_plan::projection::ProjectionExec;
        use sail_logical_plan::range::Range;
        use sail_physical_plan::range::RangeExec;

        let key = col("id", &schema()).unwrap();
        let filter = || binary(key.clone(), Operator::Eq, key.clone(), &schema()).unwrap();
        let range_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let range = Arc::new(
            RangeExec::try_new(
                Range {
                    start: 0,
                    end: 10,
                    step: 1,
                },
                1,
                range_schema.clone(),
                vec![0],
            )
            .unwrap(),
        );
        let range_key = col("id", &range_schema).unwrap();
        let range_filter = binary(
            range_key.clone(),
            Operator::Lt,
            range_key.clone(),
            &range_schema,
        )
        .unwrap();
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let sources: Vec<Arc<dyn ExecutionPlan>> = vec![
                MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap(),
                Arc::new(
                    ProjectionExec::try_new(
                        vec![(range_key.clone(), "id".to_string())],
                        Arc::new(FilterExec::try_new(range_filter.clone(), range.clone()).unwrap()),
                    )
                    .unwrap(),
                ),
                Arc::new(CooperativeExec::new(Arc::new(
                    FilterExec::try_new(filter(), Arc::new(PlaceholderRowExec::new(schema())))
                        .unwrap(),
                ))),
                Arc::new(FilterExec::try_new(filter(), empty_plan()).unwrap()),
            ];
            for build in sources {
                let name = build.name().to_string();
                let graph = JobGraph::try_new(shared_join(build), options.clone()).unwrap();
                assert_eq!(graph.stages.len(), 1, "{name}");
                let join = graph.stages[0].plan.downcast_ref::<HashJoinExec>().unwrap();
                assert_eq!(join.children()[0].name(), name);
            }

            // Every join task reads the complete exchange output and filters it itself.
            let build = Arc::new(
                FilterExec::try_new(
                    filter(),
                    Arc::new(CoalescePartitionsExec::new(
                        UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                    )),
                )
                .unwrap(),
            );
            let graph = JobGraph::try_new(shared_join(build), options.clone()).unwrap();
            let storage = matches!(options.shuffle_backend, ShuffleBackendKind::Storage { .. });
            assert_eq!(graph.stages.len(), if storage { 3 } else { 2 });
            assert!(graph.stages[0].plan.is::<UnionExec>());
            let consumer = graph.stages.last().unwrap();
            let join = consumer.plan.downcast_ref::<HashJoinExec>().unwrap();
            assert!(join.children()[0].is::<FilterExec>());
            assert!(graph.stages[..graph.stages.len() - 1].iter().all(|stage| {
                !stage
                    .plan
                    .exists(|plan| Ok(plan.is::<FilterExec>()))
                    .unwrap()
            }));
        }
    }

    #[test]
    fn shared_merge_without_fetch_is_read_by_every_join_task() {
        let key = col("id", &schema()).unwrap();
        let ordering: LexOrdering = [PhysicalSortExpr::new_default(key)].into();
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let build = Arc::new(SortPreservingMergeExec::new(
                ordering.clone(),
                Arc::new(
                    SortExec::new(
                        ordering.clone(),
                        UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                    )
                    .with_preserve_partitioning(true),
                ),
            ));
            let graph = JobGraph::try_new(shared_join(build), options).unwrap();
            assert_eq!(graph.stages.len(), 2);
            let consumer = &graph.stages[1];
            assert_eq!(consumer.plan.output_partitioning().partition_count(), 4);
            let join = consumer.plan.downcast_ref::<HashJoinExec>().unwrap();
            let merge = join.children()[0]
                .downcast_ref::<SortPreservingMergeExec>()
                .unwrap();
            assert_eq!(merge.fetch(), None);
            assert_eq!(join.children()[0].output_ordering(), Some(&ordering));
            let input = merge
                .input()
                .downcast_ref::<StageInputExec<usize>>()
                .unwrap();
            let input = &consumer.inputs[*input.input()];
            assert_eq!(input.stage, 0);
            assert!(matches!(input.mode, InputMode::Merge));
            assert_eq!(
                graph.stages[0].plan.output_partitioning().partition_count(),
                4
            );
            assert_eq!(graph.replicas(0), 4);
        }
    }

    #[test]
    fn shared_builds_that_may_differ_per_task_are_materialized() {
        use datafusion::config::ConfigOptions;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_expr::ScalarFunctionExpr;
        use datafusion::physical_plan::projection::ProjectionExec;

        let key = col("id", &schema()).unwrap();
        let random = Arc::new(
            ScalarFunctionExpr::try_new(
                datafusion::functions::math::random(),
                vec![],
                schema().as_ref(),
                Arc::new(ConfigOptions::default()),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let aggregate_expr = Arc::new(
            AggregateExprBuilder::new(
                sum_udaf(),
                vec![
                    datafusion::physical_expr::expressions::cast(
                        key.clone(),
                        &schema(),
                        DataType::Int64,
                    )
                    .unwrap(),
                ],
            )
            .schema(schema())
            .alias("total")
            .build()
            .unwrap(),
        );
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let memory = || MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();
            let builds: Vec<Arc<dyn ExecutionPlan>> = vec![
                Arc::new(
                    ProjectionExec::try_new(
                        vec![
                            (key.clone(), "id".to_string()),
                            (random.clone(), "value".to_string()),
                        ],
                        memory(),
                    )
                    .unwrap(),
                ),
                Arc::new(GlobalLimitExec::new(memory(), 0, Some(1))),
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Single,
                        PhysicalGroupBy::new_single(vec![(key.clone(), "id".to_string())]),
                        vec![aggregate_expr.clone()],
                        vec![None],
                        Arc::new(CoalescePartitionsExec::new(
                            UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                        )),
                        schema(),
                    )
                    .unwrap(),
                ),
            ];
            for build in builds {
                let name = build.name().to_string();
                let graph = JobGraph::try_new(shared_join(build), options.clone()).unwrap();
                let producer = shared_build_producer(&graph, graph.stages.last().unwrap());
                assert_eq!(producer.plan.name(), name);
            }
        }
    }

    #[test]
    fn shared_file_scan_builds_are_scanned_once() {
        use datafusion::common::tree_node::TreeNode;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::physical_plan::projection::ProjectionExec;

        let key = col("id", &schema()).unwrap();
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let project = |input| {
                Arc::new(
                    ProjectionExec::try_new(vec![(key.clone(), "id".to_string())], input).unwrap(),
                ) as Arc<dyn ExecutionPlan>
            };
            for build in [
                project(file_scan(1)),
                // As on main, a file scan anywhere in the build materializes it.
                project(Arc::new(CoalescePartitionsExec::new(file_scan(2)))),
            ] {
                let graph = JobGraph::try_new(shared_join(build), options.clone()).unwrap();
                let consumer = graph.stages.last().unwrap();
                let producer = shared_build_producer(&graph, consumer);
                assert!(producer.plan.is::<ProjectionExec>());
                assert!(
                    !consumer
                        .plan
                        .exists(|plan| Ok(plan.is::<DataSourceExec>()))
                        .unwrap()
                );
            }
        }
    }

    #[test]
    fn single_probe_build_stays_inline_unless_the_whole_join_is_shared() {
        use datafusion::common::tree_node::TreeNode;

        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let key = col("id", &schema()).unwrap();
            let inner = Arc::new(
                HashJoinExec::try_new(
                    Arc::new(GlobalLimitExec::new(empty_plan(), 0, Some(3))),
                    empty_plan(),
                    vec![(key.clone(), key.clone())],
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>;
            let graph = JobGraph::try_new(inner.clone(), options.clone()).unwrap();
            assert_eq!(graph.stages.len(), 1);

            let outer = HashJoinExec::try_new(
                inner,
                UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                vec![(key.clone(), key)],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap();
            let graph = JobGraph::try_new(Arc::new(outer), options.clone()).unwrap();
            let producer = shared_build_producer(&graph, graph.stages.last().unwrap());
            assert!(producer.plan.is::<HashJoinExec>());
            assert!(
                producer
                    .plan
                    .exists(|plan| Ok(plan.is::<GlobalLimitExec>()))
                    .unwrap()
            );
            if matches!(options.shuffle_backend, ShuffleBackendKind::Flight { .. }) {
                assert_eq!(graph.stages.len(), 2);
            }
        }
    }

    #[test]
    fn unshared_global_limit_stays_in_its_consuming_stage() {
        let input = UnionExec::try_new(vec![empty_plan(); 4]).unwrap();
        let plan = Arc::new(GlobalLimitExec::new(input, 2, Some(3)));
        let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();
        assert_eq!(graph.stages.len(), 2);
        let stage = graph.stages.last().unwrap();
        let limit = stage.plan.downcast_ref::<GlobalLimitExec>().unwrap();
        assert_eq!((limit.skip(), limit.fetch()), (2, Some(3)));
        assert_eq!(limit.input().output_partitioning().partition_count(), 1);
        assert!(
            stage
                .inputs
                .iter()
                .all(|input| matches!(input.mode, InputMode::Shuffle))
        );
    }

    /// A shared CTE with the partitions of its body, and a function that returns its
    /// references.
    fn shared_cte(
        id: usize,
        body: &Arc<dyn ExecutionPlan>,
    ) -> (
        Arc<sail_physical_plan::shared_cte::SharedCte>,
        impl Fn() -> Arc<dyn ExecutionPlan> + use<>,
    ) {
        use datafusion::common::Statistics;
        use sail_physical_plan::shared_cte::{SharedCte, SharedCteScanExec};

        let cte = Arc::new(SharedCte::new(
            id,
            format!("c{id}"),
            body.output_partitioning().clone(),
        ));
        let scan = {
            let cte = Arc::clone(&cte);
            move || -> Arc<dyn ExecutionPlan> {
                Arc::new(SharedCteScanExec::new(
                    Arc::clone(&cte),
                    schema(),
                    &Statistics::new_unknown(&schema()),
                    None,
                ))
            }
        };
        (cte, scan)
    }

    /// Returns the modes of the inputs that read the given stage.
    fn reader_modes(graph: &JobGraph, stage: usize) -> Vec<InputMode> {
        graph
            .stages
            .iter()
            .flat_map(|x| x.inputs.iter().filter(|i| i.stage == stage).map(|i| i.mode))
            .collect()
    }

    #[test]
    fn shared_cte_is_a_pipelined_stage_read_by_each_reference_with_flight_shuffle() {
        use sail_physical_plan::shared_cte::{SharedCteExec, share_ctes};

        let body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
        let (cte, scan) = shared_cte(0, &body);
        let plan = share_ctes(
            UnionExec::try_new(vec![scan(), scan(), scan()]).unwrap(),
            vec![(cte, body)],
        )
        .unwrap();
        let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();
        // The CTE is planned once, and the references are read in the last stage.
        assert_eq!(graph.stages.len(), 2);
        assert!(graph.stages[0].plan.is::<UnionExec>());
        assert!(graph.stages[0].inputs.is_empty());
        assert!(matches!(graph.stages[0].mode, OutputMode::Pipelined));
        assert_eq!(graph.stages[0].distribution.channels(), 1);
        let modes = reader_modes(&graph, 0);
        assert_eq!(modes.len(), 3);
        assert!(
            modes
                .iter()
                .all(|x| matches!(x, InputMode::Rescale { partitions: 2 }))
        );
        // Each reference reads its own copy of the output at its own pace.
        assert_eq!(graph.replicas(0), 3);
        assert!(!graph.bounded_output(0));

        // The output of a shared CTE with a single reference has a single reader.
        let body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
        let (cte, scan) = shared_cte(0, &body);
        let plan = Arc::new(SharedCteExec::new(scan(), vec![(cte, body)]));
        let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();
        assert_eq!(graph.stages.len(), 2);
        assert_eq!(graph.replicas(0), 1);
        assert!(graph.bounded_output(0));
    }

    #[test]
    fn shared_cte_is_a_blocking_stage_with_blocking_shuffle_backends() {
        use sail_physical_plan::shared_cte::share_ctes;

        let join = |build: Arc<dyn ExecutionPlan>, probe: Arc<dyn ExecutionPlan>| {
            let key = col("id", &schema()).unwrap();
            Arc::new(
                HashJoinExec::try_new(
                    Arc::new(CoalescePartitionsExec::new(build)),
                    probe,
                    vec![(Arc::clone(&key), key)],
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>
        };
        for options in [blocking_shuffle_options(), celeborn_shuffle_options()] {
            let celeborn = matches!(options.shuffle_backend, ShuffleBackendKind::Celeborn { .. });
            for joined in [false, true] {
                let body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
                let (cte, scan) = shared_cte(0, &body);
                let input = if joined {
                    join(scan(), scan())
                } else {
                    UnionExec::try_new(vec![scan(), scan()]).unwrap()
                };
                let plan = share_ctes(input, vec![(cte, body)]).unwrap();
                let graph = JobGraph::try_new(plan, options.clone()).unwrap();
                // The CTE is planned once.
                let stages = (0..graph.stages.len())
                    .filter(|&s| {
                        let stage = &graph.stages[s];
                        stage.plan.is::<UnionExec>() && stage.inputs.is_empty()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(stages, vec![0]);
                assert!(matches!(graph.stages[0].mode, OutputMode::Blocking));
                // The shuffle service serves each partition of the CTE as a channel.
                assert_eq!(
                    graph.stages[0].distribution.channels(),
                    if celeborn { 2 } else { 1 }
                );
                let modes = reader_modes(&graph, 0);
                assert_eq!(modes.len(), 2);
                assert!(modes.iter().all(|x| if celeborn {
                    matches!(x, InputMode::Shuffle)
                } else {
                    matches!(x, InputMode::Rescale { partitions: 2 })
                }));
            }
        }
    }

    #[test]
    fn shared_cte_reference_as_a_shared_build_is_broadcast_without_another_stage() {
        use sail_physical_plan::shared_cte::share_ctes;

        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            let flight = matches!(options.shuffle_backend, ShuffleBackendKind::Flight { .. });
            let body = empty_plan();
            let (cte, scan) = shared_cte(0, &body);
            let plan = share_ctes(
                UnionExec::try_new(vec![shared_join(scan()), shared_join(scan())]).unwrap(),
                vec![(cte, body)],
            )
            .unwrap();
            let graph = JobGraph::try_new(plan, options).unwrap();
            assert_eq!(graph.stages.len(), 2);
            assert!(graph.stages[0].plan.is::<EmptyExec>());
            if flight {
                assert!(matches!(graph.stages[0].mode, OutputMode::Pipelined));
            } else {
                assert!(matches!(graph.stages[0].mode, OutputMode::Blocking));
            }
            assert_eq!(graph.stages[0].distribution.channels(), 1);
            let modes = reader_modes(&graph, 0);
            assert_eq!(modes.len(), 2);
            assert!(modes.iter().all(|x| matches!(x, InputMode::Broadcast)));
        }
    }

    #[test]
    fn nested_shared_ctes_are_planned_as_one_stage_each() {
        use sail_physical_plan::shared_cte::share_ctes;

        let join = |build: Arc<dyn ExecutionPlan>, probe: Arc<dyn ExecutionPlan>| {
            let key = col("id", &schema()).unwrap();
            Arc::new(
                HashJoinExec::try_new(
                    Arc::new(CoalescePartitionsExec::new(build)),
                    probe,
                    vec![(Arc::clone(&key), key)],
                    None,
                    &JoinType::Inner,
                    Some(vec![0]),
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>
        };
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            // Each CTE joins two references to the CTE before it.
            let stages = |depth: usize| {
                let mut body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
                let mut ctes = vec![];
                for id in 0..depth {
                    let (cte, scan) = shared_cte(id, &body);
                    ctes.push((cte, body));
                    body = join(scan(), scan());
                }
                let plan = share_ctes(body, ctes).unwrap();
                JobGraph::try_new(plan, options.clone())
                    .unwrap()
                    .stages
                    .len()
            };
            // The number of stages grows by the same number for each CTE.
            let (a, b, c) = (stages(4), stages(8), stages(12));
            assert!(a < b);
            assert_eq!(b - a, c - b);
        }
    }

    #[test]
    fn null_aware_hash_join_consumes_both_inputs_in_one_task() {
        for options in [flight_shuffle_options(), blocking_shuffle_options()] {
            for (left_partitions, right_partitions) in [(1, 1), (1, 4), (4, 1), (4, 4)] {
                for null_aware in [false, true] {
                    let key = col("id", &schema()).unwrap();
                    let join = HashJoinExec::try_new(
                        UnionExec::try_new(vec![empty_plan(); left_partitions]).unwrap(),
                        UnionExec::try_new(vec![empty_plan(); right_partitions]).unwrap(),
                        vec![(Arc::clone(&key), key)],
                        None,
                        &JoinType::LeftAnti,
                        Some(vec![0]),
                        PartitionMode::CollectLeft,
                        NullEquality::NullEqualsNothing,
                        null_aware,
                    )
                    .unwrap();
                    let expected_schema = join.schema();
                    let graph = JobGraph::try_new(Arc::new(join), options.clone()).unwrap();
                    let stage = graph.stages.last().unwrap();
                    let join = stage.plan.downcast_ref::<HashJoinExec>().unwrap();
                    assert_eq!(join.null_aware, null_aware);
                    assert_eq!(join.schema(), expected_schema);
                    assert_eq!(
                        *join.partition_mode(),
                        if null_aware {
                            PartitionMode::CollectLeft
                        } else {
                            PartitionMode::Partitioned
                        }
                    );
                    let partitions = if null_aware { 1 } else { right_partitions };
                    assert_eq!(
                        stage.plan.output_partitioning().partition_count(),
                        partitions
                    );
                    for input in join.children() {
                        assert_eq!(input.output_partitioning().partition_count(), partitions);
                    }
                }
            }
        }
    }

    #[test]
    fn nested_loop_build_output_has_one_probe_partition() {
        use datafusion::common::JoinType;
        use datafusion::physical_plan::joins::NestedLoopJoinExec;

        for (join_type, partitions) in [
            (JoinType::Left, 1),
            (JoinType::LeftAnti, 1),
            (JoinType::LeftSemi, 1),
            (JoinType::LeftMark, 1),
            (JoinType::Full, 1),
            (JoinType::Inner, 4),
            (JoinType::Right, 4),
            (JoinType::RightAnti, 4),
            (JoinType::RightSemi, 4),
            (JoinType::RightMark, 4),
        ] {
            let right = Arc::new(
                RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(4)).unwrap(),
            );
            let join = Arc::new(
                NestedLoopJoinExec::try_new(empty_plan(), right, None, &join_type, None).unwrap(),
            );
            let graph = JobGraph::try_new(join, flight_shuffle_options()).unwrap();
            let stage = graph.stages.last().unwrap();
            let join = stage.plan.downcast_ref::<NestedLoopJoinExec>().unwrap();
            assert_eq!(
                join.right().output_partitioning().partition_count(),
                partitions
            );
            let probe = join
                .right()
                .downcast_ref::<StageInputExec<usize>>()
                .unwrap();
            let input = &stage.inputs[*probe.input()];
            assert!(matches!(input.mode, InputMode::Shuffle));
            assert!(matches!(
                graph.stages[input.stage].distribution,
                OutputDistribution::RoundRobinBatch { channels } if channels == partitions
            ));
        }
    }

    #[test]
    fn test_job_graph_distinguishes_batch_and_explicit_round_robin_shuffle() {
        let generic_graph = JobGraph::try_new(
            Arc::new(
                RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(4)).unwrap(),
            ),
            flight_shuffle_options(),
        )
        .unwrap();
        let explicit_graph = JobGraph::try_new(
            Arc::new(ExplicitRepartitionExec::new(
                empty_plan(),
                Partitioning::RoundRobinBatch(4),
            )),
            flight_shuffle_options(),
        )
        .unwrap();

        assert_eq!(generic_graph.stages().len(), 2);
        assert!(matches!(
            &generic_graph.stages()[0].distribution,
            OutputDistribution::RoundRobinBatch { channels: 4 }
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
    fn test_job_graph_preserves_range_shuffle() {
        let schema = schema();
        let ordering = [PhysicalSortExpr::new_default(col("id", &schema).unwrap())].into();
        let range = RangePartitioning::try_new(
            ordering,
            vec![SplitPoint::new(vec![ScalarValue::Int32(Some(10))])],
        )
        .unwrap();
        let graph = JobGraph::try_new(
            Arc::new(
                RepartitionExec::try_new(empty_plan(), Partitioning::Range(range.clone())).unwrap(),
            ),
            flight_shuffle_options(),
        )
        .unwrap();

        assert_eq!(graph.stages().len(), 2);
        assert!(matches!(
            &graph.stages()[0].distribution,
            OutputDistribution::Range { .. }
        ));
        let OutputDistribution::Range { partitioning } = &graph.stages()[0].distribution else {
            return;
        };
        assert_eq!((*partitioning).clone(), range);
        assert_eq!(graph.stages()[0].distribution.channels(), 2);
    }

    #[test]
    fn test_order_preserving_shuffle_sorts_after_receiving_rows() {
        let ordering: LexOrdering =
            [PhysicalSortExpr::new_default(col("id", &schema()).unwrap())].into();
        for options in [flight_shuffle_options(), blocking_shuffle_options()] {
            for partitioning in [
                Partitioning::RoundRobinBatch(4),
                Partitioning::Hash(vec![col("id", &schema()).unwrap()], 4),
            ] {
                let source = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
                let source = Arc::new(
                    SortExec::new(ordering.clone(), source).with_preserve_partitioning(true),
                );
                let repartition = RepartitionExec::try_new(source, partitioning.clone())
                    .unwrap()
                    .with_preserve_order();
                let graph = JobGraph::try_new(Arc::new(repartition), options.clone()).unwrap();
                let output = graph
                    .stages()
                    .last()
                    .unwrap()
                    .plan
                    .downcast_ref::<SortExec>()
                    .unwrap();
                assert!(output.preserve_partitioning());
                assert_eq!(output.expr(), &ordering);
                assert_eq!(output.properties().output_partitioning(), &partitioning);
                let input = output
                    .input()
                    .downcast_ref::<StageInputExec<usize>>()
                    .unwrap();
                assert!(input.properties().output_ordering().is_none());
            }
        }
    }

    #[test]
    fn test_storage_shuffle_inserts_a_blocking_merge_stage() {
        let graph = JobGraph::try_new(
            Arc::new(
                RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(4)).unwrap(),
            ),
            blocking_shuffle_options(),
        )
        .unwrap();

        assert_eq!(graph.stages().len(), 3);
        assert!(matches!(graph.stages()[0].mode, OutputMode::Pipelined));
        assert!(matches!(
            graph.stages()[1].inputs.as_slice(),
            [StageInput {
                stage: 0,
                mode: InputMode::Shuffle,
            }]
        ));
        assert!(matches!(graph.stages()[1].mode, OutputMode::Blocking));
        assert!(matches!(
            graph.stages()[2].inputs.as_slice(),
            [StageInput {
                stage: 1,
                mode: InputMode::Forward,
            }]
        ));
    }

    #[test]
    fn test_celeborn_shuffle_uses_the_shuffle_service_stage() {
        let graph = JobGraph::try_new(
            Arc::new(
                RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(4)).unwrap(),
            ),
            celeborn_shuffle_options(),
        )
        .unwrap();

        assert_eq!(graph.stages().len(), 2);
        assert!(matches!(graph.stages()[0].mode, OutputMode::Blocking));
        assert!(matches!(
            graph.stages()[1].inputs.as_slice(),
            [StageInput {
                stage: 0,
                mode: InputMode::Shuffle,
            }]
        ));
    }

    #[test]
    fn test_scalar_subquery_input_uses_blocking_shuffle_backends() {
        for (options, is_blocking) in [
            (blocking_shuffle_options(), true),
            (celeborn_shuffle_options(), true),
            (flight_shuffle_options(), false),
        ] {
            let mut graph = JobGraph {
                stages: vec![],
                schema: schema(),
                options,
            };
            let input = create_scalar_subquery_input(&empty_plan(), &mut graph).unwrap();

            assert_eq!(
                matches!(graph.stages()[0].mode, OutputMode::Blocking),
                is_blocking
            );
            assert!(matches!(
                input.downcast_ref::<StageInputExec<StageInput>>(),
                Some(stage) if matches!(stage.input().mode, InputMode::Broadcast)
            ));
        }
    }

    #[test]
    fn test_celeborn_explicit_repartition_uses_a_blocking_stage() {
        let graph = JobGraph::try_new(
            Arc::new(ExplicitRepartitionExec::new(
                empty_plan(),
                Partitioning::RoundRobinBatch(4),
            )),
            celeborn_shuffle_options(),
        )
        .unwrap();

        assert!(matches!(graph.stages()[0].mode, OutputMode::Blocking));
    }

    #[test]
    fn test_job_graph_uses_rescale_input_for_coalesce_exec() {
        let input =
            UnionExec::try_new(vec![empty_plan(), empty_plan(), empty_plan(), empty_plan()])
                .unwrap();
        for output_partitions in [1, 2] {
            let graph = JobGraph::try_new(
                Arc::new(CoalesceExec::new(input.clone(), output_partitions)),
                flight_shuffle_options(),
            )
            .unwrap();

            assert_eq!(graph.stages().len(), 2);
            assert_eq!(
                graph.stages()[1]
                    .plan
                    .output_partitioning()
                    .partition_count(),
                output_partitions
            );
            assert!(matches!(
                graph.stages()[1].inputs.as_slice(),
                [StageInput {
                    stage: 0,
                    mode: InputMode::Rescale { partitions },
                }] if *partitions == output_partitions
            ));
        }
    }

    #[test]
    fn iceberg_rewrite_concurrency_bounds_the_reader_writer_stage() {
        use datafusion::common::tree_node::TreeNode;
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
        use sail_common_datafusion::datasource::PhysicalSinkMode;
        use sail_iceberg::SnapshotUpdateKind;
        use sail_iceberg::physical_plan::{
            IcebergCommitExec, IcebergFileTasksExec, IcebergRewriteExec,
            IcebergScanByDataFilesExec, IcebergWriterExec, IcebergWriterExecOptions,
            prepare_iceberg_write_context,
        };
        use url::Url;

        let table_url = Url::parse("memory://bucket/rewrite").unwrap();
        let tasks = Arc::new(IcebergFileTasksExec::try_new(vec![vec![]; 6]).unwrap());
        let scan = Arc::new(
            IcebergScanByDataFilesExec::new(
                tasks,
                table_url.to_string(),
                schema(),
                None,
                None,
                None,
            )
            .unwrap()
            .preserve_file_groups(),
        );
        let options = IcebergWriterExecOptions::default();
        let context = prepare_iceberg_write_context(
            &table_url,
            None,
            &options,
            &[],
            &PhysicalSinkMode::Append,
            &schema(),
        )
        .unwrap();
        let writer = Arc::new(
            IcebergWriterExec::new(
                scan,
                table_url.clone(),
                vec![],
                PhysicalSinkMode::Append,
                false,
                options,
                context,
            )
            .unwrap(),
        );
        let runner = Arc::new(
            IcebergRewriteExec::try_new(writer, vec![vec![0, 2, 4], vec![1, 3, 5]]).unwrap(),
        );
        let commit = Arc::new(IcebergCommitExec::new(
            Arc::new(CoalescePartitionsExec::new(runner)),
            table_url,
            None,
            SnapshotUpdateKind::RewriteDataFiles,
        ));
        let graph = JobGraph::try_new(commit, flight_shuffle_options()).unwrap();
        let stages = graph
            .stages()
            .iter()
            .filter(|stage| {
                stage
                    .plan
                    .exists(|plan| Ok(plan.is::<IcebergRewriteExec>()))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(stages.len(), 1);
        let stage = stages[0];
        assert_eq!(stage.placement, TaskPlacement::Worker);
        assert_eq!(stage.plan.output_partitioning().partition_count(), 2);
        assert!(stage.inputs.is_empty());
        assert!(
            stage
                .plan
                .exists(|plan| Ok(plan.is::<IcebergWriterExec>()))
                .unwrap()
        );
        assert!(
            stage
                .plan
                .exists(|plan| Ok(plan.is::<IcebergScanByDataFilesExec>()))
                .unwrap()
        );
        assert!(
            stage
                .plan
                .exists(|plan| Ok(plan.is::<IcebergFileTasksExec>()
                    && plan.output_partitioning().partition_count() == 6))
                .unwrap()
        );
        let commits = graph
            .stages()
            .iter()
            .filter(|stage| stage.plan.is::<IcebergCommitExec>())
            .collect::<Vec<_>>();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].placement, TaskPlacement::Driver);
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

        let graph = JobGraph::try_new(commit, flight_shuffle_options()).unwrap();

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
        let graph = JobGraph::try_new(
            Arc::new(BarrierExec::new(vec![precondition], command)),
            flight_shuffle_options(),
        )
        .unwrap();

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
    fn shared_build_keeps_scalar_subquery_context() {
        for wrapper_is_shared in [false, true] {
            let results = ScalarSubqueryResults::new(1);
            let predicate = Arc::new(ScalarSubqueryExpr::new(
                DataType::Boolean,
                false,
                SubqueryIndex::new(0),
                results.clone(),
            ));
            // The fetch keeps the filter from being recomputed by every join task.
            let filtered = FilterExec::try_new(predicate, empty_plan())
                .unwrap()
                .with_fetch(Some(1))
                .unwrap();
            let wrap =
                |input| {
                    Arc::new(ScalarSubqueryExec::new(
                        input,
                        vec![ScalarSubqueryLink {
                            plan: Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
                                Field::new("keep", DataType::Boolean, true),
                            ])))),
                            index: SubqueryIndex::new(0),
                        }],
                        results.clone(),
                    )) as Arc<dyn ExecutionPlan>
                };
            let build = if wrapper_is_shared {
                wrap(filtered)
            } else {
                filtered
            };
            let key = col("id", &schema()).unwrap();
            let join = Arc::new(
                HashJoinExec::try_new(
                    build,
                    UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
                    vec![(key.clone(), key)],
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>;
            let plan = if wrapper_is_shared { join } else { wrap(join) };
            let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();
            let producer = shared_build_producer(&graph, graph.stages.last().unwrap());
            let scalar = producer.plan.downcast_ref::<ScalarSubqueryExec>().unwrap();
            assert_eq!(scalar.subqueries().len(), 1);
            let filter = scalar.input().downcast_ref::<FilterExec>().unwrap();
            let predicate = filter
                .predicate()
                .downcast_ref::<ScalarSubqueryExpr>()
                .unwrap();
            assert!(ScalarSubqueryResults::ptr_eq(scalar.results(), &results));
            assert!(ScalarSubqueryResults::ptr_eq(
                scalar.results(),
                predicate.results()
            ));
            let link = scalar.subqueries()[0]
                .plan
                .downcast_ref::<StageInputExec<usize>>()
                .unwrap();
            assert!(matches!(
                producer.inputs[*link.input()].mode,
                InputMode::Broadcast
            ));
            assert_eq!(
                graph
                    .stages
                    .iter()
                    .filter(|stage| stage.plan.is::<ScalarSubqueryExec>())
                    .count(),
                1
            );
        }
    }

    #[test]
    fn shared_build_keeps_driver_barrier_and_preconditions() {
        let precondition = Arc::new(
            RepartitionExec::try_new(empty_plan(), Partitioning::RoundRobinBatch(1)).unwrap(),
        );
        let command = Arc::new(CooperativeExec::new(Arc::new(CatalogCommandExec::new(
            CatalogCommand::CurrentCatalog,
            schema(),
        ))));
        let build = Arc::new(BarrierExec::new(vec![precondition], command));
        let key = col("id", &schema()).unwrap();
        let join = HashJoinExec::try_new(
            build,
            UnionExec::try_new(vec![empty_plan(); 4]).unwrap(),
            vec![(key.clone(), key)],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap();
        let graph = JobGraph::try_new(Arc::new(join), flight_shuffle_options()).unwrap();
        let producer = shared_build_producer(&graph, graph.stages.last().unwrap());
        let input = producer
            .plan
            .downcast_ref::<StageInputExec<usize>>()
            .unwrap();
        let input = &producer.inputs[*input.input()];
        assert!(matches!(input.mode, InputMode::Forward));
        let driver = &graph.stages[input.stage];
        assert_eq!(driver.placement, TaskPlacement::Driver);
        let barrier = driver.plan.downcast_ref::<BarrierExec>().unwrap();
        let command = barrier.plan().downcast_ref::<CooperativeExec>().unwrap();
        assert!(command.input().is::<CatalogCommandExec>());
        assert_eq!(barrier.preconditions().len(), 1);
        let precondition = barrier.preconditions()[0]
            .downcast_ref::<StageInputExec<usize>>()
            .unwrap();
        assert!(matches!(
            driver.inputs[*precondition.input()].mode,
            InputMode::Shuffle
        ));
        assert_eq!(
            graph
                .stages
                .iter()
                .filter(|stage| stage.plan.is::<BarrierExec>())
                .count(),
            1
        );
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

        let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();

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

        let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();

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

        let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();
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

    /// Plans a job, and returns for each stage and each of its inputs whether the
    /// input is neither broadcast nor merged in order, and whether the tasks that
    /// produce it wait for their readers.
    fn bounded_inputs(plan: Arc<dyn ExecutionPlan>) -> Vec<Vec<(bool, bool)>> {
        let graph = JobGraph::try_new(plan, flight_shuffle_options()).unwrap();
        let stages = graph.stages().iter();
        stages
            .map(|stage| {
                let inputs = stage.inputs.iter().map(|input| {
                    let streamed = !matches!(input.mode, InputMode::Broadcast | InputMode::Merge);
                    (streamed, graph.bounded_output(input.stage))
                });
                inputs.collect()
            })
            .collect()
    }

    /// Creates the exchange of some partitions by a key.
    fn shuffle(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let partitioning = Partitioning::Hash(vec![col("id", &schema()).unwrap()], 4);
        Arc::new(RepartitionExec::try_new(input, partitioning).unwrap())
    }

    fn source() -> Arc<dyn ExecutionPlan> {
        UnionExec::try_new(vec![empty_plan(); 2]).unwrap()
    }

    fn sort_merge_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let key = |input: &Arc<dyn ExecutionPlan>| col("id", &input.schema()).unwrap();
        let sort = |input: Arc<dyn ExecutionPlan>| {
            let ordering: LexOrdering = [PhysicalSortExpr::new_default(key(&input))].into();
            Arc::new(SortExec::new(ordering, input).with_preserve_partitioning(true))
        };
        let on = vec![(key(&left), key(&right))];
        let join = SortMergeJoinExec::try_new(
            sort(left),
            sort(right),
            on,
            None,
            JoinType::Inner,
            vec![SortOptions::default()],
            NullEquality::NullEqualsNothing,
        );
        Arc::new(join.unwrap())
    }

    #[test]
    fn test_bounded_output_under_an_ordered_merge() {
        let schema = schema();
        let ordering: LexOrdering =
            [PhysicalSortExpr::new_default(col("id", &schema).unwrap())].into();
        let predicate = binary(
            col("id", &schema).unwrap(),
            Operator::Gt,
            lit(0_i32),
            &schema,
        )
        .unwrap();

        // The exchange of a single sorted partition keeps the order of the rows in each
        // channel, and the merge on top reads the channels in the order of the data.
        let source = Arc::new(SortExec::new(ordering.clone(), empty_plan()));
        let exchange =
            Arc::new(RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(4)).unwrap());
        let filter = Arc::new(FilterExec::try_new(predicate, exchange).unwrap());
        let merge = SortPreservingMergeExec::new(ordering, filter);
        let graph = JobGraph::try_new(Arc::new(merge), flight_shuffle_options()).unwrap();
        assert_eq!(graph.stages().len(), 3);
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
                mode: InputMode::Merge,
            }]
        ));
        // The tasks under the merge cannot wait for it, since the merge may need the
        // rest of one channel before it takes anything else from the other channels.
        // So they always take the data of the exchange, whose task can wait for them.
        assert!(graph.bounded_output(0));
        assert!(!graph.bounded_output(1));
        assert!(!graph.bounded_output(2));
    }

    #[test]
    fn test_bounded_output_requires_a_single_reader_that_reads_as_the_data_comes() {
        let stage = |inputs: Vec<(usize, InputMode)>| Stage {
            inputs: inputs
                .into_iter()
                .map(|(stage, mode)| StageInput { stage, mode })
                .collect(),
            plan: empty_plan(),
            group: String::new(),
            mode: OutputMode::Pipelined,
            distribution: OutputDistribution::RoundRobinBatch { channels: 1 },
            placement: TaskPlacement::Worker,
        };
        let graph = JobGraph {
            stages: vec![
                stage(vec![]),
                stage(vec![]),
                stage(vec![]),
                stage(vec![]),
                stage(vec![]),
                stage(vec![]),
                stage(vec![
                    (0, InputMode::Shuffle),
                    (1, InputMode::Forward),
                    (2, InputMode::Rescale { partitions: 2 }),
                    (3, InputMode::Broadcast),
                    (4, InputMode::Merge),
                    (5, InputMode::Broadcast),
                ]),
                stage(vec![(5, InputMode::Broadcast), (6, InputMode::Shuffle)]),
            ],
            schema: schema(),
            options: flight_shuffle_options(),
        };
        // The stages with a single reader that takes the data as it comes.
        assert!(graph.bounded_output(0));
        assert!(graph.bounded_output(1));
        assert!(graph.bounded_output(2));
        assert!(graph.bounded_output(6));
        // The output is read in full by every task of the reading stage.
        assert!(!graph.bounded_output(3));
        // The output is merged in order.
        assert!(!graph.bounded_output(4));
        // The output is read by two stages.
        assert!(!graph.bounded_output(5));
        // The output is the result of the job.
        assert!(!graph.bounded_output(7));
    }

    #[test]
    fn test_bounded_output_with_operators_that_read_inputs_in_fixed_order() {
        let key = || col("id", &schema()).unwrap();
        let hash_join = |mode| {
            let join = HashJoinExec::try_new(
                shuffle(source()),
                shuffle(source()),
                vec![(key(), key())],
                None,
                &JoinType::Inner,
                None,
                mode,
                NullEquality::NullEqualsNothing,
                false,
            );
            Arc::new(join.unwrap()) as Arc<dyn ExecutionPlan>
        };
        let nested_loop_join = NestedLoopJoinExec::try_new(
            shuffle(source()),
            shuffle(source()),
            None,
            &JoinType::Inner,
            None,
        );
        let inputs = vec![shuffle(source()), shuffle(source())];
        let interleave = InterleaveExec::try_new(inputs.clone());
        let plans: Vec<Arc<dyn ExecutionPlan>> = vec![
            hash_join(PartitionMode::Partitioned),
            hash_join(PartitionMode::CollectLeft),
            Arc::new(nested_loop_join.unwrap()),
            UnionExec::try_new(inputs).unwrap(),
            Arc::new(interleave.unwrap()),
        ];
        for plan in plans {
            // The producers of all inputs wait, unless the input is broadcast.
            let stages = bounded_inputs(plan.clone());
            let inputs = stages.last().unwrap();
            let plan = displayable(plan.as_ref()).one_line().to_string();
            assert_eq!(inputs.len(), 2, "{plan}");
            assert!(inputs.iter().any(|(_, bounded)| *bounded), "{plan}");
            for (streamed, bounded) in stages.into_iter().flatten() {
                assert_eq!(streamed, bounded, "{plan}");
            }
        }

        // The stage of a barrier is planned in a different way.
        let barrier = BarrierExec::new(vec![empty_plan()], empty_plan());
        let barrier = Arc::new(barrier) as Arc<dyn ExecutionPlan>;
        assert!(reads_inputs_in_fixed_order(&barrier));
    }

    #[test]
    fn test_bounded_output_with_scalar_subquery() {
        let results = ScalarSubqueryResults::new(1);
        let predicate = Arc::new(ScalarSubqueryExpr::new(
            DataType::Boolean,
            false,
            SubqueryIndex::new(0),
            results.clone(),
        ));
        let filtered = Arc::new(FilterExec::try_new(predicate, shuffle(source())).unwrap());
        let plan = Arc::new(ScalarSubqueryExec::new(
            filtered,
            vec![ScalarSubqueryLink {
                plan: shuffle(source()),
                index: SubqueryIndex::new(0),
            }],
            results,
        ));
        // The stage waits for the tasks that read it, but not for the subquery to be read.
        let stages = bounded_inputs(plan);
        let mut inputs = stages.last().unwrap().clone();
        inputs.sort();
        assert_eq!(inputs, [(false, false), (true, true)]);
    }

    #[test]
    fn test_bounded_output_with_operators_that_may_read_inputs_in_any_order() {
        // The join decides by the data which input it reads next.
        let stages = bounded_inputs(sort_merge_join(shuffle(source()), shuffle(source())));
        assert_eq!(stages.last().unwrap(), &[(true, false), (true, false)]);

        // This applies to all inputs of the stage that has such an operator, whichever
        // operator reads them, but not to the stages that the stage does not read.
        let key = || col("id", &schema()).unwrap();
        let hash_join = HashJoinExec::try_new(
            shuffle(source()),
            shuffle(source()),
            vec![(key(), key())],
            None,
            &JoinType::LeftSemi,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        );
        let predicate = binary(key(), Operator::Gt, lit(0_i32), &schema()).unwrap();
        let other = Arc::new(FilterExec::try_new(predicate, shuffle(source())).unwrap());
        let other = shuffle(other);
        let stages = bounded_inputs(sort_merge_join(Arc::new(hash_join.unwrap()), other));
        assert_eq!(
            stages.last().unwrap(),
            &[(true, false), (true, false), (true, false)]
        );
        let others = &stages[..stages.len() - 1];
        assert_eq!(others.iter().flatten().collect::<Vec<_>>(), [&(true, true)]);
    }
}
