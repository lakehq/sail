use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinType, Result, internal_err, plan_datafusion_err};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
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
use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use sail_physical_plan::shared_cte::{SharedCte, SharedCteExec, SharedCteScanExec};

use crate::driver::job_scheduler::topology::JobTopology;
use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{
    InputMode, JobGraph, JobGraphOptions, OutputDistribution, OutputMode, SharedReader, Stage,
    StageInput, TaskPlacement,
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
        let plan = materialize_shared_file_scan_builds(plan)?;
        let mut plan = ensure_worker_input_for_lake_procedure(plan)?;
        let mut materialized = HashSet::new();
        // Whether each shared CTE can be evaluated by its late references is decided on the
        // plan as planned, since a CTE whose body reads another CTE that is evaluated in
        // place of its references could double its plan at each level of the CTEs.
        let ctes = shared_ctes(&plan)?;
        loop {
            let mut graph = Self {
                stages: vec![],
                schema: plan.schema(),
                options: options.clone(),
                shared_readers: vec![],
                pending_shared_readers: HashMap::new(),
                materialized_ctes: materialized.clone(),
            };
            let last = build_job_graph(Arc::clone(&plan), PartitionUsage::Once, &mut graph)?.plan;
            push_stage(
                last,
                &mut graph,
                OutputDistribution::RoundRobinBatch { channels: 1 },
                TaskPlacement::Worker,
                OutputMode::Pipelined,
            )?;
            // A reference whose stage runs only after another stage that runs with the
            // stage of the shared CTE has finished cannot read the CTE in that region.
            let late = late_shared_references(&graph);
            // A region that needs more worker task slots than the cluster can provide cannot
            // run. The shared CTEs in it are evaluated by their references or materialized,
            // as for late references, so that the region splits as it would without them.
            let late = if late.is_empty() {
                oversized_shared_references(&graph, &ctes)?
            } else {
                late
            };
            if late.is_empty() {
                return Ok(graph);
            }
            // A late reference to a deterministic CTE that reads no other shared CTE
            // evaluates the CTE itself. Any other CTE with late references is materialized,
            // since its own evaluation would make the plan grow exponentially with the depth
            // of the CTEs, or evaluate a volatile CTE more than once.
            let (inline, other) = late
                .into_iter()
                .partition::<HashSet<_>, _>(|(cte, _)| ctes.get(cte) == Some(&true));
            materialized.extend(other.into_iter().map(|(cte, _)| cte));
            if !inline.is_empty() {
                plan = plan
                    .transform_up(|plan| match plan.downcast_ref::<SharedCteExec>() {
                        Some(shared) => Ok(Transformed::yes(shared.inline_references(&inline)?)),
                        None => Ok(Transformed::no(plan)),
                    })
                    .data()?;
            }
        }
    }
}

/// Returns the shared CTEs in the plan, with whether each CTE can be evaluated by each of
/// its references: whether it is deterministic and reads no other shared CTE.
fn shared_ctes(plan: &Arc<dyn ExecutionPlan>) -> ExecutionResult<HashMap<usize, bool>> {
    let mut ctes = HashMap::new();
    plan.apply(|plan| {
        if let Some(shared) = plan.downcast_ref::<SharedCteExec>() {
            for (cte, body) in shared.ctes() {
                let leaf = !body.exists(|plan| Ok(plan.is::<SharedCteScanExec>()))?;
                ctes.insert(cte.id(), !cte.volatile() && leaf);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(ctes)
}

/// Returns the references to shared CTEs whose stages are reachable, through an edge from
/// a stage whose output is materialized, from another stage of the region that the stage
/// of the shared CTE would join, as pairs of the identifier of the CTE and the number of
/// the reference.
fn late_shared_references(graph: &JobGraph) -> Vec<(usize, usize)> {
    let stages = graph.stages();
    if graph.shared_readers.is_empty() {
        return vec![];
    }
    let n = stages.len();
    // The stages that run together, as in the topology of the job.
    let mut parent = (0..n).collect::<Vec<_>>();
    fn find(parent: &mut [usize], x: usize) -> usize {
        let mut x = x;
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }
    for (s, stage) in stages.iter().enumerate() {
        for input in &stage.inputs {
            if matches!(
                stages[input.stage].mode,
                OutputMode::Pipelined | OutputMode::Shared
            ) {
                let (a, b) = (find(&mut parent, s), find(&mut parent, input.stage));
                parent[a] = b;
            }
        }
    }
    // For each stage, its ancestors, and the ancestors from which it is reached through
    // an edge from a materialized output. The stages are in topological order.
    let mut ancestors: Vec<HashSet<usize>> = Vec::with_capacity(n);
    let mut blocked: Vec<HashSet<usize>> = Vec::with_capacity(n);
    for stage in stages {
        let mut a = HashSet::new();
        let mut b = HashSet::new();
        for input in &stage.inputs {
            let p = input.stage;
            a.insert(p);
            a.extend(ancestors[p].iter().copied());
            b.extend(blocked[p].iter().copied());
            if matches!(stages[p].mode, OutputMode::Blocking) {
                b.insert(p);
                b.extend(ancestors[p].iter().copied());
            }
        }
        ancestors.push(a);
        blocked.push(b);
    }
    graph
        .shared_readers
        .iter()
        .filter(|reader| {
            let component = find(&mut parent, reader.stage);
            blocked[reader.stage]
                .iter()
                .any(|&x| find(&mut parent, x) == component)
        })
        .map(|reader| (reader.cte, reader.reference))
        .collect()
}

/// Returns the references to the shared CTEs whose stages run in a region that needs more
/// worker task slots than the cluster can provide, as pairs of the identifier of the CTE
/// and the number of the reference. Without a shuffle backend that materializes outputs,
/// only the CTEs that can be evaluated by each of their references are returned.
fn oversized_shared_references(
    graph: &JobGraph,
    ctes: &HashMap<usize, bool>,
) -> ExecutionResult<Vec<(usize, usize)>> {
    let Some(capacity) = graph.options.worker_slots else {
        return Ok(vec![]);
    };
    if graph.shared_readers.is_empty() {
        return Ok(vec![]);
    }
    let topology = JobTopology::try_new(graph)?;
    let mut oversized = HashSet::new();
    for (r, region) in topology.regions.iter().enumerate() {
        let stages = region.tasks.iter().map(|t| t.stage).collect::<HashSet<_>>();
        let shared = stages
            .iter()
            .any(|&s| matches!(graph.stages()[s].mode, OutputMode::Shared));
        if shared && topology.worker_slots(graph, r) > capacity {
            oversized.extend(
                graph
                    .shared_readers
                    .iter()
                    .filter(|x| stages.contains(&x.stage))
                    .map(|x| x.cte),
            );
        }
    }
    let materializes = !matches!(
        graph.options.shuffle_backend,
        ShuffleBackendKind::Flight { .. }
    );
    Ok(graph
        .shared_readers
        .iter()
        .filter(|x| oversized.contains(&x.cte) && (materializes || ctes.get(&x.cte) == Some(&true)))
        .map(|x| (x.cte, x.reference))
        .collect())
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

/// A single-partition build no longer needs a physical coalesce, but should
/// still be scanned once and broadcast rather than read again by every task.
fn materialize_shared_file_scan_builds(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|plan| {
            let shared_build = plan
                .downcast_ref::<HashJoinExec>()
                .is_some_and(|join| join.mode == PartitionMode::CollectLeft)
                || plan.is::<CrossJoinExec>()
                || plan.is::<NestedLoopJoinExec>()
                || plan.is::<PiecewiseMergeJoinExec>();
            if !shared_build {
                return Ok(Transformed::no(plan));
            }
            let (left, right) = plan.children().two()?;
            if left.is::<CoalescePartitionsExec>()
                || left.output_partitioning().partition_count() != 1
                || right.output_partitioning().partition_count() <= 1
                || !left.exists(|node| {
                    Ok(node.downcast_ref::<DataSourceExec>().is_some_and(|source| {
                        source
                            .data_source()
                            .downcast_ref::<FileScanConfig>()
                            .is_some()
                    }))
                })?
            {
                return Ok(Transformed::no(plan));
            }
            let children = vec![
                Arc::new(CoalescePartitionsExec::new(Arc::clone(left))) as Arc<dyn ExecutionPlan>,
                Arc::clone(right),
            ];
            Ok(Transformed::yes(replace_children_if_necessary(
                plan, children,
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

    if let Some(shared) = plan.downcast_ref::<SharedCteExec>() {
        return build_shared_cte_job_graph(shared, usage, graph, driver_stage_handling);
    }

    if let Some(reference) = plan.downcast_ref::<StageInputExec<SharedCteStage>>() {
        return create_shared_cte_input(reference, usage, graph);
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
        let ordering = repartition
            .preserve_order()
            .then(|| repartition.properties().output_ordering().cloned())
            .flatten();
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
        match (fetch, consumption) {
            (Some(f), ShuffleConsumption::Multiple) => {
                // Every task of the stage reads the partition, so the limit is applied
                // once in a stage of its own, and its output is read by every task, rather
                // than by each task, which would keep different rows.
                let shuffled = create_shuffle(
                    child,
                    graph,
                    properties.clone(),
                    ShuffleConsumption::Single,
                    scalar_context,
                )?;
                let limit = Arc::new(GlobalLimitExec::new(shuffled, 0, Some(f)));
                let limit = PlannedSubtree::without_pending_scalar_subquery_expr(limit);
                PlannedSubtree::without_pending_scalar_subquery_expr(create_shuffle(
                    limit,
                    graph,
                    properties,
                    ShuffleConsumption::Multiple,
                    None,
                )?)
            }
            (Some(f), ShuffleConsumption::Single) => {
                let shuffled =
                    create_shuffle(child, graph, properties, consumption, scalar_context)?;
                PlannedSubtree::without_pending_scalar_subquery_expr(Arc::new(GlobalLimitExec::new(
                    shuffled,
                    0,
                    Some(f),
                ))
                    as Arc<dyn ExecutionPlan>)
            }
            (None, _) => {
                let shuffled =
                    create_shuffle(child, graph, properties, consumption, scalar_context)?;
                PlannedSubtree::without_pending_scalar_subquery_expr(shuffled)
            }
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
        let plan =
            create_rescale_input(child, coalesce.output_partitions(), graph, scalar_context)?;
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
    /// The identifier of the shared CTE and the number of the reference.
    reference: (usize, usize),
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
                    reference: (scan.cte().id(), scan.reference()),
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
    // A reference reads the partitions of the stage at its own pace, with any shuffle
    // backend. The stage runs with the stages that read it, produces its output when a
    // reference asks for rows that no reference has read yet, and returns its error only
    // to the references that read up to it.
    // A volatile CTE with references that cannot run with its stage is materialized by
    // the shuffle backend, as one channel per partition for the shuffle service, which
    // keeps the partitioning of the CTE.
    for (cte, plan) in shared.ctes() {
        let plan = plan_job_graph_stages(
            locate(plan, &stages)?,
            PartitionUsage::Once,
            graph,
            DriverStageHandling::CreateStage,
            None,
        )?;
        let (mode, distribution) = if !graph.materialized_ctes.contains(&cte.id()) {
            let distribution = OutputDistribution::RoundRobinBatch { channels: 1 };
            (OutputMode::Shared, distribution)
        } else if let ShuffleBackendKind::Celeborn { .. } = graph.options.shuffle_backend {
            let partitioning = plan.plan.output_partitioning();
            let channels = partitioning.partition_count();
            let distribution = match partitioning {
                Partitioning::Hash(keys, _) => OutputDistribution::Hash {
                    keys: keys.clone(),
                    channels,
                },
                _ => OutputDistribution::RoundRobinBatch { channels },
            };
            (OutputMode::Blocking, distribution)
        } else {
            let distribution = OutputDistribution::RoundRobinBatch { channels: 1 };
            (OutputMode::Blocking, distribution)
        };
        let stage = push_stage(plan.plan, graph, distribution, TaskPlacement::Worker, mode)?;
        stages.insert(Arc::as_ptr(cte), stage);
    }
    plan_job_graph_stages(
        locate(shared.input(), &stages)?,
        usage,
        graph,
        driver_stage_handling,
        None,
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
) -> ExecutionResult<PlannedSubtree> {
    let SharedCteStage {
        stage,
        reference: (cte, number),
        projection,
    } = reference.input();
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
    let materialized = graph.materialized_ctes.contains(cte);
    let mode = match usage {
        // The only partition of the reference is read by every task of its stage.
        PartitionUsage::Shared if preserved && partitions == 1 => InputMode::Broadcast,
        // Each partition of the shared CTE is a channel of the shuffle service.
        _ if materialized
            && matches!(
                graph.options.shuffle_backend,
                ShuffleBackendKind::Celeborn { .. }
            ) =>
        {
            InputMode::Shuffle
        }
        _ => InputMode::Rescale { partitions },
    };
    let mut properties = stage_properties_with_unknown_partitioning(graph, *stage, partitions);
    if preserved && let Some(expected) = expected {
        properties = Arc::new(properties.as_ref().clone().with_partitioning(expected));
    }
    let input = stage_input_exec(*stage, mode, properties);
    if !materialized {
        // The stage that reads the input is found when it is created.
        graph
            .pending_shared_readers
            .insert(Arc::as_ptr(&input) as *const () as usize, (*cte, *number));
    }
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
        None,
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
    let index = graph.stages.len();
    if !graph.pending_shared_readers.is_empty() {
        plan.apply(|node| {
            let address = Arc::as_ptr(node) as *const () as usize;
            if let Some((cte, reference)) = graph.pending_shared_readers.remove(&address) {
                graph.shared_readers.push(SharedReader {
                    cte,
                    reference,
                    stage: index,
                });
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    let (plan, inputs) = rewrite_inputs(plan)?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode,
        distribution,
        placement,
    };
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

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::{JoinType, NullEquality, ScalarValue};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{binary, col};
    use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion::physical_expr::{
        LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
    };
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
    use sail_catalog::command::CatalogCommand;
    use sail_celeborn::common::PartitionSplitMode;
    use sail_physical_plan::barrier::BarrierExec;
    use sail_physical_plan::catalog_command::CatalogCommandExec;
    use sail_physical_plan::coalesce::CoalesceExec;
    use sail_physical_plan::remote_checkpoint::RemoteCheckpointCommitExec;
    use sail_physical_plan::repartition::ExplicitRepartitionExec;

    use super::{JobGraph, JobGraphOptions, create_scalar_subquery_input};
    use crate::driver::job_scheduler::topology::JobTopology;
    use crate::job_graph::{InputMode, OutputDistribution, OutputMode, StageInput, TaskPlacement};
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
            worker_slots: None,
        }
    }

    fn blocking_shuffle_options() -> JobGraphOptions {
        JobGraphOptions {
            shuffle_backend: ShuffleBackendKind::Storage {
                path: None,
                max_file_size: 1,
                compression: ShuffleCompression::None,
            },
            worker_slots: None,
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
            worker_slots: None,
        }
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

    /// A shared CTE of two partitions, and a function that returns its references.
    fn shared_cte(
        id: usize,
        volatile: bool,
        body: Arc<dyn ExecutionPlan>,
    ) -> (
        Arc<sail_physical_plan::shared_cte::SharedCte>,
        impl Fn() -> Arc<dyn ExecutionPlan>,
    ) {
        use datafusion::common::Statistics;
        use sail_physical_plan::shared_cte::{SharedCte, SharedCteScanExec};

        let cte = Arc::new(
            SharedCte::new(id, format!("c{id}"), body.output_partitioning().clone())
                .with_volatile(volatile),
        );
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

    fn collect_left_join(
        build: Arc<dyn ExecutionPlan>,
        probe: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
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
        )
    }

    /// Returns whether the job has a stage of a shared CTE that runs with its readers, and
    /// the shared CTEs that are materialized.
    fn shared_stages(graph: &JobGraph) -> (bool, Vec<usize>) {
        let shared = graph
            .stages
            .iter()
            .any(|stage| matches!(stage.mode, OutputMode::Shared));
        let mut materialized = graph.materialized_ctes.iter().copied().collect::<Vec<_>>();
        materialized.sort_unstable();
        (shared, materialized)
    }

    #[test]
    fn shared_cte_references_behind_materialized_outputs_evaluate_the_cte() {
        use sail_physical_plan::shared_cte::share_ctes;

        for (options, shared) in [
            (flight_shuffle_options(), true),
            (blocking_shuffle_options(), false),
            (celeborn_shuffle_options(), false),
        ] {
            let body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
            let (cte, scan) = shared_cte(0, false, body.clone());
            // The probe side runs after the build side, which is materialized by a blocking
            // shuffle backend, so it cannot read the CTE with the build side.
            let plan = share_ctes(collect_left_join(scan(), scan()), vec![(cte, body)]).unwrap();
            let graph = JobGraph::try_new(plan, options).unwrap();
            assert_eq!(shared_stages(&graph), (shared, vec![]));
        }
    }

    #[test]
    fn shared_cte_references_that_run_together_read_the_cte() {
        use sail_physical_plan::shared_cte::share_ctes;

        for options in [blocking_shuffle_options(), celeborn_shuffle_options()] {
            let body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
            let (cte, scan) = shared_cte(0, false, body.clone());
            let plan = share_ctes(
                UnionExec::try_new(vec![scan(), scan()]).unwrap(),
                vec![(cte, body)],
            )
            .unwrap();
            let graph = JobGraph::try_new(plan, options).unwrap();
            assert_eq!(shared_stages(&graph), (true, vec![]));
        }
    }

    #[test]
    fn volatile_shared_cte_with_late_references_is_materialized() {
        use sail_physical_plan::shared_cte::share_ctes;

        for options in [blocking_shuffle_options(), celeborn_shuffle_options()] {
            let celeborn = matches!(options.shuffle_backend, ShuffleBackendKind::Celeborn { .. });
            let body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
            let (cte, scan) = shared_cte(0, true, body.clone());
            let plan = share_ctes(collect_left_join(scan(), scan()), vec![(cte, body)]).unwrap();
            let graph = JobGraph::try_new(plan, options).unwrap();
            assert_eq!(shared_stages(&graph), (false, vec![0]));
            let stage = graph
                .stages
                .iter()
                .position(|x| matches!(x.mode, OutputMode::Blocking) && x.inputs.is_empty())
                .unwrap();
            // The shuffle service serves each partition of the CTE as a channel.
            assert_eq!(
                graph.stages[stage].distribution.channels(),
                if celeborn { 2 } else { 1 }
            );
            let modes = graph
                .stages
                .iter()
                .flat_map(|x| x.inputs.iter().filter(|i| i.stage == stage).map(|i| i.mode))
                .collect::<Vec<_>>();
            assert_eq!(modes.len(), 2);
            assert!(modes.iter().all(|x| if celeborn {
                matches!(x, InputMode::Shuffle)
            } else {
                matches!(x, InputMode::Rescale { .. })
            }));
        }
    }

    /// A plan whose two references to a shared CTE of ten partitions are read by a union
    /// of twenty partitions, which runs in one region with the stage of the CTE.
    fn union_of_shared_cte(volatile: bool) -> Arc<dyn ExecutionPlan> {
        use sail_physical_plan::shared_cte::share_ctes;

        let body = UnionExec::try_new(vec![empty_plan(); 10]).unwrap();
        let (cte, scan) = shared_cte(0, volatile, body.clone());
        share_ctes(
            UnionExec::try_new(vec![scan(), scan()]).unwrap(),
            vec![(cte, body)],
        )
        .unwrap()
    }

    fn with_worker_slots(options: JobGraphOptions, slots: Option<usize>) -> JobGraphOptions {
        JobGraphOptions {
            worker_slots: slots,
            ..options
        }
    }

    /// Returns the largest number of worker task slots that a region of the job needs.
    fn max_worker_slots(graph: &JobGraph) -> usize {
        let topology = JobTopology::try_new(graph).unwrap();
        (0..topology.regions.len())
            .map(|r| topology.worker_slots(graph, r))
            .max()
            .unwrap_or(0)
    }

    #[test]
    fn shared_cte_in_a_region_larger_than_the_cluster_is_evaluated_by_its_references() {
        let options = with_worker_slots(celeborn_shuffle_options(), Some(10));
        let graph = JobGraph::try_new(union_of_shared_cte(false), options).unwrap();
        assert_eq!(shared_stages(&graph), (false, vec![]));
        assert!(max_worker_slots(&graph) <= 10);
    }

    #[test]
    fn shared_cte_runs_with_its_readers_when_the_cluster_size_is_not_limited() {
        let options = with_worker_slots(celeborn_shuffle_options(), None);
        let graph = JobGraph::try_new(union_of_shared_cte(false), options).unwrap();
        assert_eq!(shared_stages(&graph), (true, vec![]));
        assert_eq!(max_worker_slots(&graph), 20);
    }

    #[test]
    fn volatile_shared_cte_in_a_region_larger_than_the_cluster_is_materialized() {
        let options = with_worker_slots(celeborn_shuffle_options(), Some(10));
        let graph = JobGraph::try_new(union_of_shared_cte(true), options).unwrap();
        assert_eq!(shared_stages(&graph), (false, vec![0]));
        let stage = graph
            .stages
            .iter()
            .position(|x| matches!(x.mode, OutputMode::Blocking) && x.inputs.is_empty())
            .unwrap();
        assert_eq!(graph.stages[stage].distribution.channels(), 10);
        assert!(max_worker_slots(&graph) <= 10);
    }

    #[test]
    fn shared_cte_in_a_region_larger_than_the_cluster_is_evaluated_by_its_references_on_flight() {
        let options = with_worker_slots(flight_shuffle_options(), Some(10));
        let graph = JobGraph::try_new(union_of_shared_cte(false), options).unwrap();
        assert_eq!(shared_stages(&graph), (false, vec![]));
    }

    #[test]
    fn volatile_shared_cte_in_a_region_larger_than_the_cluster_runs_with_its_readers_on_flight() {
        // Flight keeps mandatory sharing. The existing scheduler reports the region's
        // twenty-slot requirement when only ten slots are configured.
        let options = with_worker_slots(flight_shuffle_options(), Some(10));
        let graph = JobGraph::try_new(union_of_shared_cte(true), options).unwrap();
        assert_eq!(shared_stages(&graph), (true, vec![]));
        assert_eq!(max_worker_slots(&graph), 20);
        assert!(
            graph
                .stages
                .iter()
                .all(|x| !matches!(x.mode, OutputMode::Blocking))
        );
    }

    #[test]
    fn late_references_are_found_across_shared_ctes() {
        use sail_physical_plan::shared_cte::share_ctes;

        for options in [blocking_shuffle_options(), celeborn_shuffle_options()] {
            let body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
            let (inner, inner_scan) = shared_cte(0, false, body.clone());
            // The outer CTE reads the inner one, and the regions of both join through the
            // reference to the inner CTE that runs after the build side.
            let outer_body = inner_scan();
            let (outer, outer_scan) = shared_cte(1, false, Arc::clone(&outer_body));
            let plan = share_ctes(
                collect_left_join(outer_scan(), inner_scan()),
                vec![(inner, body), (outer, outer_body)],
            )
            .unwrap();
            let graph = JobGraph::try_new(plan, options).unwrap();
            assert_eq!(shared_stages(&graph), (false, vec![]));
        }
    }

    #[test]
    fn nested_shared_ctes_with_late_references_stay_linear() {
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
        let depth = 12;
        for options in [
            flight_shuffle_options(),
            blocking_shuffle_options(),
            celeborn_shuffle_options(),
        ] {
            // Each CTE joins two references to the CTE before it.
            let mut body = UnionExec::try_new(vec![empty_plan(); 2]).unwrap();
            let mut ctes = vec![];
            for id in 0..depth {
                let (cte, scan) = shared_cte(id, false, Arc::clone(&body));
                ctes.push((cte, body));
                body = join(scan(), scan());
            }
            let plan = share_ctes(body, ctes).unwrap();
            let graph = JobGraph::try_new(plan, options).unwrap();
            assert!(
                graph.stages.len() < 20 * depth,
                "{} stages",
                graph.stages.len()
            );
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
                shared_readers: vec![],
                pending_shared_readers: Default::default(),
                materialized_ctes: Default::default(),
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
        let graph = JobGraph::try_new(
            Arc::new(CoalesceExec::new(input, 2)),
            flight_shuffle_options(),
        )
        .unwrap();

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
                mode: InputMode::Rescale { partitions: 2 },
            }]
        ));
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
    #[test]
    fn nested_volatile_shared_ctes_keep_one_evaluation_and_their_capacity_requirement() {
        use sail_physical_plan::shared_cte::share_ctes;
        for outer_volatile in [false, true] {
            for slots in [None, Some(10), Some(40)] {
                let body = UnionExec::try_new(vec![empty_plan(); 10]).unwrap();
                let (inner, inner_scan) = shared_cte(0, true, body.clone());
                let outer_body = UnionExec::try_new(vec![inner_scan(), inner_scan()]).unwrap();
                let (outer, outer_scan) = shared_cte(1, outer_volatile, outer_body.clone());
                let plan = share_ctes(
                    UnionExec::try_new(vec![outer_scan(), outer_scan()]).unwrap(),
                    vec![(inner, body), (outer, outer_body)],
                )
                .unwrap();
                let graph =
                    JobGraph::try_new(plan, with_worker_slots(flight_shuffle_options(), slots))
                        .unwrap();
                assert_eq!(shared_stages(&graph), (true, vec![]));
                assert_eq!(graph.shared_readers.len(), 4);
                assert_eq!(max_worker_slots(&graph), 40);
            }
        }
    }
}
