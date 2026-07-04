use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_datafusion_err, JoinType, Result};
use datafusion::logical_expr::execution_props::ScalarSubqueryResults;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    displayable, with_new_children_if_necessary, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};
use sail_catalog_system::physical_plan::SystemTableExec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_data_source::listing::delete::FileDeleteExec;
use sail_delta_lake::physical_plan::DeltaCommitExec;
use sail_iceberg::physical_plan::IcebergCommitExec;
use sail_physical_plan::barrier::BarrierExec;
use sail_physical_plan::catalog_command::CatalogCommandExec;
use sail_physical_plan::coalesce::CoalesceExec;
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
        let last = build_job_graph(plan, PartitionUsage::Once, &mut graph)?;
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

/// Recursively splits an execution plan into stages at shuffle boundaries and adds them to the job graph.
fn build_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    plan_job_graph_stages(plan, usage, graph, DriverStageHandling::CreateStage, None)
}

fn plan_job_graph_stages(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    driver_stage_handling: DriverStageHandling,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    if let Some(scalar) = plan.downcast_ref::<ScalarSubqueryExec>() {
        return build_scalar_subquery_job_graph(scalar, usage, graph, driver_stage_handling);
    }

    if let Some(barrier) = plan.downcast_ref::<BarrierExec>() {
        return build_barrier_job_graph(barrier, usage, graph, scalar_subqueries);
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
                        scalar_subqueries,
                    )?,
                    plan_job_graph_stages(
                        right.clone(),
                        usage,
                        graph,
                        DriverStageHandling::CreateStage,
                        scalar_subqueries,
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
                        scalar_subqueries,
                    )?,
                    plan_job_graph_stages(
                        right.clone(),
                        usage,
                        graph,
                        DriverStageHandling::CreateStage,
                        scalar_subqueries,
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
                scalar_subqueries,
            )?,
            plan_job_graph_stages(
                right.clone(),
                usage,
                graph,
                DriverStageHandling::CreateStage,
                scalar_subqueries,
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
            scalar_subqueries,
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
            scalar_subqueries,
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
                    scalar_subqueries,
                )
            })
            .collect::<ExecutionResult<Vec<_>>>()?
    };
    let plan = with_new_children_if_necessary(plan, children)?;

    let consumption = match usage {
        PartitionUsage::Once => ShuffleConsumption::Single,
        PartitionUsage::Shared => ShuffleConsumption::Multiple,
    };
    let plan = if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        if repartition.preserve_order() {
            // We haven't found a case when order-preserving repartition can be constructed,
            // so it's fine to return an error for now.
            // TODO: support order-preserving repartition
            return Err(ExecutionError::InternalError(
                "repartition is order-preserving and would result in incorrect results in distributed execution".to_string()
            ));
        }
        let properties = repartition.properties().clone();
        let child = plan.children().one()?;
        match &properties.partitioning {
            Partitioning::UnknownPartitioning(n) => {
                let n = *n;
                let properties = Arc::new(
                    properties
                        .as_ref()
                        .clone()
                        .with_partitioning(Partitioning::RoundRobinBatch(n)),
                );
                create_shuffle(child, graph, properties, consumption, scalar_subqueries)?
            }
            Partitioning::RoundRobinBatch(_) | Partitioning::Hash(_, _) => {
                create_shuffle(child, graph, properties, consumption, scalar_subqueries)?
            }
        }
    } else if let Some(repartition) = plan.downcast_ref::<ExplicitRepartitionExec>() {
        let properties = repartition.properties().clone();
        let child = plan.children().one()?;
        match &properties.partitioning {
            Partitioning::RoundRobinBatch(channels) => create_row_shuffle(
                child,
                *channels,
                graph,
                properties,
                consumption,
                scalar_subqueries,
            )?,
            other => {
                return Err(ExecutionError::DataFusionError(plan_datafusion_err!(
                    "unexpected explicit repartition partitioning in distributed planning: {other:?}"
                )));
            }
        }
    } else if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        let properties = coalesce.properties().clone();
        let child = plan.children().one()?;
        let fetch = coalesce.fetch();
        let shuffled = create_shuffle(child, graph, properties, consumption, scalar_subqueries)?;
        if let Some(f) = fetch {
            Arc::new(GlobalLimitExec::new(shuffled, 0, Some(f))) as Arc<dyn ExecutionPlan>
        } else {
            shuffled
        }
    } else if plan.is::<SortPreservingMergeExec>() {
        let child = plan.children().one()?;
        plan.clone().with_new_children(vec![create_merge_input(
            child,
            graph,
            scalar_subqueries,
        )?])?
    } else if let Some(coalesce) = plan.downcast_ref::<CoalesceExec>() {
        let child = plan.children().one()?;
        create_rescale_input(
            child,
            coalesce.output_partitions(),
            graph,
            scalar_subqueries,
        )?
    } else if plan.is::<SystemTableExec>()
        || plan.is::<CatalogCommandExec>()
        || plan.is::<FileDeleteExec>()
        || plan.is::<DeltaCommitExec>()
        || plan.is::<IcebergCommitExec>()
    {
        if matches!(driver_stage_handling, DriverStageHandling::PreserveRoot) {
            plan
        } else {
            create_driver_stage(&plan, graph, scalar_subqueries)?
        }
    } else {
        plan
    };
    Ok(plan)
}

fn build_scalar_subquery_job_graph(
    scalar: &ScalarSubqueryExec,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    driver_stage_handling: DriverStageHandling,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
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
            let plan = create_scalar_subquery_input(&plan, graph)?;
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
        Some(subqueries.as_slice()),
    )?;
    Ok(wrap_scalar_subquery_stage_if_needed(
        input,
        Some(subqueries.as_slice()),
    ))
}

fn build_barrier_job_graph(
    barrier: &BarrierExec,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let preconditions = barrier
        .preconditions()
        .iter()
        .map(|precondition| {
            plan_job_graph_stages(
                precondition.clone(),
                usage,
                graph,
                DriverStageHandling::CreateStage,
                scalar_subqueries,
            )
        })
        .collect::<ExecutionResult<Vec<_>>>()?;
    let plan_is_driver_stage = is_driver_stage_plan(barrier.plan());
    let plan = if plan_is_driver_stage {
        plan_job_graph_stages(
            barrier.plan().clone(),
            usage,
            graph,
            DriverStageHandling::PreserveRoot,
            scalar_subqueries,
        )?
    } else {
        plan_job_graph_stages(
            barrier.plan().clone(),
            usage,
            graph,
            DriverStageHandling::CreateStage,
            scalar_subqueries,
        )?
    };
    let barrier = Arc::new(BarrierExec::new(preconditions, plan)) as Arc<dyn ExecutionPlan>;
    if plan_is_driver_stage {
        create_driver_stage(&barrier, graph, scalar_subqueries)
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
}

fn wrap_scalar_subquery_stage_if_needed(
    plan: Arc<dyn ExecutionPlan>,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> Arc<dyn ExecutionPlan> {
    let Some(scalar_subqueries) = scalar_subqueries else {
        return plan;
    };
    if scalar_subqueries.is_empty() || !plan_contains_scalar_subquery_expr(&plan) {
        return plan;
    }
    Arc::new(ScalarSubqueryExec::new(
        plan,
        scalar_subqueries.to_vec(),
        ScalarSubqueryResults::new(scalar_subqueries.len()),
    ))
}

fn plan_contains_scalar_subquery_expr(plan: &Arc<dyn ExecutionPlan>) -> bool {
    // DataFusion does not expose a generic ExecutionPlan expression visitor.
    // The display tree is the uniform view that includes expressions for both
    // DataFusion built-in nodes and Sail extension nodes.
    displayable(plan.as_ref())
        .indent(false)
        .to_string()
        .contains("scalar_subquery(")
}

fn create_merge_input(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let properties = plan.properties().clone();
    let plan = wrap_scalar_subquery_stage_if_needed(plan.clone(), scalar_subqueries);
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution: OutputDistribution::RoundRobin { channels: 1 },
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    Ok(Arc::new(StageInputExec::new(
        StageInput {
            stage: s,
            mode: InputMode::Merge,
        },
        properties,
    )))
}

fn create_scalar_subquery_input(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution: OutputDistribution::RoundRobin { channels: 1 },
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    // ScalarSubqueryExec reads the link as a scalar value on every output
    // partition, so the materialized stage is exposed as one broadcast input.
    let properties = Arc::new(PlanProperties::new(
        datafusion::physical_expr::EquivalenceProperties::new(graph.stages[s].plan.schema()),
        Partitioning::UnknownPartitioning(1),
        graph.stages[s].plan.pipeline_behavior(),
        graph.stages[s].plan.boundedness(),
    ));
    Ok(Arc::new(StageInputExec::new(
        StageInput {
            stage: s,
            mode: InputMode::Broadcast,
        },
        properties,
    )))
}

fn create_rescale_input(
    plan: &Arc<dyn ExecutionPlan>,
    output_partitions: usize,
    graph: &mut JobGraph,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let plan = wrap_scalar_subquery_stage_if_needed(plan.clone(), scalar_subqueries);
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution: OutputDistribution::RoundRobin { channels: 1 },
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    let properties = Arc::new(PlanProperties::new(
        datafusion::physical_expr::EquivalenceProperties::new(graph.stages[s].plan.schema()),
        Partitioning::UnknownPartitioning(output_partitions),
        graph.stages[s].plan.pipeline_behavior(),
        graph.stages[s].plan.boundedness(),
    ));
    Ok(Arc::new(StageInputExec::new(
        StageInput {
            stage: s,
            mode: InputMode::Rescale,
        },
        properties,
    )))
}

fn create_shuffle(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    // These are the properties after repartition/coalesce,
    // which are different from the properties of the input plan.
    properties: Arc<PlanProperties>,
    consumption: ShuffleConsumption,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let distribution = match properties.partitioning.clone() {
        Partitioning::RoundRobinBatch(channels) | Partitioning::UnknownPartitioning(channels) => {
            OutputDistribution::RoundRobin { channels }
        }
        Partitioning::Hash(keys, channels) => OutputDistribution::Hash { keys, channels },
    };
    let plan = wrap_scalar_subquery_stage_if_needed(plan.clone(), scalar_subqueries);
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution,
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    Ok(Arc::new(StageInputExec::new(
        StageInput { stage: s, mode },
        properties,
    )))
}

/// Creates a shuffle stage with row-level round-robin distribution.
/// Used for explicit user-requested repartition calls where individual rows
/// must be distributed evenly across output partitions.
fn create_row_shuffle(
    plan: &Arc<dyn ExecutionPlan>,
    channels: usize,
    graph: &mut JobGraph,
    properties: Arc<PlanProperties>,
    consumption: ShuffleConsumption,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let distribution = OutputDistribution::RoundRobinRow { channels };
    let plan = wrap_scalar_subquery_stage_if_needed(plan.clone(), scalar_subqueries);
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution,
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    Ok(Arc::new(StageInputExec::new(
        StageInput { stage: s, mode },
        properties,
    )))
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

fn create_driver_stage(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    scalar_subqueries: Option<&[ScalarSubqueryLink]>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let plan = wrap_scalar_subquery_stage_if_needed(plan.clone(), scalar_subqueries);
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let properties = plan.properties().clone();
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution: OutputDistribution::RoundRobin { channels: 1 },
        placement: TaskPlacement::Driver,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    Ok(Arc::new(StageInputExec::new(
        StageInput {
            stage: s,
            mode: InputMode::Forward,
        },
        properties,
    )))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::logical_expr::execution_props::{ScalarSubqueryResults, SubqueryIndex};
    use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use sail_catalog::command::CatalogCommand;
    use sail_physical_plan::barrier::BarrierExec;
    use sail_physical_plan::catalog_command::CatalogCommandExec;
    use sail_physical_plan::coalesce::CoalesceExec;
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
        assert!(stage.input().is::<FilterExec>());
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
        assert!(stage
            .inputs
            .iter()
            .any(|input| matches!(input.mode, InputMode::Broadcast)));
    }
}
