use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_datafusion_err, JoinSide, JoinType, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::utils::{build_join_schema, ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, JoinOn, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    with_new_children_if_necessary, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
};
use sail_catalog_system::physical_plan::SystemTableExec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_physical_plan::distributed_collect_left_join::DistributedCollectLeftJoinExec;

use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
};
use crate::plan::{
    AddRowIdExec, ApplyMatchSetExec, BuildMatchSetExec, MatchSetOrExec, ShuffleConsumption,
    StageInputExec,
};

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
        if let Some(gl) = node.as_any().downcast_ref::<GlobalLimitExec>() {
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
        let plan = if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
            Arc::clone(coalesce.input())
        } else if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
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
        let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
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
            join.projection.clone(),
            PartitionMode::Partitioned,
            join.null_equality,
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

fn build_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    if let Some(join) = plan
        .as_any()
        .downcast_ref::<DistributedCollectLeftJoinExec>()
    {
        return build_distributed_collect_left_join(join, usage, graph);
    }
    // Recursively build the job graph for the children first
    // and propagate partition usage information.
    let children = if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let (left, right) = join.children().two()?;
        match join.mode {
            PartitionMode::Partitioned => {
                vec![
                    build_job_graph(left.clone(), usage, graph)?,
                    build_job_graph(right.clone(), usage, graph)?,
                ]
            }
            PartitionMode::CollectLeft => {
                vec![
                    build_job_graph(left.clone(), PartitionUsage::Shared, graph)?,
                    build_job_graph(right.clone(), usage, graph)?,
                ]
            }
            PartitionMode::Auto => {
                return Err(ExecutionError::DataFusionError(plan_datafusion_err!(
                    "unresolved auto partition mode in hash join"
                )));
            }
        }
    } else if plan.as_any().is::<NestedLoopJoinExec>()
        || plan.as_any().is::<CrossJoinExec>()
        || plan.as_any().is::<PiecewiseMergeJoinExec>()
    {
        let (left, right) = plan.children().two()?;
        vec![
            build_job_graph(left.clone(), PartitionUsage::Shared, graph)?,
            build_job_graph(right.clone(), usage, graph)?,
        ]
    } else if plan.as_any().is::<RepartitionExec>() || plan.as_any().is::<CoalescePartitionsExec>()
    {
        let child = plan.children().one()?;
        // At the shuffle boundary, we only expect to use the child partition once
        // since the shuffle writer can materialize the data for multiple consumption.
        vec![build_job_graph(child.clone(), PartitionUsage::Once, graph)?]
    } else {
        plan.children()
            .into_iter()
            .map(|x| build_job_graph(x.clone(), usage, graph))
            .collect::<ExecutionResult<Vec<_>>>()?
    };
    // Avoid calling `with_new_children` on leaf nodes: some DataFusion test execs (e.g.
    // `physical_plan::test::TestMemoryExec`) intentionally leave it unimplemented.
    let plan = if children.is_empty() {
        plan
    } else {
        with_new_children_if_necessary(plan, children)?
    };

    let consumption = match usage {
        PartitionUsage::Once => ShuffleConsumption::Single,
        PartitionUsage::Shared => ShuffleConsumption::Multiple,
    };
    let plan = if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        let child = plan.children().one()?;
        match repartition.partitioning() {
            Partitioning::UnknownPartitioning(n) => {
                create_shuffle(child, graph, Partitioning::RoundRobinBatch(*n), consumption)?
            }
            x @ Partitioning::RoundRobinBatch(_) | x @ Partitioning::Hash(_, _) => {
                create_shuffle(child, graph, x.clone(), consumption)?
            }
        }
    } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        let child = plan.children().one()?;
        let partitioning = coalesce.properties().partitioning.clone();
        let fetch = coalesce.fetch();
        let shuffled = create_shuffle(child, graph, partitioning, consumption)?;
        if let Some(f) = fetch {
            Arc::new(GlobalLimitExec::new(shuffled, 0, Some(f))) as Arc<dyn ExecutionPlan>
        } else {
            shuffled
        }
    } else if plan.as_any().is::<SortPreservingMergeExec>() {
        let child = plan.children().one()?;
        plan.clone()
            .with_new_children(vec![create_merge_input(child, graph)?])?
    } else if plan.as_any().is::<SystemTableExec>() {
        plan.children().zero()?;
        create_driver_stage(&plan, graph)?
    } else {
        plan
    };
    Ok(plan)
}

fn build_distributed_collect_left_join(
    join: &DistributedCollectLeftJoinExec,
    usage: PartitionUsage,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    // The distributed collect-left join rewrite builds its own fan-out stage (`left_stage`)
    // for the left side. Upstream of that, the left subtree is planned as single-consumer.
    let left_base = build_job_graph(join.left().clone(), PartitionUsage::Once, graph)?;
    let right = build_job_graph(join.right().clone(), usage, graph)?;

    let optimized_collect_left = matches!(
        join.join_type(),
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
    );

    if optimized_collect_left {
        let left_with_row_id: Arc<dyn ExecutionPlan> =
            Arc::new(AddRowIdExec::try_new(left_base.clone())?);
        let left_row_id_index = left_with_row_id.schema().fields().len() - 1;

        let (left_stage_input, left_schema, left_partitioning) =
            create_forward_stage_input(&left_with_row_id, graph)?;
        let left_stage_for_apply: Arc<dyn ExecutionPlan> = Arc::new(StageInputExec::new(
            left_stage_input.clone(),
            left_schema.clone(),
            left_partitioning.clone(),
        ));
        let left_stage_for_probe: Arc<dyn ExecutionPlan> = Arc::new(StageInputExec::new(
            left_stage_input,
            left_schema,
            left_partitioning,
        ));

        match build_projected_join_inputs(join.on(), join.filter(), &left_stage_for_probe, &right) {
            Ok((left_keys, right_keys, join_on, join_filter, row_id_index)) => {
                let partition_count = right_keys.output_partitioning().partition_count();
                let (left_keys, right_keys) =
                    repartition_join_keys(left_keys, right_keys, join.on().len(), partition_count)?;
                let hash_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
                    left_keys,
                    right_keys,
                    join_on,
                    join_filter,
                    &JoinType::Inner,
                    None,
                    PartitionMode::Partitioned,
                    join.null_equality(),
                )?);
                let join_partitioning = hash_join.output_partitioning().clone();
                let join_stage = create_shuffle(
                    &hash_join,
                    graph,
                    join_partitioning,
                    ShuffleConsumption::Single,
                )?;
                let build_match_set: Arc<dyn ExecutionPlan> =
                    Arc::new(BuildMatchSetExec::new(join_stage.clone(), row_id_index));
                let match_set_partial = create_shuffle(
                    &build_match_set,
                    graph,
                    Partitioning::RoundRobinBatch(1),
                    ShuffleConsumption::Single,
                )?;
                let match_set_plan: Arc<dyn ExecutionPlan> =
                    Arc::new(MatchSetOrExec::new(match_set_partial));
                let match_set_stage = create_shuffle(
                    &match_set_plan,
                    graph,
                    Partitioning::RoundRobinBatch(1),
                    ShuffleConsumption::Multiple,
                )?;

                let (output_schema, _) = build_join_schema(
                    join.left().schema().as_ref(),
                    join.right().schema().as_ref(),
                    &join.join_type(),
                );
                let mut output_plan: Arc<dyn ExecutionPlan> = Arc::new(ApplyMatchSetExec::new(
                    left_stage_for_apply,
                    match_set_stage,
                    left_row_id_index,
                    join.join_type(),
                    Arc::new(output_schema),
                ));

                if let Some(projection) = join.projection() {
                    output_plan = apply_projection_indices(output_plan, projection)?;
                }

                return Ok(output_plan);
            }
            Err(err) => {
                log::warn!("optimized collect-left fallback: {err}");
            }
        }
    }

    let left_with_row_id: Arc<dyn ExecutionPlan> = Arc::new(AddRowIdExec::try_new(left_base)?);
    let row_id_index = left_with_row_id.schema().fields().len() - 1;
    let left_partitioning = left_with_row_id.output_partitioning().clone();
    let (left_stage_input, left_schema, _) = create_forward_stage_input(&left_with_row_id, graph)?;
    let left_stage_for_apply: Arc<dyn ExecutionPlan> = Arc::new(StageInputExec::new(
        left_stage_input.clone(),
        left_schema.clone(),
        left_partitioning.clone(),
    ));
    let left_stage_for_probe: Arc<dyn ExecutionPlan> = Arc::new(StageInputExec::new(
        StageInput {
            stage: left_stage_input.stage,
            mode: InputMode::Shuffle,
        },
        left_schema,
        Partitioning::RoundRobinBatch(1),
    ));
    // The probe stage primarily determines matches, but we intentionally keep FULL here so the
    // produced schema / nullability matches the eventual FULL join output (used by union/projection).
    let probe_join_type = match join.join_type() {
        JoinType::Full => JoinType::Full,
        _ => JoinType::Inner,
    };
    let hash_join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
        left_stage_for_probe,
        right,
        join.on().clone(),
        join.filter().cloned(),
        &probe_join_type,
        None,
        PartitionMode::CollectLeft,
        join.null_equality(),
    )?);
    let join_partitioning = hash_join.output_partitioning().clone();
    let join_stage = create_shuffle(
        &hash_join,
        graph,
        join_partitioning,
        ShuffleConsumption::Single,
    )?;

    let build_match_set: Arc<dyn ExecutionPlan> =
        Arc::new(BuildMatchSetExec::new(join_stage.clone(), row_id_index));
    let match_set_partial = create_shuffle(
        &build_match_set,
        graph,
        Partitioning::RoundRobinBatch(1),
        ShuffleConsumption::Single,
    )?;
    let match_set_plan: Arc<dyn ExecutionPlan> = Arc::new(MatchSetOrExec::new(match_set_partial));
    let match_set_stage = create_shuffle(
        &match_set_plan,
        graph,
        Partitioning::RoundRobinBatch(1),
        ShuffleConsumption::Multiple,
    )?;

    let (output_schema, _) = build_join_schema(
        join.left().schema().as_ref(),
        join.right().schema().as_ref(),
        &join.join_type(),
    );
    let apply_match_set: Arc<dyn ExecutionPlan> = Arc::new(ApplyMatchSetExec::new(
        left_stage_for_apply,
        match_set_stage,
        row_id_index,
        join.join_type(),
        Arc::new(output_schema),
    ));

    let mut output_plan: Arc<dyn ExecutionPlan> = match join.join_type() {
        JoinType::Left | JoinType::Full => {
            let projected_join = project_drop_row_id(join_stage, row_id_index)?;
            UnionExec::try_new(vec![projected_join, apply_match_set.clone()])?
        }
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => apply_match_set,
        _ => {
            return Err(ExecutionError::InternalError(format!(
                "unsupported distributed collect-left join type: {}",
                join.join_type()
            )))
        }
    };

    if let Some(projection) = join.projection() {
        output_plan = apply_projection_indices(output_plan, projection)?;
    }

    Ok(output_plan)
}

const JOIN_KEY_PREFIX: &str = "__sail_join_key_";
const JOIN_FILTER_LEFT_PREFIX: &str = "__sail_join_filter_left_";
const JOIN_FILTER_RIGHT_PREFIX: &str = "__sail_join_filter_right_";

type ProjectedJoinInputs = (
    Arc<dyn ExecutionPlan>,
    Arc<dyn ExecutionPlan>,
    JoinOn,
    Option<JoinFilter>,
    usize,
);

fn build_projected_join_inputs(
    on: &JoinOn,
    filter: Option<&JoinFilter>,
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
) -> ExecutionResult<ProjectedJoinInputs> {
    let left_schema = left.schema();
    let right_schema = right.schema();
    let mut left_exprs = Vec::with_capacity(on.len() + 1);
    for (index, (left_expr, _)) in on.iter().enumerate() {
        left_exprs.push((Arc::clone(left_expr), format!("{JOIN_KEY_PREFIX}{index}")));
    }
    let row_id_field = left_schema
        .fields()
        .last()
        .ok_or_else(|| ExecutionError::InternalError("row id column missing".to_string()))?;
    let row_id_expr = Column::new_with_schema(row_id_field.name(), left_schema.as_ref())
        .map_err(ExecutionError::DataFusionError)?;
    left_exprs.push((Arc::new(row_id_expr), row_id_field.name().clone()));
    let row_id_index = left_exprs.len() - 1;

    let mut right_exprs = Vec::with_capacity(on.len());
    for (index, (_, right_expr)) in on.iter().enumerate() {
        right_exprs.push((Arc::clone(right_expr), format!("{JOIN_KEY_PREFIX}{index}")));
    }

    let mut left_index_map = HashMap::new();
    let mut right_index_map = HashMap::new();
    if let Some(filter) = filter {
        for column_index in filter.column_indices() {
            match column_index.side {
                JoinSide::Left => append_projected_column(
                    &mut left_exprs,
                    &left_schema,
                    column_index.index,
                    JOIN_FILTER_LEFT_PREFIX,
                    &mut left_index_map,
                )?,
                JoinSide::Right => append_projected_column(
                    &mut right_exprs,
                    &right_schema,
                    column_index.index,
                    JOIN_FILTER_RIGHT_PREFIX,
                    &mut right_index_map,
                )?,
                JoinSide::None => {
                    return Err(ExecutionError::InternalError(
                        "join filter column index missing join side".to_string(),
                    ))
                }
            }
        }
    }

    let left_projected: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(left_exprs, left.clone())?);
    let right_projected: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(right_exprs, right.clone())?);
    let join_on = build_projected_join_on(&left_projected, &right_projected, on.len())?;
    let join_filter = filter
        .map(|filter| {
            rewrite_join_filter_for_projection(
                filter,
                &left_index_map,
                &right_index_map,
                &left_projected,
                &right_projected,
            )
        })
        .transpose()?;

    Ok((
        left_projected,
        right_projected,
        join_on,
        join_filter,
        row_id_index,
    ))
}

fn append_projected_column(
    exprs: &mut Vec<(Arc<dyn PhysicalExpr>, String)>,
    schema: &SchemaRef,
    index: usize,
    prefix: &str,
    index_map: &mut HashMap<usize, usize>,
) -> ExecutionResult<()> {
    if index_map.contains_key(&index) {
        return Ok(());
    }
    let field = schema.field(index);
    let expr = Column::new_with_schema(field.name(), schema.as_ref())
        .map_err(ExecutionError::DataFusionError)?;
    let output_index = exprs.len();
    exprs.push((Arc::new(expr), format!("{prefix}{index}")));
    index_map.insert(index, output_index);
    Ok(())
}

fn rewrite_join_filter_for_projection(
    filter: &JoinFilter,
    left_index_map: &HashMap<usize, usize>,
    right_index_map: &HashMap<usize, usize>,
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
) -> ExecutionResult<JoinFilter> {
    let mut column_indices = Vec::with_capacity(filter.column_indices().len());
    let mut fields = Vec::with_capacity(filter.column_indices().len());
    for column_index in filter.column_indices() {
        let new_index = match column_index.side {
            JoinSide::Left => left_index_map.get(&column_index.index).copied(),
            JoinSide::Right => right_index_map.get(&column_index.index).copied(),
            JoinSide::None => None,
        }
        .ok_or_else(|| {
            ExecutionError::InternalError(format!(
                "join filter column {} missing from projected inputs",
                column_index.index
            ))
        })?;
        column_indices.push(ColumnIndex {
            side: column_index.side,
            index: new_index,
        });
        let field = match column_index.side {
            JoinSide::Left => left.schema().field(new_index).clone(),
            JoinSide::Right => right.schema().field(new_index).clone(),
            JoinSide::None => {
                return Err(ExecutionError::InternalError(
                    "join filter column index missing join side".to_string(),
                ))
            }
        };
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));
    Ok(JoinFilter::new(
        filter.expression().clone(),
        column_indices,
        schema,
    ))
}

fn build_projected_join_on(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
    key_count: usize,
) -> ExecutionResult<JoinOn> {
    let left_schema = left.schema();
    let right_schema = right.schema();
    let mut on = Vec::with_capacity(key_count);
    for index in 0..key_count {
        let name = format!("{JOIN_KEY_PREFIX}{index}");
        let left_expr = Column::new_with_schema(&name, left_schema.as_ref())
            .map_err(ExecutionError::DataFusionError)?;
        let right_expr = Column::new_with_schema(&name, right_schema.as_ref())
            .map_err(ExecutionError::DataFusionError)?;
        on.push((
            Arc::new(left_expr) as Arc<dyn PhysicalExpr>,
            Arc::new(right_expr) as Arc<dyn PhysicalExpr>,
        ));
    }
    Ok(on)
}

fn repartition_join_keys(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    key_count: usize,
    partition_count: usize,
) -> ExecutionResult<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
    let left_partitioning = Partitioning::Hash(
        build_projected_partition_exprs(&left, key_count)?,
        partition_count,
    );
    let right_partitioning = Partitioning::Hash(
        build_projected_partition_exprs(&right, key_count)?,
        partition_count,
    );
    let left = Arc::new(RepartitionExec::try_new(left, left_partitioning)?);
    let right = Arc::new(RepartitionExec::try_new(right, right_partitioning)?);
    Ok((left, right))
}

fn build_projected_partition_exprs(
    plan: &Arc<dyn ExecutionPlan>,
    key_count: usize,
) -> ExecutionResult<Vec<Arc<dyn PhysicalExpr>>> {
    let schema = plan.schema();
    let mut exprs = Vec::with_capacity(key_count);
    for index in 0..key_count {
        let name = format!("{JOIN_KEY_PREFIX}{index}");
        let expr = Column::new_with_schema(&name, schema.as_ref())
            .map_err(ExecutionError::DataFusionError)?;
        exprs.push(Arc::new(expr) as Arc<dyn PhysicalExpr>);
    }
    Ok(exprs)
}

fn create_forward_stage_input(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<(StageInput, SchemaRef, Partitioning)> {
    let schema = plan.schema();
    let partitioning = plan.output_partitioning().clone();
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
    Ok((
        StageInput {
            stage: s,
            mode: InputMode::Forward,
        },
        schema,
        partitioning,
    ))
}

fn project_drop_row_id(
    plan: Arc<dyn ExecutionPlan>,
    row_id_index: usize,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let exprs = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|&(idx, _field)| idx != row_id_index)
        .map(|(_idx, field)| {
            Column::new_with_schema(field.name(), schema.as_ref()).map(|expr| {
                (
                    Arc::new(expr) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(ExecutionError::DataFusionError)?;
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

fn apply_projection_indices(
    plan: Arc<dyn ExecutionPlan>,
    projection: &[usize],
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let exprs = projection
        .iter()
        .map(|index| {
            let field = schema.fields().get(*index).ok_or_else(|| {
                ExecutionError::InternalError(format!("projection index {} out of bounds", index))
            })?;
            Column::new_with_schema(field.name(), schema.as_ref())
                .map(|expr| {
                    (
                        Arc::new(expr) as Arc<dyn PhysicalExpr>,
                        field.name().clone(),
                    )
                })
                .map_err(ExecutionError::DataFusionError)
        })
        .collect::<ExecutionResult<Vec<_>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

fn create_merge_input(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let partitioning = plan.output_partitioning().clone();
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
        schema,
        partitioning,
    )))
}

fn create_shuffle(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    partitioning: Partitioning,
    consumption: ShuffleConsumption,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let distribution = match partitioning.clone() {
        Partitioning::RoundRobinBatch(channels) | Partitioning::UnknownPartitioning(channels) => {
            OutputDistribution::RoundRobin { channels }
        }
        Partitioning::Hash(keys, channels) => OutputDistribution::Hash { keys, channels },
    };
    let schema = plan.schema();
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
        schema,
        partitioning.clone(),
    )))
}

fn rewrite_inputs(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<(Arc<dyn ExecutionPlan>, Vec<StageInput>)> {
    let mut inputs = vec![];
    let result = plan.transform(|node| {
        if let Some(placeholder) = node.as_any().downcast_ref::<StageInputExec<StageInput>>() {
            let index = inputs.len();
            inputs.push(placeholder.input().clone());
            let placeholder = StageInputExec::new(
                index,
                placeholder.schema(),
                placeholder.properties().output_partitioning().clone(),
            );
            Ok(Transformed::yes(Arc::new(placeholder)))
        } else {
            Ok(Transformed::no(node))
        }
    });
    Ok((result.data()?, inputs))
}

// TODO: support driver stage with inputs
fn create_driver_stage(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let partitioning = plan.output_partitioning().clone();
    let stage = Stage {
        inputs: vec![],
        plan: plan.clone(),
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
        schema,
        partitioning,
    )))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests use unwrap for brevity")]
#[expect(clippy::panic, reason = "tests may use panic for unreachable branches")]
mod tests {
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion::physical_plan::joins::JoinOn;
    use datafusion::physical_plan::test::TestMemoryExec;
    use sail_physical_plan::distributed_collect_left_join::DistributedCollectLeftJoinExec;

    use super::*;

    fn make_exec(schema: Arc<Schema>, partitions: usize) -> Arc<dyn ExecutionPlan> {
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Int32 => Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                DataType::Boolean => Arc::new(BooleanArray::from(vec![true, false, true])),
                other => panic!("unsupported test field type: {other:?}"),
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let data = vec![vec![batch]; partitions];
        TestMemoryExec::try_new_exec(&data, schema, None).unwrap()
    }

    fn build_join_on(left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>) -> JoinOn {
        let left_expr = Arc::new(Column::new_with_schema("id", left.schema().as_ref()).unwrap());
        let right_expr = Arc::new(Column::new_with_schema("id", right.schema().as_ref()).unwrap());
        vec![(left_expr, right_expr)]
    }

    fn plan_has_coalesce(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.as_any().is::<CoalescePartitionsExec>() {
            return true;
        }
        plan.children().iter().any(|child| plan_has_coalesce(child))
    }

    #[test]
    fn job_graph_handles_distributed_collect_left_join_types() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let left = make_exec(schema.clone(), 1);
        let right = make_exec(schema.clone(), 2);
        let join_on = build_join_on(&left, &right);
        let join_types = vec![
            JoinType::Left,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::LeftMark,
        ];
        for join_type in join_types {
            let join = DistributedCollectLeftJoinExec::try_new(
                left.clone(),
                right.clone(),
                join_on.clone(),
                None,
                join_type,
                None,
                datafusion::common::NullEquality::NullEqualsNothing,
            )
            .unwrap();
            let graph = JobGraph::try_new(Arc::new(join));
            assert!(graph.is_ok(), "job graph should build for {join_type}");
        }
    }

    #[test]
    fn fallback_rewrites_collect_left_hash_join() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let left = make_exec(schema.clone(), 1);
        let right = make_exec(schema.clone(), 2);
        let join_on = build_join_on(&left, &right);
        let join = HashJoinExec::try_new(
            left,
            right,
            join_on,
            None,
            &JoinType::Left,
            None,
            PartitionMode::CollectLeft,
            datafusion::common::NullEquality::NullEqualsNothing,
        )
        .unwrap();
        let rewritten =
            ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows(Arc::new(join))
                .unwrap();
        let Some(join) = rewritten.as_any().downcast_ref::<HashJoinExec>() else {
            panic!("expected HashJoinExec after rewrite");
        };
        assert_eq!(join.mode, PartitionMode::Partitioned);
    }

    #[test]
    fn optimized_collect_left_avoids_left_coalesce() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let left = make_exec(schema.clone(), 2);
        let right = make_exec(schema.clone(), 1);
        let join_on = build_join_on(&left, &right);
        for join_type in [JoinType::LeftSemi, JoinType::LeftAnti, JoinType::LeftMark] {
            let join = DistributedCollectLeftJoinExec::try_new(
                left.clone(),
                right.clone(),
                join_on.clone(),
                None,
                join_type,
                None,
                datafusion::common::NullEquality::NullEqualsNothing,
            )
            .unwrap();
            let graph = JobGraph::try_new(Arc::new(join)).unwrap();
            let has_coalesce = graph
                .stages()
                .iter()
                .any(|stage| plan_has_coalesce(&stage.plan));
            assert!(
                !has_coalesce,
                "optimized collect-left should avoid coalesce for {join_type}"
            );
        }
    }

    #[test]
    fn optimized_collect_left_rewrites_join_filter_indices() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let left = make_exec(left_schema.clone(), 1);
        let right = make_exec(right_schema.clone(), 1);
        let join_on = build_join_on(&left, &right);

        let filter_expr =
            Arc::new(Column::new("flag", 0)) as Arc<dyn datafusion::physical_plan::PhysicalExpr>;
        let filter = JoinFilter::new(
            filter_expr,
            vec![ColumnIndex {
                side: JoinSide::Left,
                index: 1,
            }],
            Arc::new(Schema::new(vec![left_schema.field(1).clone()])),
        );

        let (left_projected, _right_projected, _join_on, rewritten, row_id_index) =
            build_projected_join_inputs(&join_on, Some(&filter), &left, &right).unwrap();
        assert_eq!(row_id_index, 1);
        let rewritten = rewritten.unwrap();
        let new_index = rewritten.column_indices()[0].index;
        assert_eq!(new_index, left_projected.schema().fields().len() - 1);
    }

    #[test]
    fn left_full_uses_dual_left_inputs() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let left = make_exec(schema.clone(), 2);
        let right = make_exec(schema.clone(), 1);
        let join_on = build_join_on(&left, &right);
        let join = DistributedCollectLeftJoinExec::try_new(
            left,
            right,
            join_on,
            None,
            JoinType::Left,
            None,
            datafusion::common::NullEquality::NullEqualsNothing,
        )
        .unwrap();
        let graph = JobGraph::try_new(Arc::new(join)).unwrap();
        let has_dual_left_inputs = graph.stages().iter().any(|stage| {
            let has_forward = stage
                .inputs
                .iter()
                .any(|input| input.mode == InputMode::Forward);
            let has_shuffle = stage
                .inputs
                .iter()
                .any(|input| input.mode == InputMode::Shuffle);
            has_forward && has_shuffle
        });
        assert!(
            has_dual_left_inputs,
            "expected both forward and shuffle inputs for left join"
        );
    }
}
