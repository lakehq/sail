// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::physical_expr::PhysicalExprWithSource;

use super::context::PlannerContext;
use super::utils::{
    align_schemas_for_union, build_log_replay_pipeline, build_log_replay_pipeline_with_options,
    build_standard_write_layers, LogReplayFilter, LogReplayOptions,
};
use crate::datasource::schema::DataFusionMixins;
use crate::datasource::PredicateProperties;
use crate::kernel::{DeltaOperation, SaveMode};
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaDiscoveryExec,
    DeltaRemoveActionsExec, DeltaScanByAddsExec, DeltaWriterExec,
};

pub async fn build_write_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    match sink_mode.clone() {
        PhysicalSinkMode::OverwriteIf { condition } => {
            build_overwrite_if_plan(ctx, input, condition, sort_order).await
        }
        _ => build_standard_plan(ctx, input, sink_mode, sort_order).await,
    }
}

async fn build_standard_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    match sink_mode {
        PhysicalSinkMode::Overwrite => build_full_overwrite_plan(ctx, input, sort_order).await,
        other => {
            let input_schema = input.schema();
            build_standard_write_layers(ctx, input, &other, sort_order, input_schema)
        }
    }
}

async fn build_full_overwrite_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();

    let target_partitions = ctx.session().config().target_partitions().max(1);
    let plan = create_projection(input, ctx.partition_columns().to_vec())?;
    let plan = create_repartition(plan, ctx.partition_columns().to_vec(), target_partitions)?;
    let plan = create_sort(plan, ctx.partition_columns().to_vec(), sort_order)?;

    let writer_schema = plan.schema();
    let writer: Arc<dyn ExecutionPlan> = Arc::new(DeltaWriterExec::new(
        plan,
        ctx.table_url().clone(),
        ctx.options().clone(),
        ctx.partition_columns().to_vec(),
        PhysicalSinkMode::Overwrite,
        ctx.table_exists(),
        writer_schema,
        None,
    )?);

    // For existing tables, build a remove plan from the active file set and union it with the
    // writer's Add actions, so the commit is self-contained and executor-friendly.
    let commit_input: Arc<dyn ExecutionPlan> = if ctx.table_exists() {
        let table = ctx.open_table().await?;
        let snapshot_state = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        let version = snapshot_state.version();
        let partition_columns = snapshot_state.metadata().partition_columns().clone();

        let kernel_snapshot = snapshot_state.snapshot().snapshot().inner.clone();
        let log_segment = kernel_snapshot.log_segment();
        let checkpoint_files = log_segment
            .checkpoint_parts
            .iter()
            .map(|p| p.filename.clone())
            .collect::<Vec<_>>();
        let commit_files = log_segment
            .ascending_commit_files
            .iter()
            .map(|p| p.filename.clone())
            .collect::<Vec<_>>();

        let meta_scan: Arc<dyn ExecutionPlan> = build_log_replay_pipeline(
            ctx,
            ctx.table_url().clone(),
            version,
            partition_columns.clone(),
            checkpoint_files,
            commit_files,
        )
        .await?;

        let all_adds: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::from_log_scan(
            meta_scan,
            ctx.table_url().clone(),
            version,
            partition_columns,
            true, // partition_scan
        )?);
        let remove_plan: Arc<dyn ExecutionPlan> = Arc::new(DeltaRemoveActionsExec::new(all_adds)?);

        UnionExec::try_new(vec![writer, remove_plan])?
    } else {
        writer
    };

    Ok(Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(commit_input)),
        ctx.table_url().clone(),
        ctx.partition_columns().to_vec(),
        ctx.table_exists(),
        input_schema,
        PhysicalSinkMode::Overwrite,
    )))
}

async fn build_overwrite_if_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    condition: PhysicalExprWithSource,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    let version = snapshot_state.version();
    let table_schema = snapshot_state
        .snapshot()
        .arrow_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();

    let old_data_plan =
        build_old_data_plan(ctx, condition.expr.clone(), version, table_schema.clone()).await?;

    let target_partitions = ctx.session().config().target_partitions().max(1);
    let new_plan = create_projection(Arc::clone(&input), ctx.partition_columns().to_vec())
        .and_then(|plan| {
            create_repartition(plan, ctx.partition_columns().to_vec(), target_partitions)
        })
        .and_then(|plan| create_sort(plan, ctx.partition_columns().to_vec(), sort_order))?;

    let (aligned_new, aligned_old) = align_schemas_for_union(new_plan, old_data_plan)?;
    let union_plan = UnionExec::try_new(vec![aligned_new, aligned_old])?;

    let input_schema = input.schema();
    let operation_override = Some(DeltaOperation::Write {
        mode: SaveMode::Overwrite,
        partition_by: if ctx.partition_columns().is_empty() {
            None
        } else {
            Some(ctx.partition_columns().to_vec())
        },
        predicate: condition.source.clone(),
    });
    let writer = Arc::new(DeltaWriterExec::new(
        Arc::clone(&union_plan),
        ctx.table_url().clone(),
        ctx.options().clone(),
        ctx.partition_columns().to_vec(),
        PhysicalSinkMode::OverwriteIf {
            condition: condition.clone(),
        },
        ctx.table_exists(),
        union_plan.schema(),
        operation_override,
    )?);

    let mut expr_props = PredicateProperties::new(partition_columns.clone());
    expr_props
        .analyze_predicate(&condition.expr)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let kernel_snapshot = snapshot_state.snapshot().snapshot().inner.clone();
    let log_segment = kernel_snapshot.log_segment();
    let checkpoint_files = log_segment
        .checkpoint_parts
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();
    let commit_files = log_segment
        .ascending_commit_files
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();

    let mut log_replay_options = LogReplayOptions::default();
    if expr_props.partition_only {
        log_replay_options.log_filter = Some(LogReplayFilter {
            predicate: condition.expr.clone(),
            table_schema: table_schema.clone(),
        });
    }
    let meta_scan: Arc<dyn ExecutionPlan> = build_log_replay_pipeline_with_options(
        ctx,
        ctx.table_url().clone(),
        version,
        partition_columns.clone(),
        checkpoint_files,
        commit_files,
        log_replay_options,
    )
    .await?;

    let find_files_plan: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        Some(condition.expr.clone()),
        Some(table_schema.clone()),
        version,
        partition_columns.clone(),
        expr_props.partition_only,
    )?);
    let remove_plan = Arc::new(DeltaRemoveActionsExec::new(find_files_plan)?);

    let union_actions = UnionExec::try_new(vec![writer, remove_plan])?;

    Ok(Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(union_actions)),
        ctx.table_url().clone(),
        ctx.partition_columns().to_vec(),
        ctx.table_exists(),
        input_schema,
        PhysicalSinkMode::OverwriteIf { condition },
    )))
}

async fn build_old_data_plan(
    ctx: &PlannerContext<'_>,
    condition: Arc<dyn PhysicalExpr>,
    version: i64,
    table_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    // For partition-only predicates, the scan-by-adds stage will be a no-op (partition_scan=true),
    // so build the same log-derived metadata path as the main find-files plan.
    let mut expr_props = PredicateProperties::new(ctx.partition_columns().to_vec());
    expr_props
        .analyze_predicate(&condition)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    let kernel_snapshot = snapshot_state.snapshot().snapshot().inner.clone();
    let log_segment = kernel_snapshot.log_segment();
    let checkpoint_files = log_segment
        .checkpoint_parts
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();
    let commit_files = log_segment
        .ascending_commit_files
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();

    let mut log_replay_options = LogReplayOptions::default();
    if expr_props.partition_only {
        log_replay_options.log_filter = Some(LogReplayFilter {
            predicate: condition.clone(),
            table_schema: table_schema.clone(),
        });
    }
    let meta_scan: Arc<dyn ExecutionPlan> = build_log_replay_pipeline_with_options(
        ctx,
        ctx.table_url().clone(),
        version,
        ctx.partition_columns().to_vec(),
        checkpoint_files,
        commit_files,
        log_replay_options,
    )
    .await?;

    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        Some(condition.clone()),
        Some(table_schema.clone()),
        version,
        ctx.partition_columns().to_vec(),
        expr_props.partition_only,
    )?);

    // Spread Add actions across partitions so `DeltaScanByAddsExec` can scan files in parallel.
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_exec,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        Arc::clone(&find_files_exec),
        ctx.table_url().clone(),
        table_schema,
    ));

    let negated_condition = Arc::new(NotExpr::new(condition));
    let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

    Ok(filter_exec)
}
