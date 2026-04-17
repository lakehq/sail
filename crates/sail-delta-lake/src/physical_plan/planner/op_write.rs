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
use datafusion::common::{DataFusionError, Result, ToDFSchema};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::context::PlannerContext;
use super::metadata_predicate::{build_metadata_filter, predicate_requires_stats};
use super::utils::{
    align_schemas_for_union, build_log_replay_pipeline_with_options, build_standard_write_layers,
    LogReplayOptions,
};
use crate::kernel::{DeltaOperation, SaveMode};
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaDiscoveryExec,
    DeltaRemoveActionsExec, DeltaScanByAddsExec, DeltaWriterExec, DeltaWriterExecOptions,
};
use crate::table::DeltaSnapshot;

pub async fn build_write_plan(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    match sink_mode.clone() {
        PhysicalSinkMode::OverwriteIf { condition, source } => {
            let condition = condition.ok_or_else(|| {
                DataFusionError::Plan(
                    "missing overwrite-if logical condition while building Delta plan".to_string(),
                )
            })?;
            build_overwrite_if_plan(ctx, input, *condition, source, sort_order).await
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
        DeltaWriterExecOptions::from(ctx.options().clone())
            .with_generation_expressions(ctx.generation_expressions().clone()),
        ctx.metadata_configuration().clone(),
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

        let meta_scan: Arc<dyn ExecutionPlan> = build_log_replay_pipeline_with_options(
            ctx,
            &snapshot_state,
            LogReplayOptions::default(),
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
    condition: ExprWithSource,
    source: Option<String>,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    let version = snapshot_state.version();
    let table_schema = snapshot_state
        .input_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();
    let table_df_schema = table_schema
        .clone()
        .to_dfschema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let condition_expr = condition.expr.clone();
    let physical_condition = ctx
        .session()
        .create_physical_expr(condition_expr.clone(), &table_df_schema)?;
    let predicate_source = source.or(condition.source);

    let old_data_plan = build_old_data_plan(
        ctx,
        condition_expr.clone(),
        physical_condition.clone(),
        &snapshot_state,
        table_schema.clone(),
    )
    .await?;

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
        predicate: predicate_source.clone(),
    });
    let writer = Arc::new(DeltaWriterExec::new(
        Arc::clone(&union_plan),
        ctx.table_url().clone(),
        DeltaWriterExecOptions::from(ctx.options().clone())
            .with_generation_expressions(ctx.generation_expressions().clone()),
        ctx.metadata_configuration().clone(),
        ctx.partition_columns().to_vec(),
        PhysicalSinkMode::OverwriteIf {
            condition: None,
            source: predicate_source.clone(),
        },
        ctx.table_exists(),
        union_plan.schema(),
        operation_override,
    )?);

    let partition_only = !predicate_requires_stats(&condition_expr, &partition_columns);
    let log_replay_options = LogReplayOptions {
        include_stats_json: !partition_only,
        ..Default::default()
    };
    let meta_scan: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, &snapshot_state, log_replay_options).await?;
    let meta_scan: Arc<dyn ExecutionPlan> = build_metadata_filter(
        ctx.session(),
        meta_scan,
        &snapshot_state,
        condition_expr.clone(),
    )?;

    let find_files_plan: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        None,
        None,
        version,
        partition_columns.clone(),
        partition_only,
    )?);
    let remove_plan = Arc::new(DeltaRemoveActionsExec::new(find_files_plan)?);

    let union_actions = UnionExec::try_new(vec![writer, remove_plan])?;

    Ok(Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(union_actions)),
        ctx.table_url().clone(),
        ctx.partition_columns().to_vec(),
        ctx.table_exists(),
        input_schema,
        PhysicalSinkMode::OverwriteIf {
            condition: None,
            source: predicate_source,
        },
    )))
}

async fn build_old_data_plan(
    ctx: &PlannerContext<'_>,
    condition_expr: Expr,
    condition: Arc<dyn PhysicalExpr>,
    snapshot_state: &DeltaSnapshot,
    table_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let version = snapshot_state.version();
    let partition_only = !predicate_requires_stats(&condition_expr, ctx.partition_columns());
    let log_replay_options = LogReplayOptions {
        include_stats_json: !partition_only,
        ..Default::default()
    };
    let meta_scan: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options).await?;
    let meta_scan: Arc<dyn ExecutionPlan> = build_metadata_filter(
        ctx.session(),
        meta_scan,
        snapshot_state,
        condition_expr.clone(),
    )?;

    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        ctx.table_url().clone(),
        None,
        None,
        version,
        ctx.partition_columns().to_vec(),
        partition_only,
    )?);

    // Spread Add actions across partitions so `DeltaScanByAddsExec` can scan files in parallel.
    // TODO(adaptive-partitioning): Keep this aligned with `scan_planner.rs`.
    // Plan: switch from fixed `target_partitions` + round-robin to size-driven partition count
    // first, then size-aware distribution to avoid oversharding and worker skew.
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_exec: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_exec,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        Arc::clone(&find_files_exec),
        ctx.table_url().clone(),
        version,
        table_schema.clone(),
        table_schema,
        crate::datasource::DeltaScanConfig::default(),
        None,
        None,
        None,
    ));

    let negated_condition = Arc::new(NotExpr::new(condition));
    let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

    Ok(filter_exec)
}
