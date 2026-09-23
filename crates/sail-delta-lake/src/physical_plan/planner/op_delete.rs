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

use datafusion::common::{DataFusionError, Result, ToDFSchema};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::datasource::MERGE_ROW_INDEX_COLUMN;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;

use super::commit::assemble_commit_plan;
use super::context::PlannerContext;
use super::metadata_predicate::{build_metadata_filter, predicate_requires_stats};
use super::utils::{
    LogReplayOptions, build_log_replay_pipeline_with_options, prepare_delta_writer_input,
};
use crate::datasource::{
    DeltaScanConfig, PATH_COLUMN, df_logical_schema, rewrite_predicate_for_column_mapping,
};
use crate::physical_plan::{
    DeletionVectorRowOperationMode, DeletionVectorRowsWriterConfig, DeletionVectorRowsWriterExec,
    DeltaCommitContext, DeltaDecodePath, DeltaDiscoveryExec, DeltaScanByAddsExec,
    DeltaWriterExecOptions, prepare_delta_write_context,
};
use crate::spec::DeltaOperation;

pub async fn build_delete_plan(
    ctx: &PlannerContext<'_>,
    condition: ExprWithSource,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
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
    // DELETE removes rows only when the predicate is true; false and null rows remain.
    let retention_condition = condition_expr.clone().is_not_true();
    let physical_retention_condition = ctx
        .session()
        .create_physical_expr(retention_condition, &table_df_schema)?;

    // Partition-only predicates can delete entire files without scanning data. In that case,
    // build a visible metadata pipeline over a log-derived meta table.
    let partition_only = !predicate_requires_stats(&condition_expr, &partition_columns);
    let log_replay_options = LogReplayOptions {
        // `DeltaRemoveActionsExec` decodes Add.stats to report numTouchedRows, including
        // for partition-only deletes where data-skipping itself does not need stats_json.
        include_stats_json: true,
        ..Default::default()
    };

    // Independent pipeline per branch: a shared Arc subtree starves the remove
    // branch (zero rows) under distributed execution.
    let meta_scan_w: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options.clone())
            .await?;
    let meta_scan_w: Arc<dyn ExecutionPlan> = build_metadata_filter(
        ctx.session(),
        meta_scan_w,
        snapshot_state,
        snapshot_state.schema(),
        condition_expr.clone(),
    )?;
    let find_files_writer: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::new(
        meta_scan_w,
        ctx.table_url().clone(),
        version,
        partition_columns.clone(),
        partition_only,
    )?);

    let meta_scan_r: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options).await?;
    let meta_scan_r: Arc<dyn ExecutionPlan> = build_metadata_filter(
        ctx.session(),
        meta_scan_r,
        snapshot_state,
        snapshot_state.schema(),
        condition_expr,
    )?;
    let find_files_remove: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::new(
        meta_scan_r,
        ctx.table_url().clone(),
        version,
        partition_columns.clone(),
        partition_only,
    )?);

    // Spread Add actions across partitions so `DeltaScanByAddsExec` can scan files in parallel.
    // TODO(adaptive-partitioning): Keep this aligned with `scan_planner.rs`.
    let target_partitions = ctx.session().config().target_partitions().max(1);
    let find_files_writer: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_writer,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);
    let find_files_remove: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files_remove,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let scan_exec = Arc::new(DeltaScanByAddsExec::new(
        find_files_writer,
        ctx.table_url().clone(),
        version,
        table_schema.clone(),
        table_schema.clone(),
        crate::datasource::DeltaScanConfig::default(),
        None,
        None,
        None,
        ctx.lakehouse_table().cloned(),
        snapshot_state.load_config().catalog_managed_commits.clone(),
    ));

    // Adapt the predicate to the scan schema. PhysicalExpr Column indices are schema-dependent,
    // and DeltaScanByAddsExec may reorder/augment the schema compared to the original table schema.
    let adapter_factory = Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {});
    let adapter = adapter_factory
        .create(table_schema.clone(), scan_exec.schema())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let adapted_retention_condition = adapter
        .rewrite(physical_retention_condition)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let filter_exec: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(adapted_retention_condition, scan_exec)?);
    let writer_input = prepare_delta_writer_input(filter_exec, &partition_columns, None)?;

    let operation = Some(DeltaOperation::Delete {
        predicate: condition.source,
    });
    let writer_options = DeltaWriterExecOptions::from(ctx.options().clone());
    let write_context = prepare_delta_write_context(
        ctx.table_url(),
        Some(snapshot_state.as_ref()),
        &writer_options,
        ctx.metadata_configuration(),
        &partition_columns,
        &sail_common_datafusion::datasource::PhysicalSinkMode::Append,
        ctx.table_exists(),
        &writer_input.schema(),
        operation,
    )?;

    assemble_commit_plan(
        writer_input,
        Some(find_files_remove),
        Some(snapshot_state.physical_partition_columns()),
        ctx.table_url().clone(),
        writer_options,
        ctx.metadata_configuration().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        ctx.options().user_metadata.clone(),
        write_context,
        ctx.lakehouse_table().cloned(),
    )
}

/// Merge-on-Read DELETE: write deletion vectors instead of rewriting files.
///
/// The plan:
/// 1. Discover affected files via the metadata pipeline (same as CoW)
/// 2. For each affected file, scan to find matching row indices
/// 3. Write DV files with bitmaps of the matching rows
/// 4. Emit Remove(old_add) + Add(path, dv=new_dv) commit actions
pub async fn build_delete_plan_mor(
    ctx: &PlannerContext<'_>,
    condition: ExprWithSource,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let version = snapshot_state.version();

    // Verify that deletion vectors are enabled on this table
    snapshot_state
        .verify_deletion_vectors()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

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

    // Partition-only predicates can delete entire files without scanning data.
    // For MoR with partition-only predicates, we still fall back to file removal
    // since there's no benefit in writing DVs for entire files.
    let partition_only = !predicate_requires_stats(&condition_expr, &partition_columns);
    if partition_only {
        // For partition-only predicates, use the CoW path as it's more efficient
        // (just remove whole files).
        return build_delete_plan(ctx, condition).await;
    }

    let log_replay_options = LogReplayOptions {
        include_stats_json: true,
        include_extended_add_metadata: true,
        ..Default::default()
    };
    let meta_scan =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options.clone())
            .await?;
    let meta_scan = build_metadata_filter(
        ctx.session(),
        meta_scan,
        snapshot_state,
        snapshot_state.schema(),
        condition_expr.clone(),
    )?;
    let find_files: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::new(
        meta_scan,
        ctx.table_url().clone(),
        version,
        partition_columns.clone(),
        false,
    )?);
    let target_partitions = ctx.session().config().target_partitions().max(1);
    // Assign files before scanning so matching rows stay with their metadata partition.
    let discovery_path = DeltaDecodePath::expression(PATH_COLUMN, &find_files.schema())?;
    let find_files = Arc::new(RepartitionExec::try_new(
        find_files,
        Partitioning::Hash(vec![discovery_path], target_partitions),
    )?);
    let scan_config = DeltaScanConfig {
        file_column_name: Some(PATH_COLUMN.to_string()),
        row_index_column_name: Some(MERGE_ROW_INDEX_COLUMN.to_string()),
        hash_partition_files: true,
        enable_parquet_pushdown: true,
        ..Default::default()
    };
    let scan_schema = df_logical_schema(
        snapshot_state,
        &scan_config.file_column_name,
        &scan_config.row_index_column_name,
        &None,
        &None,
        Some(Arc::clone(&table_schema)),
    )
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut projection = collect_columns(&physical_condition)
        .into_iter()
        .map(|column| column.index())
        .collect::<Vec<_>>();
    projection.sort_unstable();
    projection.dedup();
    projection.push(scan_schema.index_of(PATH_COLUMN)?);
    projection.push(scan_schema.index_of(MERGE_ROW_INDEX_COLUMN)?);
    let scan_schema = Arc::new(scan_schema.project(&projection)?);
    let pushdown_filter = rewrite_predicate_for_column_mapping(
        Arc::clone(&physical_condition),
        &table_schema,
        snapshot_state.effective_column_mapping_mode(),
    )?;
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DeltaScanByAddsExec::new(
        find_files,
        ctx.table_url().clone(),
        version,
        Arc::clone(&table_schema),
        Arc::clone(&scan_schema),
        scan_config,
        Some(projection),
        None,
        Some(pushdown_filter),
        ctx.lakehouse_table().cloned(),
        snapshot_state.load_config().catalog_managed_commits.clone(),
    ));
    let physical_condition = SchemaEvolutionPhysicalExprAdapterFactory {}
        .create(Arc::clone(&table_schema), Arc::clone(&scan_schema))?
        .rewrite(physical_condition)?;
    let matches: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(physical_condition, scan)?);
    let positions = [PATH_COLUMN, MERGE_ROW_INDEX_COLUMN]
        .into_iter()
        .map(|name| {
            Ok((
                Arc::new(Column::new(name, scan_schema.index_of(name)?)) as _,
                name.to_string(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let matches: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(positions, matches)?);

    // The writer consumes metadata and matching rows independently across workers.
    let metadata =
        build_log_replay_pipeline_with_options(ctx, snapshot_state, log_replay_options).await?;
    let metadata = build_metadata_filter(
        ctx.session(),
        metadata,
        snapshot_state,
        snapshot_state.schema(),
        condition_expr,
    )?;
    let metadata_path = DeltaDecodePath::expression(PATH_COLUMN, &metadata.schema())?;
    let metadata = Arc::new(RepartitionExec::try_new(
        metadata,
        Partitioning::Hash(vec![metadata_path], target_partitions),
    )?);
    let dv_writer: Arc<dyn ExecutionPlan> = Arc::new(DeletionVectorRowsWriterExec::new(
        matches,
        metadata,
        ctx.table_url().clone(),
        DeletionVectorRowsWriterConfig::new(
            PATH_COLUMN,
            MERGE_ROW_INDEX_COLUMN,
            DeletionVectorRowOperationMode::Delete,
            version,
            Some(snapshot_state.physical_partition_columns()),
            Some(DeltaOperation::Delete {
                predicate: condition.source,
            }),
        ),
    )?);

    // Wrap in CoalescePartitions → DeltaCommitExec for final commit
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(dv_writer),
    );

    Ok(Arc::new(crate::physical_plan::DeltaCommitExec::new(
        coalesced,
        ctx.table_url().clone(),
        partition_columns,
        ctx.table_exists(),
        table_schema,
        sail_common_datafusion::datasource::PhysicalSinkMode::Append,
        ctx.options().user_metadata.clone(),
        DeltaCommitContext::from_snapshot(snapshot_state.as_ref()),
        ctx.lakehouse_table().cloned(),
    )))
}
