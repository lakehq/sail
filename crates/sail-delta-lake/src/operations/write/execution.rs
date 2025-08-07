use std::sync::Arc;

use datafusion::datasource::provider_as_source;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::DataFrame;
use deltalake::errors::DeltaResult;
use deltalake::kernel::{Action, Add, Remove};
use deltalake::logstore::LogStoreRef;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::table::state::DeltaTableState;
use deltalake::Path;
use uuid::Uuid;

/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/operations/write/execution.rs>
use crate::delta_datafusion::{
    datafusion_to_delta_error, find_files, DataFusionMixins, DeltaScanConfigBuilder,
    DeltaTableProvider,
};
use crate::operations::write::writer::{DeltaWriter, WriterConfig};

/// Configuration for the writer on how to collect stats
#[derive(Clone)]
pub struct WriterStatsConfig {
    /// Number of columns to collect stats for, idx based
    pub num_indexed_cols: i32,
    /// Optional list of columns which to collect stats for, takes precedende over num_index_cols
    pub stats_columns: Option<Vec<String>>,
}

impl WriterStatsConfig {
    /// Create new writer stats config
    pub fn new(num_indexed_cols: i32, stats_columns: Option<Vec<String>>) -> Self {
        Self {
            num_indexed_cols,
            stats_columns,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_non_empty_expr(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    partition_columns: Vec<String>,
    expression: &Expr,
    rewrite: &[Add],
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    partition_scan: bool,
    operation_id: Uuid,
) -> DeltaResult<(Vec<Action>, Option<DataFrame>)> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.
    let mut actions: Vec<Action> = Vec::new();

    // Take the insert plan schema since it might have been schema evolved, if its not
    // it is simply the table schema
    let scan_config = DeltaScanConfigBuilder::new()
        .with_schema(snapshot.input_schema()?)
        .build(snapshot)?;

    let target_provider = Arc::new(
        DeltaTableProvider::try_new(snapshot.clone(), log_store.clone(), scan_config.clone())?
            .with_files(rewrite.to_vec()),
    );

    let target_provider = provider_as_source(target_provider);
    let source = LogicalPlanBuilder::scan("target", target_provider.clone(), None)
        .map_err(datafusion_to_delta_error)?
        .build()
        .map_err(datafusion_to_delta_error)?;

    let df = DataFrame::new(state.clone(), source);

    let cdf_df = if !partition_scan {
        // Apply the negation of the filter and rewrite files
        let negated_expression = Expr::Not(Box::new(Expr::IsTrue(Box::new(expression.clone()))));

        let filter = df
            .clone()
            .filter(negated_expression)
            .map_err(datafusion_to_delta_error)?
            .create_physical_plan()
            .await
            .map_err(datafusion_to_delta_error)?;

        let add_actions: Vec<Action> = write_execution_plan(
            Some(snapshot),
            state.clone(),
            filter,
            partition_columns.clone(),
            log_store.object_store(Some(operation_id)),
            Path::from(""),
            Some(snapshot.table_config().target_file_size() as usize),
            None,
            writer_properties.clone(),
            writer_stats_config.clone(),
        )
        .await?;

        actions.extend(add_actions);

        // TODO: support CDC
        None
    } else {
        None
    };

    Ok((actions, cdf_df))
}

#[allow(clippy::too_many_arguments)]
async fn write_execution_plan(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: Arc<dyn object_store::ObjectStore>,
    table_path: Path,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<Vec<Action>> {
    // Create writer config based on snapshot or defaults
    let schema = if let Some(snapshot) = snapshot {
        snapshot.arrow_schema()?
    } else {
        plan.schema()
    };

    let writer_config = WriterConfig::new(
        schema,
        partition_columns,
        writer_properties,
        target_file_size.unwrap_or(32 * 1024 * 1024),
        write_batch_size.unwrap_or(1024),
        writer_stats_config.num_indexed_cols,
        writer_stats_config.stats_columns,
    );

    let mut writer = DeltaWriter::new(object_store, table_path, writer_config);

    // Execute the plan and write data
    let task_ctx = Arc::new(datafusion::execution::context::TaskContext::from(&state));
    for i in 0..plan.output_partitioning().partition_count() {
        let mut stream = plan
            .execute(i, task_ctx.clone())
            .map_err(datafusion_to_delta_error)?;

        while let Some(batch_result) = futures::StreamExt::next(&mut stream).await {
            let batch = batch_result.map_err(datafusion_to_delta_error)?;
            writer.write(&batch).await?;
        }
    }

    let add_actions = writer.close().await?;
    Ok(add_actions.into_iter().map(Action::Add).collect())
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_predicate_actions(
    predicate: Expr,
    log_store: LogStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    partition_columns: Vec<String>,
    writer_properties: Option<WriterProperties>,
    deletion_timestamp: i64,
    writer_stats_config: WriterStatsConfig,
    operation_id: Uuid,
) -> DeltaResult<(Vec<Action>, Option<DataFrame>)> {
    let candidates =
        find_files(snapshot, log_store.clone(), &state, Some(predicate.clone())).await?;

    let (mut actions, cdf_df) = execute_non_empty_expr(
        snapshot,
        log_store,
        state,
        partition_columns,
        &predicate,
        &candidates.candidates,
        writer_properties,
        writer_stats_config,
        candidates.partition_scan,
        operation_id,
    )
    .await?;

    let remove = candidates.candidates;

    for action in remove {
        actions.push(Action::Remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            deletion_vector: action.deletion_vector,
            tags: None,
            base_row_id: action.base_row_id,
            default_row_commit_version: action.default_row_commit_version,
        }))
    }
    Ok((actions, cdf_df))
}
