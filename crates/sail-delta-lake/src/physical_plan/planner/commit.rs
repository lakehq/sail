//! Shared helpers for assembling the tail of a row-level operation physical plan.
//!
//! Both DELETE and MERGE produce the same pipeline shape:
//!
//! ```text
//! DeltaWriterExec(writer_input)
//!   ∪ DeltaRemoveActionsExec(remove_source)    [optional]
//!     → CoalescePartitionsExec
//!       → DeltaCommitExec
//! ```
//!
//! This module eliminates duplication by providing:
//!
//! - [`assemble_commit_plan`]: builds the writer → (∪ remover) → coalesce → commit tail.
//! - [`build_remove_from_touched_files`]: joins a touched-file plan with the log replay
//!   pipeline to produce the Add-action stream consumed by `DeltaRemoveActionsExec`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{JoinType, NullEquality};
use datafusion_physical_expr::expressions::Column;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::context::PlannerContext;
use super::utils::{build_log_replay_pipeline_with_options, LogReplayOptions};
use crate::datasource::PATH_COLUMN;
use crate::kernel::DeltaOperation;
use crate::physical_plan::{
    DeltaCommitExec, DeltaDiscoveryExec, DeltaRemoveActionsExec, DeltaWriterExec,
    DeltaWriterExecOptions,
};
use crate::table::DeltaSnapshot;

/// Assemble the common tail of a row-level operation physical plan.
///
/// Given a `writer_input` (rows to write) and an optional `remove_source` (Add-action
/// metadata rows for files to remove), this builds:
///
/// ```text
/// DeltaWriterExec
///   ∪ DeltaRemoveActionsExec    [if remove_source is Some]
///     → CoalescePartitionsExec
///       → DeltaCommitExec
/// ```
#[expect(clippy::too_many_arguments)]
pub fn assemble_commit_plan(
    writer_input: Arc<dyn ExecutionPlan>,
    remove_source: Option<Arc<dyn ExecutionPlan>>,
    table_url: Url,
    options: DeltaWriterExecOptions,
    metadata_configuration: HashMap<String, String>,
    partition_columns: Vec<String>,
    table_exists: bool,
    table_schema: SchemaRef,
    operation: Option<DeltaOperation>,
    user_metadata: Option<String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let writer: Arc<dyn ExecutionPlan> = Arc::new(DeltaWriterExec::new(
        writer_input,
        table_url.clone(),
        options,
        metadata_configuration,
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        table_exists,
        table_schema.clone(),
        operation,
    )?);

    let commit_input: Arc<dyn ExecutionPlan> = if let Some(remove_src) = remove_source {
        let remover: Arc<dyn ExecutionPlan> = Arc::new(DeltaRemoveActionsExec::new(remove_src)?);
        UnionExec::try_new(vec![writer, remover])?
    } else {
        writer
    };

    Ok(Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(commit_input)),
        table_url,
        partition_columns,
        table_exists,
        table_schema,
        PhysicalSinkMode::Append,
        user_metadata,
    )))
}

/// Build a remove-action source from a set of touched file paths.
///
/// Joins the `touched_file_plan` (which yields `PATH_COLUMN` values for files that
/// were modified) with a log replay pipeline to retrieve the full Add-action metadata.
/// The output is suitable for feeding into [`DeltaRemoveActionsExec`].
pub async fn build_remove_from_touched_files(
    ctx: &PlannerContext<'_>,
    snapshot: &DeltaSnapshot,
    touched_file_plan: Arc<dyn ExecutionPlan>,
    table_url: &Url,
    version: i64,
    partition_columns: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let touched_plan = reset_plan_states(touched_file_plan)?;

    let meta_scan: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot, LogReplayOptions::default()).await?;

    let touched_schema = touched_plan.schema();
    let touched_idx = touched_schema
        .index_of(PATH_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
    let meta_schema = meta_scan.schema();
    let meta_idx = meta_schema
        .index_of(PATH_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;

    let join = Arc::new(HashJoinExec::try_new(
        touched_plan,
        meta_scan,
        vec![(
            Arc::new(Column::new(PATH_COLUMN, touched_idx)),
            Arc::new(Column::new(PATH_COLUMN, meta_idx)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    // Keep only the right side columns (metadata stream schema).
    let left_cols = touched_schema.fields().len();
    let proj_exprs: Vec<(Arc<dyn datafusion_physical_expr::PhysicalExpr>, String)> = meta_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            (
                Arc::new(Column::new(f.name(), left_cols + i))
                    as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                f.name().clone(),
            )
        })
        .collect();
    let touched_meta: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(proj_exprs, join)?);

    let touched_adds: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::from_log_scan(
        touched_meta,
        table_url.clone(),
        version,
        partition_columns.to_vec(),
        true, // partition_scan
    )?);

    Ok(touched_adds)
}
