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
//! - [`build_adds_from_touched_files`]: joins a touched-file plan with the log replay
//!   pipeline to produce the Add-action stream consumed by row-level writers.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::{JoinType, NullEquality};
use datafusion_physical_expr::expressions::Column;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::context::PlannerContext;
use super::utils::{LogReplayOptions, build_log_replay_pipeline_with_options};
use crate::datasource::PATH_COLUMN;
use crate::physical_plan::{
    DeltaCommitExec, DeltaDecodePath, DeltaDiscoveryExec, DeltaRemoveActionsExec,
    DeltaWriteContext, DeltaWriterExec, DeltaWriterExecOptions,
};
use crate::schema::PhysicalPartitionColumn;
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
    remove_partition_value_columns: Option<Vec<PhysicalPartitionColumn>>,
    table_url: Url,
    options: DeltaWriterExecOptions,
    metadata_configuration: HashMap<String, String>,
    partition_columns: Vec<String>,
    table_exists: bool,
    table_schema: SchemaRef,
    user_metadata: Option<String>,
    write_context: DeltaWriteContext,
    lakehouse_table: Option<LakehouseExecutionContext>,
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
        write_context.clone(),
        lakehouse_table.clone(),
    )?);

    let commit_input: Arc<dyn ExecutionPlan> = if let Some(remove_src) = remove_source {
        let remover: Arc<dyn ExecutionPlan> = Arc::new(DeltaRemoveActionsExec::try_new(
            remove_src,
            remove_partition_value_columns,
        )?);
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
        write_context.commit_context.clone(),
        lakehouse_table,
    )))
}

/// Build an Add-action metadata source from a set of touched file paths.
///
/// Joins the `touched_file_plan` (which yields `PATH_COLUMN` values for files that
/// were modified) with a log replay pipeline to retrieve the full Add-action metadata.
/// Touched paths must come from the target scan at the supplied snapshot.
pub async fn build_adds_from_touched_files(
    ctx: &PlannerContext<'_>,
    snapshot: &DeltaSnapshot,
    touched_file_plan: Arc<dyn ExecutionPlan>,
    table_url: &Url,
    version: i64,
    partition_columns: &[String],
    log_replay_options: LogReplayOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let touched_plan = reset_plan_states(touched_file_plan)?;

    let meta_scan: Arc<dyn ExecutionPlan> =
        build_log_replay_pipeline_with_options(ctx, snapshot, log_replay_options).await?;
    let touched_meta = join_touched_file_metadata(touched_plan, meta_scan)?;

    Ok(Arc::new(DeltaDiscoveryExec::new(
        touched_meta,
        table_url.clone(),
        version,
        partition_columns.to_vec(),
        true, // partition_scan
    )?))
}

fn join_touched_file_metadata(
    touched_plan: Arc<dyn ExecutionPlan>,
    meta_scan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let touched_schema = touched_plan.schema();
    let touched_idx = touched_schema
        .index_of(PATH_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
    let meta_schema = meta_scan.schema();
    // Scan paths are decoded; preserve the original log URI in the metadata payload.
    let decoded_meta_path = DeltaDecodePath::expression(PATH_COLUMN, meta_schema.as_ref())?;
    let join = Arc::new(HashJoinExec::try_new(
        touched_plan,
        meta_scan,
        vec![(
            Arc::new(Column::new(PATH_COLUMN, touched_idx)),
            decoded_meta_path,
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
    Ok(Arc::new(ProjectionExec::try_new(proj_exprs, join)?))
}

/// Build a remove-action source from a set of touched file paths.
///
/// The output is suitable for feeding into [`DeltaRemoveActionsExec`].
pub async fn build_remove_from_touched_files(
    ctx: &PlannerContext<'_>,
    snapshot: &DeltaSnapshot,
    touched_file_plan: Arc<dyn ExecutionPlan>,
    table_url: &Url,
    version: i64,
    partition_columns: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    build_adds_from_touched_files(
        ctx,
        snapshot,
        touched_file_plan,
        table_url,
        version,
        partition_columns,
        LogReplayOptions::default(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_common::cast::as_string_array;
    use datafusion_common::internal_datafusion_err;

    use super::*;

    fn path_metadata(paths: Vec<&str>) -> Result<Arc<dyn ExecutionPlan>> {
        let paths: ArrayRef = Arc::new(StringArray::from(paths));
        let batch = RecordBatch::try_from_iter(vec![(PATH_COLUMN, paths)])?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch.clone()]],
            batch.schema(),
            None,
        )?)
    }

    #[tokio::test]
    async fn touched_file_join_preserves_log_uri_payload() -> Result<()> {
        let session = SessionContext::new();
        let metadata = join_touched_file_metadata(
            path_metadata(vec!["p=a+b%20c/file.parquet"])?,
            path_metadata(vec!["p=a+b%2520c/file.parquet", "untouched.parquet"])?,
        )?;
        let batches = collect(metadata, session.task_ctx()).await?;
        let paths = batches
            .iter()
            .map(|batch| as_string_array(batch.column(0)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            paths
                .iter()
                .flat_map(|array| array.iter())
                .collect::<Vec<_>>(),
            vec![Some("p=a+b%2520c/file.parquet")]
        );
        Ok(())
    }

    #[test]
    fn touched_file_lookup_broadcasts_paths_without_duplicate_inputs() -> Result<()> {
        let touched_plan = path_metadata(vec!["active.parquet"])?;
        let meta_scan = path_metadata(vec!["active.parquet", "untouched.parquet"])?;
        let metadata =
            join_touched_file_metadata(Arc::clone(&touched_plan), Arc::clone(&meta_scan))?;
        let projection = metadata
            .downcast_ref::<ProjectionExec>()
            .ok_or_else(|| internal_datafusion_err!("expected metadata projection"))?;
        let lookup = projection
            .input()
            .downcast_ref::<HashJoinExec>()
            .ok_or_else(|| internal_datafusion_err!("expected metadata lookup"))?;
        assert_eq!(*lookup.join_type(), JoinType::Inner);
        assert_eq!(*lookup.partition_mode(), PartitionMode::CollectLeft);
        assert!(Arc::ptr_eq(lookup.left(), &touched_plan));
        assert!(Arc::ptr_eq(lookup.right(), &meta_scan));
        Ok(())
    }

    #[tokio::test]
    async fn touched_file_lookup_combines_metadata_partitions() -> Result<()> {
        let session = SessionContext::new();
        let partitions = [
            vec!["p=first%2520file.parquet", "untouched.parquet"],
            vec![],
            vec!["p=second%2520file.parquet"],
        ]
        .into_iter()
        .map(|paths| {
            let paths: ArrayRef = Arc::new(StringArray::from(paths));
            Ok(vec![RecordBatch::try_from_iter(vec![(
                PATH_COLUMN,
                paths,
            )])?])
        })
        .collect::<Result<Vec<_>>>()?;
        let meta_scan =
            MemorySourceConfig::try_new_exec(&partitions, partitions[0][0].schema(), None)?;
        let metadata = join_touched_file_metadata(
            path_metadata(vec!["p=first%20file.parquet", "p=second%20file.parquet"])?,
            meta_scan,
        )?;
        let batches = collect(metadata, session.task_ctx()).await?;
        let mut paths = batches
            .iter()
            .map(|batch| as_string_array(batch.column(0)))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flat_map(|array| array.iter())
            .collect::<Vec<_>>();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                Some("p=first%2520file.parquet"),
                Some("p=second%2520file.parquet"),
            ]
        );
        Ok(())
    }
}
