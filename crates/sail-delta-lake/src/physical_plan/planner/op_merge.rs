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

use datafusion::arrow::compute::SortOptions;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_common::{JoinType, NullEquality, not_impl_err};
use datafusion_physical_expr::expressions::{Column, IsNullExpr};
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{OptionLayer, PhysicalSinkMode, RowLevelCommand};
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::super::writer_options::DeltaWriterExecOptions;
use super::commit::{
    assemble_commit_plan, build_adds_from_touched_files, build_remove_from_touched_files,
};
use super::context::PlannerContext;
use super::utils::LogReplayOptions;
use crate::datasource::PATH_COLUMN;
use crate::physical_plan::{DeltaCommitExec, DeltaWriterExec, prepare_delta_write_context};
use crate::spec::{DeltaOperation, MergePredicate};

/// Target table information shared by Delta row-level operations.
#[derive(Debug, Clone)]
pub struct RowLevelTargetInfo {
    pub table_name: Vec<String>,
    pub path: String,
    pub partition_by: Vec<String>,
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

/// Operation metadata used to construct MERGE commit log `operationParameters`.
#[derive(Debug, Clone)]
pub struct MergePredicateInfo {
    pub action_type: String,
    pub predicate: Option<String>,
}

/// Delta commit metadata for a MERGE operation.
#[derive(Debug, Clone)]
pub struct MergeCommitInfo {
    pub predicate: Option<String>,
    pub merge_predicate: Option<String>,
    pub matched_predicates: Vec<MergePredicateInfo>,
    pub not_matched_predicates: Vec<MergePredicateInfo>,
    pub not_matched_by_source_predicates: Vec<MergePredicateInfo>,
}

/// Format-level operation metadata consumed by Delta row-level planning.
#[derive(Debug, Clone)]
pub enum DeltaRowLevelOperation {
    Delete { condition: Option<ExprWithSource> },
    Update { condition: Option<ExprWithSource> },
    Merge(MergeCommitInfo),
}

impl DeltaRowLevelOperation {
    pub fn command(&self) -> RowLevelCommand {
        match self {
            Self::Delete { .. } => RowLevelCommand::Delete,
            Self::Update { .. } => RowLevelCommand::Update,
            Self::Merge(_) => RowLevelCommand::Merge,
        }
    }

    pub fn condition(&self) -> Option<&ExprWithSource> {
        match self {
            Self::Delete { condition } | Self::Update { condition } => condition.as_ref(),
            Self::Merge(_) => None,
        }
    }

    fn merge(&self) -> Option<&MergeCommitInfo> {
        match self {
            Self::Merge(info) => Some(info),
            Self::Delete { .. } | Self::Update { .. } => None,
        }
    }
}

/// Unified information for Delta row-level write operations (DELETE, UPDATE, MERGE).
#[derive(Debug, Clone)]
pub struct RowLevelWriteInfo {
    pub operation: DeltaRowLevelOperation,
    pub target: RowLevelTargetInfo,
    /// Pre-expanded physical plan containing row actions and output values.
    pub expanded_input: Arc<dyn ExecutionPlan>,
    /// Physical plan that yields touched file paths for targeted rewrites.
    pub touched_file_plan: Option<Arc<dyn ExecutionPlan>>,
    /// Physical plan that yields target file path and file-local row index rows to delete via DVs.
    pub deletion_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    pub with_schema_evolution: bool,
}

// TODO: MERGE schema evolution end-to-end
// - Expand sink schema during MERGE: detect source-only columns (case-insensitive), keep target order, append new cols, project source/NULL for them.
// - Emit Metadata (and Protocol if required) in writer/commit so the new schema is persisted and readable.
// - Reading: time-travel must stay on the requested version; non-time-travel can refresh to latest snapshot to see new schema.

/// Internal metadata columns stripped before passing rows to DeltaWriterExec.
///
/// Action and metric columns remain available to `DeltaWriterExec` until it has
/// routed rows and recorded operation metrics.
const INTERNAL_ROW_LEVEL_COLUMNS: &[&str] = &[PATH_COLUMN];

/// Entry point for MERGE execution. Expects the logical MERGE to be fully
/// expanded during Delta logical MERGE planning and passed down as pre-expanded plans.
pub async fn build_merge_plan(
    ctx: &PlannerContext<'_>,
    merge_info: RowLevelWriteInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    build_row_level_rewrite_plan(ctx, merge_info).await
}

/// Copy-on-Write assembly for operations with pre-expanded row and touched-file plans.
pub(crate) async fn build_row_level_rewrite_plan(
    ctx: &PlannerContext<'_>,
    row_level_info: RowLevelWriteInfo,
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

    let mut options = DeltaWriterExecOptions::from(ctx.options().clone());
    if row_level_info.with_schema_evolution {
        options.merge_schema = true;
    }

    let expanded = Arc::clone(&row_level_info.expanded_input);
    let keeps_pathless_rows = matches!(&row_level_info.operation, DeltaRowLevelOperation::Merge(_));
    let operation = Some(build_row_level_operation(&row_level_info.operation));

    let touched_plan_opt = row_level_info.touched_file_plan.clone();

    // Targeted rewrite: if we have a touched file plan, restrict the writer input to:
    // - rows from touched files (post-merge)
    // - inserted rows (path is NULL)
    //
    // Untouched files remain as-is (not removed, not rewritten).
    let writer_input: Arc<dyn ExecutionPlan> = if let Some(touched_plan) = &touched_plan_opt {
        build_targeted_writer_input(&expanded, touched_plan, keeps_pathless_rows)?
    } else {
        Arc::clone(&expanded)
    };

    // DeltaWriterExec consumes operation/metric columns for MERGE metrics. Drop only
    // metadata already used for targeted rewrite before handing rows to the writer.
    let writer_input: Arc<dyn ExecutionPlan> = strip_internal_columns(writer_input)?;

    // Build the remove source from the touched files, if any.
    let remove_source = if let Some(touched_plan) = &touched_plan_opt {
        let touched_for_remove = reset_plan_states(Arc::clone(touched_plan))?;
        Some(
            build_remove_from_touched_files(
                ctx,
                &snapshot_state,
                touched_for_remove,
                ctx.table_url(),
                version,
                &partition_columns,
            )
            .await?,
        )
    } else {
        None
    };

    let write_context = prepare_delta_write_context(
        ctx.table_url(),
        Some(snapshot_state.as_ref()),
        &options,
        ctx.metadata_configuration(),
        &partition_columns,
        &PhysicalSinkMode::Append,
        true,
        &writer_input.schema(),
        operation.clone(),
    )?;

    assemble_commit_plan(
        writer_input,
        remove_source,
        Some(snapshot_state.physical_partition_columns()),
        ctx.table_url().clone(),
        options,
        ctx.metadata_configuration().clone(),
        partition_columns,
        true, // table exists
        table_schema,
        ctx.options().user_metadata.clone(),
        write_context,
        ctx.lakehouse_table().cloned(),
    )
}

/// Merge-on-Read MERGE using deletion vectors for target DELETE clauses.
///
/// UPDATE clauses are intentionally rejected for now: they require writing changed rows
/// while deleting the original target rows via DVs, which needs a separate "changed rows
/// only" MERGE projection. INSERT-only and DELETE+INSERT MERGE are supported.
pub async fn build_merge_plan_mor(
    ctx: &PlannerContext<'_>,
    merge_info: RowLevelWriteInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    if merge_has_update_actions(&merge_info) {
        return not_impl_err!(
            "Merge-on-Read strategy for MERGE UPDATE clauses is not yet implemented for Delta Lake"
        );
    }
    if merge_has_delete_actions(&merge_info) && merge_info.deletion_vector_plan.is_none() {
        return internal_err!(
            "Merge-on-Read MERGE DELETE clauses require file-local row-index metadata"
        );
    }

    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    let version = snapshot_state.version();
    snapshot_state
        .verify_deletion_vectors()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let table_schema = snapshot_state
        .input_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();

    let mut options = DeltaWriterExecOptions::from(ctx.options().clone());
    if merge_info.with_schema_evolution {
        options.merge_schema = true;
    }

    let expanded = Arc::clone(&merge_info.expanded_input);
    let merge_operation = Some(build_merge_operation(
        merge_info.operation.merge().ok_or_else(|| {
            DataFusionError::Internal("MERGE planner received a non-MERGE operation".to_string())
        })?,
    ));

    let deletion_vector_plan = merge_info.deletion_vector_plan.clone();
    let touched_plan_opt = merge_info.touched_file_plan.clone();

    let writer_input = if deletion_vector_plan.is_some() {
        build_insert_rows_input(&expanded)?
    } else {
        Arc::clone(&expanded)
    };
    let writer_input = strip_internal_columns(writer_input)?;
    let writer_schema = writer_input.schema();
    let write_context = prepare_delta_write_context(
        ctx.table_url(),
        Some(snapshot_state.as_ref()),
        &options,
        ctx.metadata_configuration(),
        &partition_columns,
        &PhysicalSinkMode::Append,
        true,
        &writer_schema,
        merge_operation.clone(),
    )?;

    let writer: Arc<dyn ExecutionPlan> = Arc::new(DeltaWriterExec::new(
        writer_input,
        ctx.table_url().clone(),
        options,
        ctx.metadata_configuration().clone(),
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        true,
        table_schema.clone(),
        write_context.clone(),
        ctx.lakehouse_table().cloned(),
    )?);

    let commit_input: Arc<dyn ExecutionPlan> =
        if let Some(deletion_vector_plan) = deletion_vector_plan {
            let touched_plan = touched_plan_opt.ok_or_else(|| {
                DataFusionError::Plan(
                    "pre-expanded MERGE plan missing touched-file input for deletion vectors"
                        .to_string(),
                )
            })?;
            let touched_adds = build_adds_from_touched_files(
                ctx,
                &snapshot_state,
                touched_plan,
                ctx.table_url(),
                version,
                &partition_columns,
                LogReplayOptions {
                    include_extended_add_metadata: true,
                    ..Default::default()
                },
            )
            .await?;
            let target_partitions = ctx.session().config().target_partitions().max(1);
            let deletion_vector_plan =
                hash_repartition_by_column(deletion_vector_plan, PATH_COLUMN, target_partitions)?;
            let deletion_vector_plan =
                sort_by_column_preserving_partitioning(deletion_vector_plan, PATH_COLUMN)?;
            let touched_adds =
                hash_repartition_by_column(touched_adds, PATH_COLUMN, target_partitions)?;
            let dv_writer: Arc<dyn ExecutionPlan> =
                Arc::new(crate::physical_plan::DeletionVectorRowsWriterExec::new(
                    deletion_vector_plan,
                    touched_adds,
                    ctx.table_url().clone(),
                    PATH_COLUMN,
                    sail_common_datafusion::datasource::ROW_LEVEL_ROW_INDEX_COLUMN,
                    version,
                    Some(snapshot_state.physical_partition_columns()),
                    merge_operation,
                )?);
            UnionExec::try_new(vec![writer, dv_writer])?
        } else {
            writer
        };

    Ok(Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(commit_input)),
        ctx.table_url().clone(),
        partition_columns,
        true,
        table_schema,
        PhysicalSinkMode::Append,
        ctx.options().user_metadata.clone(),
        write_context.commit_context.clone(),
        ctx.lakehouse_table().cloned(),
    )))
}

fn hash_repartition_by_column(
    input: Arc<dyn ExecutionPlan>,
    column_name: &str,
    partition_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let idx = input
        .schema()
        .index_of(column_name)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
    let expr: Arc<dyn datafusion_physical_expr::PhysicalExpr> =
        Arc::new(Column::new(column_name, idx));
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(vec![expr], partition_count.max(1)),
    )?))
}

fn sort_by_column_preserving_partitioning(
    input: Arc<dyn ExecutionPlan>,
    column_name: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    let idx = input
        .schema()
        .index_of(column_name)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
    let ordering = LexOrdering::new(vec![PhysicalSortExpr {
        expr: Arc::new(Column::new(column_name, idx)),
        options: SortOptions {
            descending: false,
            nulls_first: false,
        },
    }])
    .ok_or_else(|| {
        DataFusionError::Internal("failed to create MERGE deletion-vector ordering".to_string())
    })?;
    Ok(Arc::new(
        SortExec::new(ordering, input).with_preserve_partitioning(true),
    ))
}

/// Build targeted writer input for a Copy-on-Write row-level operation.
///
/// The result contains rows from touched files and, when requested, pathless
/// insert or metric rows.
fn build_targeted_writer_input(
    expanded: &Arc<dyn ExecutionPlan>,
    touched_plan: &Arc<dyn ExecutionPlan>,
    keeps_pathless_rows: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Physical plans can hold runtime state after execution. Each consumer needs
    // its own reset copy rather than sharing a multi-parent DAG.
    let projected_for_touched = reset_plan_states(Arc::clone(expanded))?;
    let touched_plan_for_writer = reset_plan_states(Arc::clone(touched_plan))?;
    let projected_schema = expanded.schema();
    if projected_schema.column_with_name(PATH_COLUMN).is_none() {
        return internal_err!(
            "row-level writer input is missing required column '{PATH_COLUMN}' for targeted rewrite"
        );
    }
    if touched_plan
        .schema()
        .column_with_name(PATH_COLUMN)
        .is_none()
    {
        return internal_err!(
            "row-level touched file plan is missing required column '{PATH_COLUMN}'"
        );
    }

    let path_idx = projected_schema
        .index_of(PATH_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;

    // Touched rows: inner join touched_paths (small, collected) with writer input (big).
    let touched_schema = touched_plan.schema();
    let touched_idx = touched_schema
        .index_of(PATH_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;

    let join = Arc::new(HashJoinExec::try_new(
        touched_plan_for_writer,
        projected_for_touched,
        vec![(
            Arc::new(Column::new(PATH_COLUMN, touched_idx)),
            Arc::new(Column::new(PATH_COLUMN, path_idx)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    // Keep only the right side columns (original writer input schema) after join.
    let left_cols = touched_schema.fields().len();
    let proj_exprs = projected_schema
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
        .collect::<Vec<_>>();
    let touched_rows: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(proj_exprs, join)?);

    if keeps_pathless_rows {
        let insert_pred: Arc<dyn datafusion_physical_expr::PhysicalExpr> = Arc::new(
            IsNullExpr::new(Arc::new(Column::new(PATH_COLUMN, path_idx))),
        );
        let insert_rows: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(insert_pred, Arc::clone(expanded))?);
        UnionExec::try_new(vec![insert_rows, touched_rows])
    } else {
        Ok(touched_rows)
    }
}

/// Build MERGE MoR writer input for source-only INSERT rows.
fn build_insert_rows_input(expanded: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let projected_schema = expanded.schema();
    if projected_schema.column_with_name(PATH_COLUMN).is_none() {
        return internal_err!(
            "MERGE writer input is missing required column '{PATH_COLUMN}' for insert filtering"
        );
    }

    let path_idx = projected_schema
        .index_of(PATH_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
    let insert_pred: Arc<dyn datafusion_physical_expr::PhysicalExpr> = Arc::new(IsNullExpr::new(
        Arc::new(Column::new(PATH_COLUMN, path_idx)),
    ));
    Ok(Arc::new(FilterExec::try_new(
        insert_pred,
        Arc::clone(expanded),
    )?))
}

/// Strip row-level metadata columns already consumed by the physical planner.
fn strip_internal_columns(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let has_internal = INTERNAL_ROW_LEVEL_COLUMNS
        .iter()
        .any(|col| schema.column_with_name(col).is_some());
    if has_internal {
        let proj_exprs = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !INTERNAL_ROW_LEVEL_COLUMNS.contains(&f.name().as_str()))
            .map(|(i, f)| {
                (
                    Arc::new(Column::new(f.name(), i))
                        as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                    f.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
    } else {
        Ok(input)
    }
}

fn build_row_level_operation(operation: &DeltaRowLevelOperation) -> DeltaOperation {
    match operation {
        DeltaRowLevelOperation::Delete { condition } => DeltaOperation::Delete {
            predicate: condition
                .as_ref()
                .and_then(|condition| condition.source.clone()),
        },
        DeltaRowLevelOperation::Update { condition } => DeltaOperation::Update {
            predicate: condition
                .as_ref()
                .and_then(|condition| condition.source.clone()),
        },
        DeltaRowLevelOperation::Merge(info) => build_merge_operation(info),
    }
}

fn build_merge_operation(info: &MergeCommitInfo) -> DeltaOperation {
    let to_kernel_preds = |predicates: &[MergePredicateInfo]| -> Vec<MergePredicate> {
        predicates
            .iter()
            .map(|predicate| MergePredicate {
                action_type: predicate.action_type.clone(),
                predicate: predicate.predicate.clone(),
            })
            .collect()
    };

    DeltaOperation::Merge {
        predicate: info.predicate.clone(),
        merge_predicate: info.merge_predicate.clone(),
        matched_predicates: to_kernel_preds(&info.matched_predicates),
        not_matched_predicates: to_kernel_preds(&info.not_matched_predicates),
        not_matched_by_source_predicates: to_kernel_preds(&info.not_matched_by_source_predicates),
    }
}

fn merge_has_update_actions(info: &RowLevelWriteInfo) -> bool {
    let Some(info) = info.operation.merge() else {
        return false;
    };

    info.matched_predicates
        .iter()
        .chain(&info.not_matched_by_source_predicates)
        .any(|predicate| predicate.action_type.eq_ignore_ascii_case("update"))
}

fn merge_has_delete_actions(info: &RowLevelWriteInfo) -> bool {
    let Some(info) = info.operation.merge() else {
        return false;
    };

    info.matched_predicates
        .iter()
        .chain(&info.not_matched_by_source_predicates)
        .any(|predicate| predicate.action_type.eq_ignore_ascii_case("delete"))
}
