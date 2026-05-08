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
use datafusion::common::{internal_err, DataFusionError, Result};
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
use datafusion_common::{not_impl_err, JoinType, NullEquality};
use datafusion_physical_expr::expressions::{Column, IsNullExpr};
use sail_common_datafusion::datasource::{
    MergePredicateInfo, OperationOverride, PhysicalSinkMode, RowLevelWriteInfo,
};

use super::super::writer_options::DeltaWriterExecOptions;
use super::commit::{
    assemble_commit_plan, build_adds_from_touched_files, build_remove_from_touched_files,
};
use super::context::PlannerContext;
use super::utils::LogReplayOptions;
use crate::datasource::PATH_COLUMN;
use crate::kernel::{DeltaOperation, MergePredicate};
use crate::physical_plan::{DeltaCommitExec, DeltaWriterExec};

/// Internal metadata columns stripped before passing rows to DeltaWriterExec.
const INTERNAL_MERGE_COLUMNS: &[&str] = &[
    PATH_COLUMN,
    sail_common_datafusion::datasource::OPERATION_COLUMN,
];

/// Entry point for MERGE execution. Expects the logical MERGE to be fully
/// expanded (handled by ExpandRowLevelOp) and passed down as pre-expanded plans.
pub async fn build_merge_plan(
    ctx: &PlannerContext<'_>,
    merge_info: RowLevelWriteInfo,
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
    if merge_info.with_schema_evolution {
        options.merge_schema = true;
    }

    let expanded = merge_info.expanded_input.clone().ok_or_else(|| {
        DataFusionError::Plan("pre-expanded MERGE plan missing expanded input".to_string())
    })?;

    let merge_operation = build_merge_operation(&merge_info);

    let touched_plan_opt = merge_info.touched_file_plan.clone();

    // Targeted rewrite: if we have a touched file plan, restrict the writer input to:
    // - rows from touched files (post-merge)
    // - inserted rows (path is NULL)
    //
    // Untouched files remain as-is (not removed, not rewritten).
    let writer_input: Arc<dyn ExecutionPlan> = if let Some(touched_plan) = &touched_plan_opt {
        build_targeted_writer_input(&expanded, touched_plan)?
    } else {
        Arc::clone(&expanded)
    };

    // DeltaWriterExec expects rows to match the target table schema. Drop the internal
    // merge metadata columns (file path, operation type) after using them for targeted
    // rewrite filtering.
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

    assemble_commit_plan(
        writer_input,
        remove_source,
        ctx.table_url().clone(),
        options,
        ctx.metadata_configuration().clone(),
        partition_columns,
        true, // table exists
        table_schema,
        merge_operation,
        ctx.options().user_metadata.clone(),
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

    let expanded = merge_info.expanded_input.clone().ok_or_else(|| {
        DataFusionError::Plan("pre-expanded MERGE plan missing expanded input".to_string())
    })?;
    let merge_operation = build_merge_operation(&merge_info);

    let deletion_vector_plan = merge_info.deletion_vector_plan.clone();
    let touched_plan_opt = merge_info.touched_file_plan.clone();

    let writer_input = if deletion_vector_plan.is_some() {
        build_insert_rows_input(&expanded)?
    } else {
        Arc::clone(&expanded)
    };
    let writer_input = strip_internal_columns(writer_input)?;

    let writer: Arc<dyn ExecutionPlan> = Arc::new(DeltaWriterExec::new(
        writer_input,
        ctx.table_url().clone(),
        options,
        ctx.metadata_configuration().clone(),
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        true,
        table_schema.clone(),
        merge_operation.clone(),
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
                    sail_common_datafusion::datasource::MERGE_ROW_INDEX_COLUMN,
                    version,
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

/// Build targeted writer input for Copy-on-Write MERGE.
///
/// Filters the expanded plan to include only:
/// - Insert rows (path is NULL) — new rows not in any existing file
/// - Touched rows (inner join with touched files) — rows from files being rewritten
fn build_targeted_writer_input(
    expanded: &Arc<dyn ExecutionPlan>,
    touched_plan: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Physical plans can hold runtime state after execution. MERGE branches this subtree,
    // so each consumer needs its own reset copy rather than sharing a multi-parent DAG.
    let projected_for_touched = reset_plan_states(Arc::clone(expanded))?;
    let touched_plan_for_writer = reset_plan_states(Arc::clone(touched_plan))?;
    let projected_schema = expanded.schema();
    if projected_schema.column_with_name(PATH_COLUMN).is_none() {
        return internal_err!(
            "MERGE writer input is missing required column '{PATH_COLUMN}' for targeted rewrite"
        );
    }
    if touched_plan
        .schema()
        .column_with_name(PATH_COLUMN)
        .is_none()
    {
        return internal_err!("MERGE touched file plan is missing required column '{PATH_COLUMN}'");
    }

    // Insert rows: path is NULL.
    let path_idx = projected_schema
        .index_of(PATH_COLUMN)
        .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
    let insert_pred: Arc<dyn datafusion_physical_expr::PhysicalExpr> = Arc::new(IsNullExpr::new(
        Arc::new(Column::new(PATH_COLUMN, path_idx)),
    ));
    let insert_rows: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(insert_pred, Arc::clone(expanded))?);

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

    UnionExec::try_new(vec![insert_rows, touched_rows])
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

/// Strip internal merge metadata columns (file path, operation type) from the writer input.
fn strip_internal_columns(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let has_internal = INTERNAL_MERGE_COLUMNS
        .iter()
        .any(|col| schema.column_with_name(col).is_some());
    if has_internal {
        let proj_exprs = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !INTERNAL_MERGE_COLUMNS.contains(&f.name().as_str()))
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

/// Convert `OperationOverride` to `DeltaOperation::Merge`.
fn build_merge_operation(info: &RowLevelWriteInfo) -> Option<DeltaOperation> {
    let OperationOverride::Merge {
        predicate,
        merge_predicate,
        matched_predicates,
        not_matched_predicates,
        not_matched_by_source_predicates,
    } = info.operation_override.as_ref()?;

    let to_kernel_preds = |preds: &[MergePredicateInfo]| -> Vec<MergePredicate> {
        preds
            .iter()
            .map(|p| MergePredicate {
                action_type: p.action_type.clone(),
                predicate: p.predicate.clone(),
            })
            .collect()
    };

    Some(DeltaOperation::Merge {
        predicate: predicate.clone(),
        merge_predicate: merge_predicate.clone(),
        matched_predicates: to_kernel_preds(matched_predicates),
        not_matched_predicates: to_kernel_preds(not_matched_predicates),
        not_matched_by_source_predicates: to_kernel_preds(not_matched_by_source_predicates),
    })
}

fn merge_has_update_actions(info: &RowLevelWriteInfo) -> bool {
    let Some(OperationOverride::Merge {
        matched_predicates,
        not_matched_by_source_predicates,
        ..
    }) = info.operation_override.as_ref()
    else {
        return false;
    };

    matched_predicates
        .iter()
        .chain(not_matched_by_source_predicates)
        .any(|p| p.action_type.eq_ignore_ascii_case("update"))
}

fn merge_has_delete_actions(info: &RowLevelWriteInfo) -> bool {
    let Some(OperationOverride::Merge {
        matched_predicates,
        not_matched_by_source_predicates,
        ..
    }) = info.operation_override.as_ref()
    else {
        return false;
    };

    matched_predicates
        .iter()
        .chain(not_matched_by_source_predicates)
        .any(|p| p.action_type.eq_ignore_ascii_case("delete"))
}
