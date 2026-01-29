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

use datafusion::common::{internal_err, DataFusionError, Result};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{JoinType, NullEquality};
use datafusion_physical_expr::expressions::{Column, IsNullExpr};
use sail_common_datafusion::datasource::{
    MergeInfo as PhysicalMergeInfo, MergePredicateInfo, OperationOverride, PhysicalSinkMode,
};
use url::Url;

use super::context::PlannerContext;
use super::utils::build_log_replay_pipeline;
use crate::datasource::{DataFusionMixins, PATH_COLUMN};
use crate::kernel::{DeltaOperation, MergePredicate};
use crate::options::TableDeltaOptions;
use crate::physical_plan::{
    DeltaCommitExec, DeltaDiscoveryExec, DeltaRemoveActionsExec, DeltaWriterExec,
};

/// Entry point for MERGE execution. Expects the logical MERGE to be fully
/// expanded (handled by ExpandMergeRule) and passed down as pre-expanded plans.
pub async fn build_merge_plan(
    ctx: &PlannerContext<'_>,
    merge_info: PhysicalMergeInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !merge_info.pre_expanded {
        return internal_err!(
            "MERGE planning expects a pre-expanded logical plan. Ensure expand_merge is enabled."
        );
    }

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

    let mut options = ctx.options().clone();
    if merge_info.with_schema_evolution {
        options.merge_schema = true;
    }

    let expanded = merge_info.expanded_input.clone().ok_or_else(|| {
        DataFusionError::Plan("pre-expanded MERGE plan missing expanded input".to_string())
    })?;

    let merge_operation = match merge_info.operation_override.as_ref() {
        None => None,
        Some(OperationOverride::Merge {
            predicate,
            merge_predicate,
            matched_predicates,
            not_matched_predicates,
            not_matched_by_source_predicates,
        }) => {
            let to_kernel_preds = |preds: &Vec<MergePredicateInfo>| -> Vec<MergePredicate> {
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
    };
    finalize_merge(
        ctx,
        expanded,
        ctx.table_url().clone(),
        version,
        options,
        partition_columns,
        table_schema,
        merge_info.touched_file_plan.clone(),
        checkpoint_files,
        commit_files,
        merge_operation,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finalize_merge(
    ctx: &PlannerContext<'_>,
    projected: Arc<dyn ExecutionPlan>,
    table_url: Url,
    version: i64,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    table_schema: datafusion::arrow::datatypes::SchemaRef,
    touched_file_plan: Option<Arc<dyn ExecutionPlan>>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    operation_override: Option<DeltaOperation>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let touched_plan_opt = touched_file_plan;

    // Targeted rewrite: if we have a touched file plan, restrict the writer input to:
    // - rows from touched files (post-merge)
    // - inserted rows (path is NULL)
    //
    // Untouched files remain as-is (not removed, not rewritten).
    let writer_input: Arc<dyn ExecutionPlan> = if let Some(touched_plan) = &touched_plan_opt {
        let projected_schema = projected.schema();
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
            return internal_err!(
                "MERGE touched file plan is missing required column '{PATH_COLUMN}'"
            );
        }

        // Insert rows: path is NULL.
        let path_idx = projected_schema
            .index_of(PATH_COLUMN)
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        let insert_pred: Arc<dyn datafusion_physical_expr::PhysicalExpr> = Arc::new(
            IsNullExpr::new(Arc::new(Column::new(PATH_COLUMN, path_idx))),
        );
        let insert_rows: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(insert_pred, Arc::clone(&projected))?);

        // Touched rows: inner join touched_paths (small, collected) with writer input (big).
        let touched_schema = touched_plan.schema();
        let touched_idx = touched_schema
            .index_of(PATH_COLUMN)
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;

        let join = Arc::new(HashJoinExec::try_new(
            Arc::clone(touched_plan),
            Arc::clone(&projected),
            vec![(
                Arc::new(Column::new(PATH_COLUMN, touched_idx)),
                Arc::new(Column::new(PATH_COLUMN, path_idx)),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
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
        let touched_rows: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_exprs, join)?);

        UnionExec::try_new(vec![insert_rows, touched_rows])?
    } else {
        Arc::clone(&projected)
    };

    // DeltaWriterExec expects rows to match the target table schema. Drop the internal path column
    // after using it for targeted rewrite filtering.
    let writer_input: Arc<dyn ExecutionPlan> = if writer_input
        .schema()
        .column_with_name(PATH_COLUMN)
        .is_some()
    {
        let schema = writer_input.schema();
        let proj_exprs = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != PATH_COLUMN)
            .map(|(i, f)| {
                (
                    Arc::new(Column::new(f.name(), i))
                        as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                    f.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        Arc::new(ProjectionExec::try_new(proj_exprs, writer_input)?)
    } else {
        writer_input
    };

    let writer = Arc::new(DeltaWriterExec::new(
        writer_input,
        table_url.clone(),
        options,
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        true,
        table_schema.clone(),
        operation_override,
    )?);

    let mut action_inputs: Vec<Arc<dyn ExecutionPlan>> = vec![writer.clone()];

    if let Some(touched_plan) = &touched_plan_opt {
        // Build a log-side stream of active Add rows using a visible log replay pipeline:
        // Union(DataSourceExec parquet/json) -> DeltaLogReplayExec -> ... -> DeltaDiscoveryExec.
        let meta_scan: Arc<dyn ExecutionPlan> = build_log_replay_pipeline(
            ctx,
            table_url.clone(),
            version,
            partition_columns.clone(),
            checkpoint_files,
            commit_files,
        )
        .await?;

        // Restrict to touched file paths by joining touched_paths with the metadata stream.
        let touched_schema = touched_plan.schema();
        let touched_idx = touched_schema
            .index_of(PATH_COLUMN)
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        let meta_schema = meta_scan.schema();
        let meta_idx = meta_schema
            .index_of(PATH_COLUMN)
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;

        let join = Arc::new(HashJoinExec::try_new(
            Arc::clone(touched_plan),
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
        )?);

        // Keep only the right side columns (metadata stream schema).
        let left_cols = touched_schema.fields().len();
        let proj_exprs = meta_schema
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
        let touched_meta: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_exprs, join)?);

        let touched_adds: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::from_log_scan(
            touched_meta,
            table_url.clone(),
            version,
            partition_columns.clone(),
            true, // partition_scan
        )?);

        // Convert Add rows -> Remove action rows (touched-only).
        let remove_plan = Arc::new(DeltaRemoveActionsExec::new(touched_adds)?);
        action_inputs.push(remove_plan);
    }

    let commit_input: Arc<dyn ExecutionPlan> = if action_inputs.len() == 1 {
        writer
    } else {
        UnionExec::try_new(action_inputs)?
    };

    let commit = Arc::new(DeltaCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(commit_input)),
        table_url,
        partition_columns,
        true, // table exists
        table_schema,
        PhysicalSinkMode::Append,
    ));

    Ok(commit)
}
