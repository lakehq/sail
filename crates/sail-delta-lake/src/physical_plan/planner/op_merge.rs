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

use datafusion::common::{internal_err, DataFusionError, JoinType, NullEquality, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{
    MergeInfo as PhysicalMergeInfo, MergePredicateInfo, OperationOverride, PhysicalSinkMode,
};
use url::Url;

use super::context::PlannerContext;
use crate::datasource::{DataFusionMixins, PATH_COLUMN};
use crate::kernel::{DeltaOperation, MergePredicate};
use crate::options::TableDeltaOptions;
use crate::physical_plan::{
    DeltaCommitExec, DeltaFindFilesExec, DeltaRemoveActionsExec, DeltaWriterExec, COL_PATH,
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
        expanded,
        ctx.table_url().clone(),
        version,
        options,
        partition_columns,
        table_schema,
        merge_info.touched_file_plan.clone(),
        merge_operation,
    )
    .await
}

async fn finalize_merge(
    projected: Arc<dyn ExecutionPlan>,
    table_url: Url,
    version: i64,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    table_schema: datafusion::arrow::datatypes::SchemaRef,
    touched_file_plan: Option<Arc<dyn ExecutionPlan>>,
    operation_override: Option<DeltaOperation>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let writer = Arc::new(DeltaWriterExec::new(
        Arc::clone(&projected),
        table_url.clone(),
        options,
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        true,
        table_schema.clone(),
        operation_override,
    ));

    let mut action_inputs: Vec<Arc<dyn ExecutionPlan>> = vec![writer.clone()];

    if let Some(touched_plan) = touched_file_plan {
        // Build a log-side stream of Add rows.
        let log_adds: Arc<dyn ExecutionPlan> = Arc::new(DeltaFindFilesExec::new(
            table_url.clone(),
            None, // no predicate
            None, // schema not needed without predicate
            version,
        ));

        // touched_paths JOIN log_adds ON path
        let left_idx = touched_plan.schema().index_of(PATH_COLUMN)?;
        let right_idx = log_adds.schema().index_of(COL_PATH)?;
        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = vec![(
            Arc::new(Column::new(PATH_COLUMN, left_idx)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new(COL_PATH, right_idx)) as Arc<dyn PhysicalExpr>,
        )];
        let joined: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            touched_plan,
            log_adds,
            on,
            None, // filter
            &JoinType::Inner,
            None, // projection
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Convert joined Add rows -> Remove action rows.
        let remove_plan = Arc::new(DeltaRemoveActionsExec::new(joined));
        action_inputs.push(remove_plan);
    }

    let commit_input: Arc<dyn ExecutionPlan> = if action_inputs.len() == 1 {
        writer
    } else {
        UnionExec::try_new(action_inputs)?
    };

    let commit = Arc::new(DeltaCommitExec::new(
        commit_input,
        table_url,
        partition_columns,
        true, // table exists
        table_schema,
        PhysicalSinkMode::Append,
    ));

    Ok(commit)
}
