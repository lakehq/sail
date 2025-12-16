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
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{MergeInfo as PhysicalMergeInfo, PhysicalSinkMode};
use url::Url;

use super::context::PlannerContext;
use crate::datasource::DataFusionMixins;
use crate::options::TableDeltaOptions;
use crate::physical_plan::{
    DeltaCommitExec, DeltaFileLookupExec, DeltaRemoveActionsExec, DeltaWriterExec,
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

    finalize_merge(
        expanded,
        ctx.table_url().clone(),
        version,
        options,
        partition_columns,
        table_schema,
        merge_info.touched_file_plan.clone(),
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
) -> Result<Arc<dyn ExecutionPlan>> {
    let writer = Arc::new(DeltaWriterExec::new(
        Arc::clone(&projected),
        table_url.clone(),
        options,
        partition_columns.clone(),
        PhysicalSinkMode::Append,
        true,
        table_schema.clone(),
        None,
    ));

    let mut action_inputs: Vec<Arc<dyn ExecutionPlan>> = vec![writer.clone()];

    if let Some(touched_plan) = touched_file_plan {
        // Convert Path stream -> Add(JSON) stream on the Worker.
        let lookup_plan = Arc::new(DeltaFileLookupExec::new(
            touched_plan,
            table_url.clone(),
            version,
        ));

        // Convert Add(JSON) -> Remove actions commit info.
        let remove_plan = Arc::new(DeltaRemoveActionsExec::new(lookup_plan));
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
