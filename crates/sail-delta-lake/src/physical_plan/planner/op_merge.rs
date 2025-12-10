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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{internal_err, DataFusionError, Result};
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use futures::TryStreamExt;
use sail_common_datafusion::datasource::{MergeInfo as PhysicalMergeInfo, PhysicalSinkMode};
use url::Url;

use super::context::PlannerContext;
use crate::datasource::DataFusionMixins;
use crate::kernel::models::Add;
use crate::options::TableDeltaOptions;
use crate::physical_plan::{DeltaCommitExec, DeltaRemoveActionsExec, DeltaWriterExec};
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

/// Entry point for MERGE execution. Expects the logical MERGE to be fully
/// expanded (handled by ExpandMergeRule) and passed down as pre-expanded plans.
pub async fn build_merge_plan(
    ctx: &PlannerContext<'_>,
    merge_info: PhysicalMergeInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !merge_info.pre_expanded {
        return internal_err!(
            "MERGE planning expects a pre-expanded logical plan. Ensure ExpandMergeRule is enabled."
        );
    }

    let table = ctx.open_table().await?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    let table_schema = snapshot_state
        .snapshot()
        .arrow_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = snapshot_state.metadata().partition_columns().clone();
    let log_store = table.log_store();

    let mut options = ctx.options().clone();
    if merge_info.with_schema_evolution {
        options.merge_schema = true;
    }

    let expanded = merge_info.expanded_input.clone().ok_or_else(|| {
        DataFusionError::Plan("pre-expanded MERGE plan missing expanded input".to_string())
    })?;

    let touched_adds = if let Some(plan) = merge_info.touched_file_plan.clone() {
        collect_touched_files(ctx.session().task_ctx(), plan, &snapshot_state, log_store).await?
    } else {
        Vec::new()
    };

    finalize_merge(
        ctx.session(),
        expanded,
        ctx.table_url().clone(),
        options,
        partition_columns,
        table_schema,
        touched_adds,
    )
    .await
}

async fn collect_touched_files(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
) -> Result<Vec<Add>> {
    let paths = collect_touched_paths(task_ctx, plan).await?;
    load_add_actions_for_paths(paths, snapshot, log_store).await
}

async fn collect_touched_paths(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<HashSet<String>> {
    let batches = collect(plan, task_ctx).await?;
    let mut paths = HashSet::new();
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("Touched file plan must yield a Utf8 path column".to_string())
            })?;
        for value in array.iter().flatten() {
            paths.insert(value.to_string());
        }
    }
    Ok(paths)
}

async fn load_add_actions_for_paths(
    paths: HashSet<String>,
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
) -> Result<Vec<Add>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let mut stream = snapshot.snapshot().files(log_store.as_ref(), None);
    let mut adds = Vec::new();
    while let Some(view) = stream
        .try_next()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    {
        let path = view.path();
        if paths.contains(path.as_ref()) {
            adds.push(view.add_action());
        }
    }

    Ok(adds)
}

async fn create_add_actions_plan(
    session: &dyn Session,
    adds: &[Add],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("add", DataType::Utf8, true),
        Field::new("partition_scan", DataType::Boolean, false),
    ]));

    let batch = if adds.is_empty() {
        RecordBatch::new_empty(schema.clone())
    } else {
        let add_strings: Result<Vec<Option<String>>> = adds
            .iter()
            .map(|add| {
                serde_json::to_string(add)
                    .map(Some)
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .collect();
        let add_array = Arc::new(StringArray::from(add_strings?));
        let bools = vec![false; adds.len()];
        let partition_scan = Arc::new(BooleanArray::from(bools));
        RecordBatch::try_new(schema.clone(), vec![add_array, partition_scan])?
    };

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    table
        .scan(session, None, &[], None)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

async fn finalize_merge(
    session: &dyn Session,
    projected: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    table_schema: SchemaRef,
    touched_adds: Vec<Add>,
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

    if !touched_adds.is_empty() {
        let remove_source = create_add_actions_plan(session, &touched_adds).await?;
        let remove_plan = Arc::new(DeltaRemoveActionsExec::new(remove_source));
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
