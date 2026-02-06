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
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_physical_expr::expressions::{lit, Column as PhysicalColumn};

mod action_schema;
mod commit_exec;
pub mod discovery_exec;
mod expr_adapter;
mod log_replay_exec;
mod meta_adds;
mod remove_actions_exec;
mod scan_by_adds_exec;
mod writer_exec;

pub use action_schema::{
    decode_actions_and_meta_from_batch, decode_adds_from_batch, delta_action_schema,
    encode_actions, encode_add_actions, CommitMeta, ExecAction, COL_ACTION,
};
pub use commit_exec::DeltaCommitExec;
pub use discovery_exec::DeltaDiscoveryExec;
pub use expr_adapter::{DeltaCastColumnExpr, DeltaPhysicalExprAdapterFactory};
pub use log_replay_exec::DeltaLogReplayExec;
pub mod planner;
pub use planner::{
    plan_delete, plan_merge, plan_update, DeltaPhysicalPlanner, DeltaTableConfig, PlannerContext,
};
pub use remove_actions_exec::DeltaRemoveActionsExec;
pub use scan_by_adds_exec::DeltaScanByAddsExec;
pub use writer_exec::DeltaWriterExec;

/// Top-level derived column used to co-locate log actions by file path for parallel replay.
pub const COL_REPLAY_PATH: &str = "__sail_delta_replay_path";

/// Derived boolean marker indicating whether a log row is a `remove(path)` action.
///
/// This is computed by the planner and consumed by `DeltaLogReplayExec` to avoid decoding the
/// `remove` struct during streaming replay.
pub const COL_LOG_IS_REMOVE: &str = "__sail_delta_is_remove";

/// Derived log row version (from the 20-digit `_delta_log` filename prefix).
///
/// The planner attaches this as a partition column during log scanning so downstream nodes can
/// order actions deterministically for replay.
pub const COL_LOG_VERSION: &str = "__sail_delta_log_version";

/// Create a `ProjectionExec` instance that reorders columns so that partition columns
/// are placed at the end of the `RecordBatch`.
pub fn create_projection(
    input: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
) -> Result<Arc<ProjectionExec>> {
    let input_schema = input.schema();
    let projection_exprs = create_projection_expressions(&input_schema, &partition_columns)?;

    Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
}

fn create_projection_expressions(
    input_schema: &ArrowSchema,
    partition_columns: &[String],
) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>> {
    let mut projection_exprs = Vec::new();
    let mut partition_indices_map = std::collections::HashMap::new();

    let partition_set: HashSet<&String> = partition_columns.iter().collect();

    // First, add all non-partition columns
    for (i, field) in input_schema.fields().iter().enumerate() {
        if partition_set.contains(field.name()) {
            partition_indices_map.insert(field.name().clone(), i);
        } else {
            let column_expr = Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>;
            projection_exprs.push((column_expr, field.name().clone()));
        }
    }

    // Then, add partition columns in the specified order
    for col_name in partition_columns {
        let idx = *partition_indices_map.get(col_name).ok_or_else(|| {
            DataFusionError::Plan(format!("Partition column '{col_name}' not found in schema"))
        })?;
        let column_expr = Arc::new(Column::new(col_name, idx)) as Arc<dyn PhysicalExpr>;
        projection_exprs.push((column_expr, col_name.clone()));
    }

    Ok(projection_exprs)
}

/// Create a `SortExec` instance for Delta Lake data sorting.
pub fn create_sort(
    input: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<SortExec>> {
    let schema = input.schema();
    let num_cols = schema.fields().len();
    let num_part_cols = partition_columns.len();

    let mut sort_exprs: Vec<PhysicalSortExpr> = (num_cols - num_part_cols..num_cols)
        .zip(partition_columns.iter())
        .map(|(idx, name)| PhysicalSortExpr {
            expr: Arc::new(PhysicalColumn::new(name, idx)) as Arc<dyn PhysicalExpr>,
            options: SortOptions::default(), // Default ascending
        })
        .collect();

    // Add user-specified sort columns
    if let Some(ref user_sort_order) = sort_order {
        for req in user_sort_order {
            sort_exprs.push(PhysicalSortExpr {
                expr: req.expr.clone(),
                options: req.options.unwrap_or_default(),
            });
        }
    }

    let sort_exec = if !sort_exprs.is_empty() {
        let lex_ordering = LexOrdering::new(sort_exprs)
            .ok_or_else(|| DataFusionError::Internal("Failed to create LexOrdering".to_string()))?;
        // We've already partitioned data in the Repartition node,
        // sorting is only within each physical partition, so we must preserve partitioning.
        Arc::new(SortExec::new(lex_ordering, input).with_preserve_partitioning(true))
    } else {
        // No sorting needed, create a minimal SortExec with empty ordering
        let dummy_expr = PhysicalSortExpr {
            expr: lit(ScalarValue::Int32(Some(1))),
            options: SortOptions::default(),
        };
        let lex_ordering = LexOrdering::new(vec![dummy_expr]).ok_or_else(|| {
            DataFusionError::Internal("Failed to create dummy LexOrdering".to_string())
        })?;
        Arc::new(SortExec::new(lex_ordering, input))
    };

    Ok(sort_exec)
}

/// Create a `RepartitionExec` instance for Delta Lake data repartitioning.
pub fn create_repartition(
    input: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    num_partitions: usize,
) -> Result<Arc<RepartitionExec>> {
    let num_partitions = num_partitions.max(1);
    let partitioning = if partition_columns.is_empty() {
        // No partition columns, ensure some parallelism
        Partitioning::RoundRobinBatch(num_partitions)
    } else {
        // Since create_projection moves partition columns to the end, we can rely on their positions.
        let schema = input.schema();
        let num_cols = schema.fields().len();
        let num_part_cols = partition_columns.len();

        // TODO: Investigate repartitioning behavior for "bucketing" with overlapping partition columns
        // Current implementation may not handle the desired output structure where multiple writers
        // can create files within the same partition directory. For example:
        // year=2024/
        //     part-00000.parquet (created by writer 1)
        //     part-00001.parquet (created by writer 2)
        //     part-00002.parquet (created by writer 3)
        //     part-00003.parquet (created by writer 4)
        // year=2025/
        //     part-00000.parquet (created by writer 1)
        //     part-00001.parquet (created by writer 2)
        //     part-00002.parquet (created by writer 3)
        //     part-00003.parquet (created by writer 4)
        let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = (num_cols - num_part_cols..num_cols)
            .zip(partition_columns.iter())
            .map(|(idx, name)| Arc::new(PhysicalColumn::new(name, idx)) as Arc<dyn PhysicalExpr>)
            .collect();

        Partitioning::Hash(partition_exprs, num_partitions)
    };

    Ok(Arc::new(RepartitionExec::try_new(input, partitioning)?))
}

pub(crate) fn current_timestamp_millis() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
