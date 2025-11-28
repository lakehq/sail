// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::pruning::PruningStatistics;
use futures::TryStreamExt;

use crate::kernel::models::Add;
use crate::kernel::DeltaResult;
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

/// Result of file pruning operation
#[derive(Debug, Clone)]
pub struct PruningResult {
    /// Files that passed the pruning filters
    pub files: Vec<Add>,
    /// Pruning mask used for statistics calculation (None if no pruning was applied)
    pub pruning_mask: Option<Vec<bool>>,
}

async fn collect_add_actions(
    snapshot: &DeltaTableState,
    log_store: &LogStoreRef,
) -> DeltaResult<Vec<Add>> {
    snapshot
        .snapshot()
        .files(log_store.as_ref(), None)
        .map_ok(|view| view.add_action())
        .try_collect()
        .await
}

/// Core file pruning function that filters files based on predicates and limit
pub async fn prune_files(
    snapshot: &DeltaTableState,
    log_store: &LogStoreRef,
    session: &dyn Session,
    filters: &[Expr],
    limit: Option<usize>,
    logical_schema: SchemaRef,
) -> Result<PruningResult> {
    let filter_expr = conjunction(filters.iter().cloned());

    // Early return if no filters and no limit
    if filter_expr.is_none() && limit.is_none() {
        let files = collect_add_actions(snapshot, log_store).await?;
        return Ok(PruningResult {
            files,
            pruning_mask: None,
        });
    }

    let log_data = snapshot.snapshot().log_data();
    let num_containers = log_data.num_containers();

    // Apply predicate-based pruning
    let files_to_prune = if let Some(predicate) = &filter_expr {
        // Convert logical expression to physical expression for pruning
        let df_schema = logical_schema.clone().to_dfschema()?;
        let physical_predicate = session.create_physical_expr(predicate.clone(), &df_schema)?;

        let pruning_predicate = PruningPredicate::try_new(physical_predicate, logical_schema)?;
        pruning_predicate.prune(&log_data)?
    } else {
        vec![true; num_containers]
    };

    // Collect all files and apply pruning logic
    let all_files = collect_add_actions(snapshot, log_store).await?;

    // Apply limit-based pruning with statistics consideration
    let mut pruned_without_stats = vec![];
    let mut rows_collected = 0;
    let mut files = vec![];

    for (action, keep) in all_files.into_iter().zip(files_to_prune.iter()) {
        if *keep {
            if let Some(limit) = limit {
                if let Some(stats) = action
                    .get_stats()
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?
                {
                    if rows_collected <= limit as i64 {
                        rows_collected += stats.num_records;
                        files.push(action);
                    } else {
                        break;
                    }
                } else {
                    pruned_without_stats.push(action);
                }
            } else {
                files.push(action);
            }
        }
    }

    // Add files without stats if we haven't reached the limit
    if let Some(limit) = limit {
        if rows_collected < limit as i64 {
            files.extend(pruned_without_stats);
        }
    }

    Ok(PruningResult {
        files,
        pruning_mask: Some(files_to_prune),
    })
}
