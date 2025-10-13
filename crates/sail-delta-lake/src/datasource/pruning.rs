use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::pruning::PruningStatistics;
use deltalake::kernel::Add;
use deltalake::logstore::LogStoreRef;
use futures::TryStreamExt;

use crate::datasource::delta_to_datafusion_error;
use crate::table::DeltaTableState;

/// Result of file pruning operation
#[derive(Debug, Clone)]
pub struct PruningResult {
    /// Files that passed the pruning filters
    pub files: Vec<Add>,
    /// Pruning mask used for statistics calculation (None if no pruning was applied)
    pub pruning_mask: Option<Vec<bool>>,
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
        let files: Vec<Add> = snapshot
            .file_actions_iter(log_store)
            .try_collect()
            .await
            .map_err(delta_to_datafusion_error)?;
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
    let all_files: Vec<Add> = snapshot
        .file_actions_iter(log_store)
        .try_collect()
        .await
        .map_err(delta_to_datafusion_error)?;

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
