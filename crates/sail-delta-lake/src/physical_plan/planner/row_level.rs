use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result, not_impl_err};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::{RowLevelCommand, RowLevelStrategy};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_data_source::options::ResolveOptions;
use sail_data_source::resolve_listing_urls;
use sail_logical_plan::row_level::RowLevelWriteNode;
use url::Url;

use crate::lake_source::{DeltaLakeSource, split_delta_write_options_and_table_properties};
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical_plan::planner::{
    DeltaPlannerConfig, DeltaRowLevelOperation, MergeCommitInfo, MergePredicateInfo,
    PlannerContext, RowLevelTargetInfo, RowLevelWriteInfo, plan_delete, plan_delete_mor,
    plan_merge_mor, plan_row_level_rewrite, plan_update,
};
use crate::table::open_table_with_object_store;

/// Creates a Delta physical execution plan for a unified `RowLevelWriteNode`.
pub async fn create_row_level_write_physical_plan(
    ctx: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let logical_target = node.target();
    let target = RowLevelTargetInfo {
        table_name: logical_target.table_name.clone(),
        path: logical_target.location.clone(),
        partition_by: logical_target.partition_by.clone(),
        options: logical_target.options.clone(),
        lakehouse_table: logical_target.lakehouse_table.clone(),
    };

    let physical_write = planner
        .create_physical_plan(node.write_rows_plan()?, ctx)
        .await?;
    let physical_touched = match node.touched_files_plan() {
        Some(plan) => Some(planner.create_physical_plan(plan, ctx).await?),
        None => None,
    };
    let physical_delete_rows = match node.delete_rows_plan() {
        Some(plan) => Some(planner.create_physical_plan(plan, ctx).await?),
        None => None,
    };

    match node.command() {
        RowLevelCommand::Delete => {
            let info = RowLevelWriteInfo {
                operation: DeltaRowLevelOperation::Delete {
                    condition: node.commit().predicate().cloned(),
                },
                target,
                expanded_input: physical_write,
                touched_file_plan: physical_touched,
                deletion_vector_plan: physical_delete_rows,
                with_schema_evolution: false,
            };
            create_delta_row_level_writer(ctx, info).await
        }
        RowLevelCommand::Merge => {
            // Insert-only MERGE does not need touched-file or deletion-vector side plans.
            let is_insert_only = node
                .merge_options()
                .map(|opts| {
                    opts.matched_clauses.is_empty()
                        && opts.not_matched_by_source_clauses.is_empty()
                        && !opts.not_matched_by_target_clauses.is_empty()
                })
                .unwrap_or(false);

            let info = RowLevelWriteInfo {
                operation: DeltaRowLevelOperation::Merge(build_merge_commit_info(node)?),
                target,
                expanded_input: physical_write,
                touched_file_plan: if is_insert_only {
                    None
                } else {
                    physical_touched
                },
                deletion_vector_plan: if is_insert_only {
                    None
                } else {
                    physical_delete_rows
                },
                with_schema_evolution: node.with_schema_evolution(),
            };
            create_delta_row_level_writer(ctx, info).await
        }
        RowLevelCommand::Update => {
            let info = RowLevelWriteInfo {
                operation: DeltaRowLevelOperation::Update {
                    condition: node.commit().predicate().cloned(),
                },
                target,
                expanded_input: physical_write,
                touched_file_plan: physical_touched,
                deletion_vector_plan: physical_delete_rows,
                with_schema_evolution: false,
            };
            create_delta_row_level_writer(ctx, info).await
        }
    }
}

async fn create_delta_row_level_writer(
    ctx: &dyn Session,
    info: RowLevelWriteInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    let command = info.operation.command();
    let effective_strategy = if matches!(command, RowLevelCommand::Delete | RowLevelCommand::Merge)
    {
        detect_row_level_strategy(ctx, &info)
            .await
            .unwrap_or(RowLevelStrategy::Eager)
    } else {
        RowLevelStrategy::Eager
    };
    let lakehouse_table = info.target.lakehouse_table.clone();
    let (target_options, _) =
        split_delta_write_options_and_table_properties(info.target.options.clone())?;

    match (effective_strategy, command) {
        (RowLevelStrategy::MergeOnRead, RowLevelCommand::Delete) => {
            let table_url = DeltaLakeSource::parse_table_url(ctx, vec![info.target.path]).await?;
            let condition = info
                .operation
                .condition()
                .cloned()
                .unwrap_or_else(|| ExprWithSource::new(datafusion_expr::lit(true), None));
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let delete_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                Vec::new(),
                None,
                true,
            )
            .with_lakehouse_table(lakehouse_table.clone());
            let delete_ctx = PlannerContext::new(ctx, delete_config);
            plan_delete_mor(&delete_ctx, condition).await
        }
        (RowLevelStrategy::MergeOnRead, RowLevelCommand::Merge) => {
            let table_url =
                DeltaLakeSource::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let merge_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                info.target.partition_by.clone(),
                None,
                true,
            )
            .with_lakehouse_table(lakehouse_table.clone());
            let merge_ctx = PlannerContext::new(ctx, merge_config);
            plan_merge_mor(&merge_ctx, info).await
        }
        (RowLevelStrategy::MergeOnRead, RowLevelCommand::Update) => {
            not_impl_err!("Merge-on-Read strategy for UPDATE is not yet implemented for Delta Lake")
        }
        (RowLevelStrategy::Eager, RowLevelCommand::Delete) => {
            let table_url = DeltaLakeSource::parse_table_url(ctx, vec![info.target.path]).await?;
            let condition = info
                .operation
                .condition()
                .cloned()
                .unwrap_or_else(|| ExprWithSource::new(datafusion_expr::lit(true), None));
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let delete_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                info.target.partition_by.clone(),
                None,
                true,
            )
            .with_lakehouse_table(lakehouse_table.clone());
            let delete_ctx = PlannerContext::new(ctx, delete_config);
            plan_delete(&delete_ctx, condition).await
        }
        (RowLevelStrategy::Eager, command) => {
            let table_url =
                DeltaLakeSource::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let row_level_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                info.target.partition_by.clone(),
                None,
                true,
            )
            .with_lakehouse_table(lakehouse_table.clone());
            let row_level_ctx = PlannerContext::new(ctx, row_level_config);
            match command {
                RowLevelCommand::Update => plan_update(&row_level_ctx, info).await,
                RowLevelCommand::Delete | RowLevelCommand::Merge => {
                    plan_row_level_rewrite(&row_level_ctx, info).await
                }
            }
        }
    }
}

/// Build Delta commit metadata from the logical MERGE clauses.
fn build_merge_commit_info(node: &RowLevelWriteNode) -> Result<MergeCommitInfo> {
    let opts = node.merge_options().ok_or_else(|| {
        DataFusionError::Internal("MERGE row-level node is missing commit options".to_string())
    })?;

    let merge_predicate = opts.on_condition.source.clone();

    let matched_predicates = opts
        .matched_clauses
        .iter()
        .map(|clause| {
            let action_type = match &clause.action {
                sail_logical_plan::row_level::MergeMatchedAction::Delete => "delete",
                sail_logical_plan::row_level::MergeMatchedAction::UpdateAll
                | sail_logical_plan::row_level::MergeMatchedAction::UpdateSet(_) => "update",
            }
            .to_string();
            let predicate = clause.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicateInfo {
                action_type,
                predicate,
            }
        })
        .collect::<Vec<_>>();

    let not_matched_predicates = opts
        .not_matched_by_target_clauses
        .iter()
        .map(|clause| {
            let predicate = clause.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicateInfo {
                action_type: "insert".to_string(),
                predicate,
            }
        })
        .collect::<Vec<_>>();

    let not_matched_by_source_predicates = opts
        .not_matched_by_source_clauses
        .iter()
        .map(|clause| {
            let action_type = match &clause.action {
                sail_logical_plan::row_level::MergeNotMatchedBySourceAction::Delete => "delete",
                sail_logical_plan::row_level::MergeNotMatchedBySourceAction::UpdateSet(_) => {
                    "update"
                }
            }
            .to_string();
            let predicate = clause.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicateInfo {
                action_type,
                predicate,
            }
        })
        .collect::<Vec<_>>();

    Ok(MergeCommitInfo {
        predicate: None,
        merge_predicate,
        matched_predicates,
        not_matched_predicates,
        not_matched_by_source_predicates,
    })
}

/// Detect the row-level write strategy from the Delta table protocol and properties.
async fn detect_row_level_strategy(
    ctx: &dyn Session,
    info: &RowLevelWriteInfo,
) -> Result<RowLevelStrategy> {
    let mut urls = resolve_listing_urls(ctx, vec![info.target.path.clone()]).await?;
    let table_url = match (urls.pop(), urls.is_empty()) {
        (Some(path), true) => <ListingTableUrl as AsRef<Url>>::as_ref(&path).clone(),
        _ => return Ok(RowLevelStrategy::Eager),
    };
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    match open_table_with_object_store(table_url, object_store, Default::default()).await {
        Ok(table) => {
            let snapshot = table
                .snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if snapshot.verify_deletion_vectors().is_ok() {
                Ok(RowLevelStrategy::MergeOnRead)
            } else {
                Ok(RowLevelStrategy::Eager)
            }
        }
        Err(_) => Ok(RowLevelStrategy::Eager),
    }
}
