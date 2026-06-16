use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{internal_err, not_impl_err, DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::{MergeStrategy, RowLevelCommand};
use sail_data_source::options::ResolveOptions;
use sail_data_source::resolve_listing_urls;
use sail_logical_plan::merge::RowLevelWriteNode;
use url::Url;

use crate::options::gen::DeltaWriteOptions;
use crate::physical_plan::planner::{
    plan_delete, plan_delete_mor, plan_merge, plan_merge_mor, DeltaPlannerConfig,
    MergePredicateInfo, OperationOverride, PlannerContext, RowLevelTargetInfo, RowLevelWriteInfo,
};
use crate::table::open_table_with_object_store;
use crate::table_format::{split_delta_write_options_and_table_properties, DeltaTableFormat};

/// Creates a Delta physical execution plan for a unified `RowLevelWriteNode`.
pub async fn create_row_level_write_physical_plan(
    ctx: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let target = RowLevelTargetInfo {
        table_name: node.target_table_name().to_vec(),
        path: node.target_location().to_string(),
        partition_by: node.target_partition_by().to_vec(),
        options: node.target_options().to_vec(),
    };

    match node.command() {
        RowLevelCommand::Delete => {
            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Delete,
                target,
                condition: node.condition().cloned(),
                expanded_input: None,
                touched_file_plan: None,
                deletion_vector_plan: None,
                with_schema_evolution: false,
                operation_override: None,
            };
            create_delta_row_level_writer(ctx, info).await
        }
        RowLevelCommand::Merge => {
            let write_plan = node.write_plan().ok_or_else(|| {
                DataFusionError::Internal("MERGE RowLevelWriteNode must have a write_plan".into())
            })?;
            let physical_write = planner.create_physical_plan(write_plan, ctx).await?;

            let physical_touched = if let Some(plan) = node.touched_files_plan() {
                Some(planner.create_physical_plan(plan, ctx).await?)
            } else {
                None
            };
            let physical_deletion_vector = if let Some(plan) = node.deletion_vector_plan() {
                Some(planner.create_physical_plan(plan, ctx).await?)
            } else {
                None
            };

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
                command: RowLevelCommand::Merge,
                target,
                condition: None,
                expanded_input: Some(physical_write),
                touched_file_plan: if is_insert_only {
                    None
                } else {
                    physical_touched
                },
                deletion_vector_plan: if is_insert_only {
                    None
                } else {
                    physical_deletion_vector
                },
                with_schema_evolution: node.with_schema_evolution(),
                operation_override: build_merge_operation_override(node),
            };
            create_delta_row_level_writer(ctx, info).await
        }
        RowLevelCommand::Update => internal_err!("UPDATE is not yet implemented"),
    }
}

async fn create_delta_row_level_writer(
    ctx: &dyn Session,
    info: RowLevelWriteInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    let effective_strategy = if matches!(
        info.command,
        RowLevelCommand::Delete | RowLevelCommand::Merge
    ) {
        detect_merge_strategy(ctx, &info)
            .await
            .unwrap_or(MergeStrategy::Eager)
    } else {
        MergeStrategy::Eager
    };
    let catalog_table =
        (!info.target.table_name.is_empty()).then(|| info.target.table_name.clone());
    let (target_options, _) =
        split_delta_write_options_and_table_properties(info.target.options.clone())?;

    match (effective_strategy, info.command) {
        (MergeStrategy::MergeOnRead, RowLevelCommand::Delete) => {
            let table_url = DeltaTableFormat::parse_table_url(ctx, vec![info.target.path]).await?;
            let condition = info.condition.ok_or_else(|| {
                DataFusionError::Plan("DELETE operation requires a WHERE condition".to_string())
            })?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let delete_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                Vec::new(),
                None,
                true,
            )
            .with_catalog_table(catalog_table.clone());
            let delete_ctx = PlannerContext::new(ctx, delete_config);
            plan_delete_mor(&delete_ctx, condition).await
        }
        (MergeStrategy::MergeOnRead, RowLevelCommand::Merge) => {
            let table_url =
                DeltaTableFormat::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let merge_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                info.target.partition_by.clone(),
                None,
                true,
            )
            .with_catalog_table(catalog_table.clone());
            let merge_ctx = PlannerContext::new(ctx, merge_config);
            plan_merge_mor(&merge_ctx, info).await
        }
        (MergeStrategy::MergeOnRead, RowLevelCommand::Update) => {
            not_impl_err!("Merge-on-Read strategy for UPDATE is not yet implemented for Delta Lake")
        }
        (MergeStrategy::Eager, RowLevelCommand::Delete) => {
            let table_url = DeltaTableFormat::parse_table_url(ctx, vec![info.target.path]).await?;
            let condition = info.condition.ok_or_else(|| {
                DataFusionError::Plan("DELETE operation requires a WHERE condition".to_string())
            })?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let delete_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                Vec::new(),
                None,
                true,
            )
            .with_catalog_table(catalog_table.clone());
            let delete_ctx = PlannerContext::new(ctx, delete_config);
            plan_delete(&delete_ctx, condition).await
        }
        (MergeStrategy::Eager, RowLevelCommand::Merge) => {
            let table_url =
                DeltaTableFormat::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let merge_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                info.target.partition_by.clone(),
                None,
                true,
            )
            .with_catalog_table(catalog_table.clone());
            let merge_ctx = PlannerContext::new(ctx, merge_config);
            plan_merge(&merge_ctx, info).await
        }
        (_, RowLevelCommand::Update) => {
            not_impl_err!("UPDATE is not yet implemented for Delta Lake")
        }
    }
}

/// Build `OperationOverride::Merge` from the logical MERGE options on the write node.
fn build_merge_operation_override(node: &RowLevelWriteNode) -> Option<OperationOverride> {
    let opts = node.merge_options()?;

    let merge_predicate = opts.on_condition.source.clone();

    let matched_predicates = opts
        .matched_clauses
        .iter()
        .map(|clause| {
            let action_type = match &clause.action {
                sail_logical_plan::merge::MergeMatchedAction::Delete => "delete",
                sail_logical_plan::merge::MergeMatchedAction::UpdateAll
                | sail_logical_plan::merge::MergeMatchedAction::UpdateSet(_) => "update",
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
                sail_logical_plan::merge::MergeNotMatchedBySourceAction::Delete => "delete",
                sail_logical_plan::merge::MergeNotMatchedBySourceAction::UpdateSet(_) => "update",
            }
            .to_string();
            let predicate = clause.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicateInfo {
                action_type,
                predicate,
            }
        })
        .collect::<Vec<_>>();

    Some(OperationOverride::Merge {
        predicate: None,
        merge_predicate,
        matched_predicates,
        not_matched_predicates,
        not_matched_by_source_predicates,
    })
}

/// Detect the merge strategy for a Delta table by inspecting its snapshot properties.
async fn detect_merge_strategy(
    ctx: &dyn Session,
    info: &RowLevelWriteInfo,
) -> Result<MergeStrategy> {
    let mut urls = resolve_listing_urls(ctx, vec![info.target.path.clone()]).await?;
    let table_url = match (urls.pop(), urls.is_empty()) {
        (Some(path), true) => <ListingTableUrl as AsRef<Url>>::as_ref(&path).clone(),
        _ => return Ok(MergeStrategy::Eager),
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
                Ok(MergeStrategy::MergeOnRead)
            } else {
                Ok(MergeStrategy::Eager)
            }
        }
        Err(_) => Ok(MergeStrategy::Eager),
    }
}
