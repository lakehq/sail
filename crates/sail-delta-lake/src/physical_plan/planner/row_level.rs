use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::{MergeStrategy, RowLevelCommand};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::row_level::RowLevelWriteNode;

use crate::lake_source::{DeltaLakeSource, split_delta_write_options_and_table_properties};
use crate::logical::table_source::DeltaTableSource;
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical_plan::planner::{
    DeltaPlannerConfig, MergePredicateInfo, OperationOverride, PlannerContext, RowLevelTargetInfo,
    RowLevelWriteInfo, plan_delete, plan_delete_mor, plan_merge, plan_merge_mor, plan_update,
    plan_update_mor,
};
use crate::table::DeltaSnapshot;

/// Creates a Delta physical execution plan for a unified `RowLevelWriteNode`.
pub async fn create_row_level_write_physical_plan(
    ctx: &dyn Session,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let target_snapshot = find_target_snapshot(node.raw_target())?;
    let target = RowLevelTargetInfo {
        table_name: node.target_table_name().to_vec(),
        path: node.target_location().to_string(),
        partition_by: node.target_partition_by().to_vec(),
        options: node.target_options().to_vec(),
        lakehouse_table: node.target_lakehouse_table().cloned(),
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
            create_delta_row_level_writer(ctx, info, target_snapshot).await
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
            let physical_deletion_vector = if let Some(plan) = node.row_index_delete_plan() {
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
            create_delta_row_level_writer(ctx, info, target_snapshot).await
        }
        RowLevelCommand::Update => {
            let physical_write = planner
                .create_physical_plan(
                    node.write_plan().ok_or_else(|| {
                        DataFusionError::Internal(
                            "UPDATE RowLevelWriteNode must have a write plan".into(),
                        )
                    })?,
                    ctx,
                )
                .await?;
            let physical_touched = match node.touched_files_plan() {
                Some(plan) => Some(planner.create_physical_plan(plan, ctx).await?),
                None => None,
            };
            let physical_deletion_vector = match node.row_index_delete_plan() {
                Some(plan) => Some(planner.create_physical_plan(plan, ctx).await?),
                None => None,
            };
            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Update,
                target,
                condition: node.condition().cloned(),
                expanded_input: Some(physical_write),
                touched_file_plan: physical_touched,
                deletion_vector_plan: physical_deletion_vector,
                with_schema_evolution: false,
                operation_override: Some(OperationOverride::Update {
                    predicate: node
                        .condition()
                        .and_then(|condition| condition.source.clone()),
                }),
            };
            create_delta_row_level_writer(ctx, info, target_snapshot).await
        }
    }
}

async fn create_delta_row_level_writer(
    ctx: &dyn Session,
    info: RowLevelWriteInfo,
    target_snapshot: Arc<DeltaSnapshot>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let effective_strategy = if merge_operation_requires_eager(&info) {
        MergeStrategy::Eager
    } else if matches!(
        info.command,
        RowLevelCommand::Delete | RowLevelCommand::Update | RowLevelCommand::Merge
    ) {
        if target_snapshot.verify_deletion_vectors().is_ok() {
            MergeStrategy::MergeOnRead
        } else {
            MergeStrategy::Eager
        }
    } else {
        MergeStrategy::Eager
    };
    let lakehouse_table = info.target.lakehouse_table.clone();
    let (target_options, _) =
        split_delta_write_options_and_table_properties(info.target.options.clone())?;

    match (effective_strategy, info.command) {
        (MergeStrategy::MergeOnRead, RowLevelCommand::Delete) => {
            let table_url = DeltaLakeSource::parse_table_url(ctx, vec![info.target.path]).await?;
            let condition = info.condition.unwrap_or_else(|| {
                sail_common_datafusion::logical_expr::ExprWithSource::new(
                    datafusion_expr::lit(true),
                    None,
                )
            });
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let delete_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                Vec::new(),
                None,
                true,
            )
            .with_table_snapshot(Some(Arc::clone(&target_snapshot)))
            .with_lakehouse_table(lakehouse_table.clone());
            let delete_ctx = PlannerContext::new(ctx, delete_config);
            plan_delete_mor(&delete_ctx, condition).await
        }
        (MergeStrategy::MergeOnRead, RowLevelCommand::Merge) => {
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
            .with_table_snapshot(Some(Arc::clone(&target_snapshot)))
            .with_lakehouse_table(lakehouse_table.clone());
            let merge_ctx = PlannerContext::new(ctx, merge_config);
            plan_merge_mor(&merge_ctx, info).await
        }
        (MergeStrategy::MergeOnRead, RowLevelCommand::Update) => {
            let table_url =
                DeltaLakeSource::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let update_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                info.target.partition_by.clone(),
                None,
                true,
            )
            .with_table_snapshot(Some(Arc::clone(&target_snapshot)))
            .with_lakehouse_table(lakehouse_table.clone());
            let update_ctx = PlannerContext::new(ctx, update_config);
            plan_update_mor(&update_ctx, info).await
        }
        (MergeStrategy::Eager, RowLevelCommand::Delete) => {
            let table_url = DeltaLakeSource::parse_table_url(ctx, vec![info.target.path]).await?;
            let condition = info.condition.unwrap_or_else(|| {
                sail_common_datafusion::logical_expr::ExprWithSource::new(
                    datafusion_expr::lit(true),
                    None,
                )
            });
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let delete_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                Vec::new(),
                None,
                true,
            )
            .with_table_snapshot(Some(Arc::clone(&target_snapshot)))
            .with_lakehouse_table(lakehouse_table.clone());
            let delete_ctx = PlannerContext::new(ctx, delete_config);
            plan_delete(&delete_ctx, condition).await
        }
        (MergeStrategy::Eager, RowLevelCommand::Merge) => {
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
            .with_table_snapshot(Some(Arc::clone(&target_snapshot)))
            .with_lakehouse_table(lakehouse_table.clone());
            let merge_ctx = PlannerContext::new(ctx, merge_config);
            plan_merge(&merge_ctx, info).await
        }
        (MergeStrategy::Eager, RowLevelCommand::Update) => {
            let table_url =
                DeltaLakeSource::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
            let delta_options = DeltaWriteOptions::resolve(ctx, target_options.clone())?;
            let update_config = DeltaPlannerConfig::new(
                table_url,
                delta_options,
                HashMap::new(),
                info.target.partition_by.clone(),
                None,
                true,
            )
            .with_table_snapshot(Some(target_snapshot))
            .with_lakehouse_table(lakehouse_table.clone());
            let update_ctx = PlannerContext::new(ctx, update_config);
            plan_update(&update_ctx, info).await
        }
    }
}

fn find_target_snapshot(plan: &datafusion_expr::LogicalPlan) -> Result<Arc<DeltaSnapshot>> {
    let mut snapshot = None;
    plan.apply(|node| {
        if let datafusion_expr::LogicalPlan::TableScan(scan) = node
            && let Some(source) = scan.source.downcast_ref::<DeltaTableSource>()
        {
            if let Some(existing) = &snapshot
                && !Arc::ptr_eq(existing, source.snapshot())
            {
                return internal_err!("row-level target contains multiple Delta snapshots");
            }
            snapshot = Some(Arc::clone(source.snapshot()));
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    snapshot.ok_or_else(|| {
        DataFusionError::Internal(
            "row-level target does not contain a Delta table snapshot".to_string(),
        )
    })
}

fn merge_operation_requires_eager(info: &RowLevelWriteInfo) -> bool {
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
        .any(|predicate| predicate.action_type.eq_ignore_ascii_case("update"))
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
