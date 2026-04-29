use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::{internal_err, Result};
use sail_common_datafusion::datasource::{
    MergePredicateInfo, OperationOverride, RowLevelCommand, RowLevelTargetInfo, RowLevelWriteInfo,
    TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::merge::RowLevelWriteNode;

/// Creates a physical execution plan for a unified `RowLevelWriteNode`.
///
/// Routes to the appropriate `TableFormat::create_row_level_writer` based on the
/// command kind (DELETE, UPDATE, MERGE).
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

    let format = node.target_format().to_string();
    let registry = ctx.extension::<TableFormatRegistry>()?;
    let table_format = registry.get(&format)?;
    let merge_strategy = table_format.merge_strategy();

    match node.command() {
        RowLevelCommand::Delete => {
            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Delete,
                target,
                condition: node.condition().cloned(),
                expanded_input: None,
                touched_file_plan: None,
                with_schema_evolution: false,
                operation_override: None,
                merge_strategy,
            };
            table_format.create_row_level_writer(ctx, info).await
        }
        RowLevelCommand::Merge => {
            let write_plan = node.write_plan().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "MERGE RowLevelWriteNode must have a write_plan".into(),
                )
            })?;
            let physical_write = planner.create_physical_plan(write_plan, ctx).await?;

            let touched_file_plan = node.touched_files_plan();
            let physical_touched = if let Some(tp) = touched_file_plan {
                Some(planner.create_physical_plan(tp, ctx).await?)
            } else {
                None
            };

            // Insert-only detection: no matched or not-matched-by-source clauses.
            let is_insert_only = node
                .merge_options()
                .map(|opts| {
                    opts.matched_clauses.is_empty()
                        && opts.not_matched_by_source_clauses.is_empty()
                        && !opts.not_matched_by_target_clauses.is_empty()
                })
                .unwrap_or(false);

            let operation_override = build_merge_operation_override(node);

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
                with_schema_evolution: node.with_schema_evolution(),
                operation_override,
                merge_strategy,
            };
            table_format.create_row_level_writer(ctx, info).await
        }
        RowLevelCommand::Update => {
            internal_err!("UPDATE is not yet implemented")
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
        .map(|c| {
            let action_type = match &c.action {
                sail_logical_plan::merge::MergeMatchedAction::Delete => "delete",
                sail_logical_plan::merge::MergeMatchedAction::UpdateAll
                | sail_logical_plan::merge::MergeMatchedAction::UpdateSet(_) => "update",
            }
            .to_string();
            let predicate = c.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicateInfo {
                action_type,
                predicate,
            }
        })
        .collect::<Vec<_>>();

    let not_matched_predicates = opts
        .not_matched_by_target_clauses
        .iter()
        .map(|c| {
            let predicate = c.condition.as_ref().and_then(|x| x.source.clone());
            MergePredicateInfo {
                action_type: "insert".to_string(),
                predicate,
            }
        })
        .collect::<Vec<_>>();

    let not_matched_by_source_predicates = opts
        .not_matched_by_source_clauses
        .iter()
        .map(|c| {
            let action_type = match &c.action {
                sail_logical_plan::merge::MergeNotMatchedBySourceAction::Delete => "delete",
                sail_logical_plan::merge::MergeNotMatchedBySourceAction::UpdateSet(_) => "update",
            }
            .to_string();
            let predicate = c.condition.as_ref().and_then(|x| x.source.clone());
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
