use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{RowLevelCommand, RowLevelWriteMode};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::{MergeMatchedAction, MergeNotMatchedBySourceAction};
use sail_logical_plan::row_level::{RowLevelEffectPlans, RowLevelWriteNode};

use crate::lake_source::{DeltaLakeSource, split_delta_write_options_and_table_properties};
use crate::logical::table_source::DeltaTableSource;
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical_plan::DeletionVectorRowOperationMode;
use crate::physical_plan::planner::{
    DeltaPlannerConfig, PlannerContext, RowLevelWriteInfo, plan_delete, plan_delete_mor,
    plan_merge, plan_merge_mor, plan_update, plan_update_mor,
};
use crate::spec::{DeltaOperation, MergePredicate};
use crate::table::DeltaSnapshot;

/// Creates a Delta physical execution plan for a unified `RowLevelWriteNode`.
pub async fn create_row_level_write_physical_plan(
    ctx: &dyn Session,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let target_snapshot = find_target_snapshot(node.raw_target())?;
    let effects = collect_physical_effects(node.effects(), physical_inputs)?;
    let target = node.target().clone();

    match node.command() {
        RowLevelCommand::Delete => {
            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Delete,
                target,
                condition: node.condition().cloned(),
                expanded_input: None,
                touched_file_plan: None,
                deletion_vector_plan: None,
                deletion_vector_operation_mode: None,
                with_schema_evolution: false,
                operation: None,
            };
            create_delta_row_level_writer(ctx, node.mode(), info, target_snapshot).await
        }
        RowLevelCommand::Merge => {
            let expanded_input = effects.write_rows.ok_or_else(|| {
                DataFusionError::Internal("MERGE RowLevelWriteNode must have a write_plan".into())
            })?;

            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Merge,
                target,
                condition: None,
                expanded_input: Some(expanded_input),
                touched_file_plan: effects.touched_files,
                deletion_vector_plan: effects.row_index_deletes,
                deletion_vector_operation_mode: merge_deletion_vector_operation_mode(node),
                with_schema_evolution: node.with_schema_evolution(),
                operation: build_merge_operation(node),
            };
            create_delta_row_level_writer(ctx, node.mode(), info, target_snapshot).await
        }
        RowLevelCommand::Update => {
            let expanded_input = effects.write_rows.ok_or_else(|| {
                DataFusionError::Internal("UPDATE RowLevelWriteNode must have a write plan".into())
            })?;
            let info = RowLevelWriteInfo {
                command: RowLevelCommand::Update,
                target,
                condition: node.condition().cloned(),
                expanded_input: Some(expanded_input),
                touched_file_plan: effects.touched_files,
                deletion_vector_plan: effects.row_index_deletes,
                deletion_vector_operation_mode: Some(DeletionVectorRowOperationMode::Update),
                with_schema_evolution: false,
                operation: Some(DeltaOperation::Update {
                    predicate: node
                        .condition()
                        .and_then(|condition| condition.source.clone()),
                }),
            };
            create_delta_row_level_writer(ctx, node.mode(), info, target_snapshot).await
        }
    }
}

#[derive(Default)]
struct PhysicalRowLevelEffects {
    write_rows: Option<Arc<dyn ExecutionPlan>>,
    touched_files: Option<Arc<dyn ExecutionPlan>>,
    row_index_deletes: Option<Arc<dyn ExecutionPlan>>,
}

fn collect_physical_effects(
    logical_effects: &RowLevelEffectPlans,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<PhysicalRowLevelEffects> {
    if logical_effects.len() != physical_inputs.len() {
        return internal_err!(
            "RowLevelWriteNode expected {} physical inputs, got {}",
            logical_effects.len(),
            physical_inputs.len()
        );
    }

    let mut physical_inputs = physical_inputs.iter();
    let mut take = |present: bool| -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if !present {
            return Ok(None);
        }
        physical_inputs
            .next()
            .map(Arc::clone)
            .map(Some)
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "RowLevelWriteNode physical effect input is missing".to_string(),
                )
            })
    };
    Ok(PhysicalRowLevelEffects {
        write_rows: take(logical_effects.write_rows().is_some())?,
        touched_files: take(logical_effects.touched_files().is_some())?,
        row_index_deletes: take(logical_effects.row_index_deletes().is_some())?,
    })
}

async fn create_delta_row_level_writer(
    ctx: &dyn Session,
    mode: RowLevelWriteMode,
    info: RowLevelWriteInfo,
    target_snapshot: Arc<DeltaSnapshot>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (target_options, _) =
        split_delta_write_options_and_table_properties(info.target.options.clone())?;
    let table_url =
        DeltaLakeSource::parse_table_url(ctx, vec![info.target.location.clone()]).await?;
    let delta_options = DeltaWriteOptions::resolve(ctx, target_options)?;
    let partition_columns = match info.command {
        RowLevelCommand::Delete => Vec::new(),
        RowLevelCommand::Merge | RowLevelCommand::Update => info.target.partition_by.clone(),
    };
    let config = DeltaPlannerConfig::new(
        table_url,
        delta_options,
        HashMap::new(),
        partition_columns,
        None,
        true,
    )
    .with_table_snapshot(Some(target_snapshot))
    .with_lakehouse_table(info.target.lakehouse_table.clone());
    let planner_ctx = PlannerContext::new(ctx, config);

    match (mode, info.command) {
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Delete) => {
            plan_delete_mor(&planner_ctx, delete_condition(&info)).await
        }
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Merge) => {
            plan_merge_mor(&planner_ctx, info).await
        }
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Update) => {
            plan_update_mor(&planner_ctx, info).await
        }
        (RowLevelWriteMode::CopyOnWrite, RowLevelCommand::Delete) => {
            plan_delete(&planner_ctx, delete_condition(&info)).await
        }
        (RowLevelWriteMode::CopyOnWrite, RowLevelCommand::Merge) => {
            plan_merge(&planner_ctx, info).await
        }
        (RowLevelWriteMode::CopyOnWrite, RowLevelCommand::Update) => {
            plan_update(&planner_ctx, info).await
        }
    }
}

fn delete_condition(
    info: &RowLevelWriteInfo,
) -> sail_common_datafusion::logical_expr::ExprWithSource {
    info.condition.clone().unwrap_or_else(|| {
        sail_common_datafusion::logical_expr::ExprWithSource::new(datafusion_expr::lit(true), None)
    })
}

fn find_target_snapshot(plan: &datafusion_expr::LogicalPlan) -> Result<Arc<DeltaSnapshot>> {
    let mut snapshot = None;
    plan.apply(|node| {
        if let datafusion_expr::LogicalPlan::TableScan(scan) = node
            && let Some(source) = scan.source.downcast_ref::<DeltaTableSource>()
        {
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

fn merge_deletion_vector_operation_mode(
    node: &RowLevelWriteNode,
) -> Option<DeletionVectorRowOperationMode> {
    let options = node.merge_options()?;
    let has_update = options.matched_clauses.iter().any(|clause| {
        matches!(
            clause.action,
            MergeMatchedAction::UpdateAll | MergeMatchedAction::UpdateSet(_)
        )
    }) || options
        .not_matched_by_source_clauses
        .iter()
        .any(|clause| matches!(clause.action, MergeNotMatchedBySourceAction::UpdateSet(_)));
    let has_delete = options
        .matched_clauses
        .iter()
        .any(|clause| matches!(clause.action, MergeMatchedAction::Delete))
        || options
            .not_matched_by_source_clauses
            .iter()
            .any(|clause| matches!(clause.action, MergeNotMatchedBySourceAction::Delete));

    match (has_update, has_delete) {
        (true, true) => Some(DeletionVectorRowOperationMode::Mixed),
        (true, false) => Some(DeletionVectorRowOperationMode::Update),
        (false, true) => Some(DeletionVectorRowOperationMode::Delete),
        (false, false) => None,
    }
}

fn build_merge_operation(node: &RowLevelWriteNode) -> Option<DeltaOperation> {
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
            MergePredicate {
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
            MergePredicate {
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
            MergePredicate {
                action_type,
                predicate,
            }
        })
        .collect::<Vec<_>>();

    Some(DeltaOperation::Merge {
        predicate: None,
        merge_predicate,
        matched_predicates,
        not_matched_predicates,
        not_matched_by_source_predicates,
    })
}
