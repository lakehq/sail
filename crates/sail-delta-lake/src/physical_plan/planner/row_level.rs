use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{MergeStrategy, RowLevelCommand};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::row_level::{RowLevelEffect, RowLevelWriteNode};

use crate::lake_source::{DeltaLakeSource, split_delta_write_options_and_table_properties};
use crate::logical::table_source::DeltaTableSource;
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical_plan::planner::op_merge::merge_has_update_actions;
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
                with_schema_evolution: false,
                operation: None,
            };
            create_delta_row_level_writer(ctx, info, target_snapshot).await
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
                deletion_vector_plan: effects.deleted_rows,
                with_schema_evolution: node.with_schema_evolution(),
                operation: build_merge_operation(node),
            };
            create_delta_row_level_writer(ctx, info, target_snapshot).await
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
                deletion_vector_plan: effects.deleted_rows,
                with_schema_evolution: false,
                operation: Some(DeltaOperation::Update {
                    predicate: node
                        .condition()
                        .and_then(|condition| condition.source.clone()),
                }),
            };
            create_delta_row_level_writer(ctx, info, target_snapshot).await
        }
    }
}

#[derive(Default)]
struct PhysicalRowLevelEffects {
    write_rows: Option<Arc<dyn ExecutionPlan>>,
    touched_files: Option<Arc<dyn ExecutionPlan>>,
    deleted_rows: Option<Arc<dyn ExecutionPlan>>,
}

fn collect_physical_effects(
    logical_effects: &[RowLevelEffect],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<PhysicalRowLevelEffects> {
    if logical_effects.len() != physical_inputs.len() {
        return internal_err!(
            "RowLevelWriteNode expected {} physical inputs, got {}",
            logical_effects.len(),
            physical_inputs.len()
        );
    }

    let mut effects = PhysicalRowLevelEffects::default();
    for (logical_effect, physical_input) in logical_effects.iter().zip(physical_inputs) {
        let slot = match logical_effect {
            RowLevelEffect::WriteRows(_) => &mut effects.write_rows,
            RowLevelEffect::TouchFiles(_) => &mut effects.touched_files,
            RowLevelEffect::DeleteRows(_) => &mut effects.deleted_rows,
        };
        if slot.replace(Arc::clone(physical_input)).is_some() {
            return internal_err!("RowLevelWriteNode contains duplicate logical effects");
        }
    }
    Ok(effects)
}

async fn create_delta_row_level_writer(
    ctx: &dyn Session,
    info: RowLevelWriteInfo,
    target_snapshot: Arc<DeltaSnapshot>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let effective_strategy = if merge_has_update_actions(&info) {
        MergeStrategy::Eager
    } else if target_snapshot.verify_deletion_vectors().is_ok() {
        MergeStrategy::MergeOnRead
    } else {
        MergeStrategy::Eager
    };
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

    match (effective_strategy, info.command) {
        (MergeStrategy::MergeOnRead, RowLevelCommand::Delete) => {
            plan_delete_mor(&planner_ctx, delete_condition(&info)).await
        }
        (MergeStrategy::MergeOnRead, RowLevelCommand::Merge) => {
            plan_merge_mor(&planner_ctx, info).await
        }
        (MergeStrategy::MergeOnRead, RowLevelCommand::Update) => {
            plan_update_mor(&planner_ctx, info).await
        }
        (MergeStrategy::Eager, RowLevelCommand::Delete) => {
            plan_delete(&planner_ctx, delete_condition(&info)).await
        }
        (MergeStrategy::Eager, RowLevelCommand::Merge) => plan_merge(&planner_ctx, info).await,
        (MergeStrategy::Eager, RowLevelCommand::Update) => plan_update(&planner_ctx, info).await,
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
