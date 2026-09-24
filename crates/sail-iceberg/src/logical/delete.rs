use std::sync::Arc;

use datafusion_common::Result;
use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder, col, lit, when};
use sail_common_datafusion::datasource::{
    DeleteInfo, MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, OPERATION_COLUMN, RowLevelCommand,
    RowLevelOperationType, RowLevelWriteMode,
};
use sail_logical_plan::row_level::{
    RowLevelWriteNode, rewrite_row_level_target_condition, row_level_expr_contains_subquery,
};

use crate::logical::merge::ensure_merge_metadata_columns;
use crate::logical::row_level::target_write_state;

pub(crate) fn expand_delete_node(info: DeleteInfo) -> Result<LogicalPlan> {
    let (mode, snapshot_id) = target_write_state(&info.target_plan, RowLevelCommand::Delete)?;
    super::row_level::validate_row_level_columns(
        &info.input_schema,
        &info.resolved_target_field_names,
        info.case_sensitive,
    )?;
    let condition = rewrite_row_level_target_condition(
        info.condition,
        &info.input_schema,
        info.target_plan.schema(),
        &info.resolved_target_field_names,
    )?;
    let deletion_vectors = mode == RowLevelWriteMode::MergeOnRead
        && super::row_level::target_scan(&info.target_plan)?.has_row_lineage();
    let target_plan = if deletion_vectors {
        ensure_merge_metadata_columns(
            info.target_plan.as_ref().clone(),
            MERGE_FILE_COLUMN,
            Some(MERGE_ROW_INDEX_COLUMN),
        )?
    } else if mode == RowLevelWriteMode::CopyOnWrite {
        let target = ensure_merge_metadata_columns(
            info.target_plan.as_ref().clone(),
            MERGE_FILE_COLUMN,
            None,
        )?;
        super::row_level::select_copy_on_write_candidates(
            target,
            condition
                .as_ref()
                .map(|predicate| predicate.expr.clone())
                .unwrap_or_else(|| lit(true)),
        )?
    } else {
        info.target_plan.as_ref().clone()
    };
    let mut projection = target_plan
        .schema()
        .fields()
        .iter()
        .zip(&info.resolved_target_field_names)
        .map(|(field, name)| {
            datafusion_expr::Expr::Column(datafusion_common::Column::from_name(field.name()))
                .alias(name)
        })
        .collect::<Vec<_>>();
    if mode == RowLevelWriteMode::CopyOnWrite || deletion_vectors {
        projection.push(col(MERGE_FILE_COLUMN));
        projection.extend(
            super::row_level::lineage_columns(&target_plan)?
                .iter()
                .map(|name| col(*name)),
        );
    }
    if deletion_vectors {
        projection.extend(
            [
                MERGE_ROW_INDEX_COLUMN,
                crate::row_level_metadata::MERGE_PARTITION_SPEC_ID_COLUMN,
                crate::row_level_metadata::MERGE_PARTITION_COLUMN,
            ]
            .map(col),
        );
    }
    let target_plan = LogicalPlanBuilder::from(target_plan)
        .project(projection)?
        .build()?;
    let write_rows = if mode == RowLevelWriteMode::CopyOnWrite {
        let predicate = condition
            .as_ref()
            .map(|condition| condition.expr.clone())
            .unwrap_or_else(|| lit(true));
        let mut projections = target_plan
            .schema()
            .columns()
            .into_iter()
            .map(datafusion_expr::Expr::Column)
            .collect::<Vec<_>>();
        let rows = if row_level_expr_contains_subquery(&predicate)? {
            let mut copy_projection = projections.clone();
            projections.push(lit(RowLevelOperationType::Delete.as_i32()).alias(OPERATION_COLUMN));
            copy_projection.push(lit(RowLevelOperationType::Copy.as_i32()).alias(OPERATION_COLUMN));
            let deleted = LogicalPlanBuilder::from(target_plan.clone())
                .filter(predicate.clone())?
                .project(projections)?
                .build()?;
            let copied = LogicalPlanBuilder::from(target_plan.clone())
                .filter(predicate.is_not_true())?
                .project(copy_projection)?
                .build()?;
            LogicalPlanBuilder::from(deleted).union(copied)?.build()?
        } else {
            projections.push(
                when(predicate, lit(RowLevelOperationType::Delete.as_i32()))
                    .otherwise(lit(RowLevelOperationType::Copy.as_i32()))?
                    .alias(OPERATION_COLUMN),
            );
            LogicalPlanBuilder::from(target_plan.clone())
                .project(projections)?
                .build()?
        };
        super::row_level::select_copy_on_write_rows(rows)?
    } else {
        let predicate = condition
            .as_ref()
            .map(|condition| condition.expr.clone())
            .unwrap_or_else(|| lit(true));
        let rows = LogicalPlanBuilder::from(target_plan.clone())
            .filter(predicate)?
            .build()?;
        if deletion_vectors {
            let mut projection = rows
                .schema()
                .columns()
                .into_iter()
                .map(datafusion_expr::Expr::Column)
                .collect::<Vec<_>>();
            projection.push(lit(RowLevelOperationType::Delete.as_i32()).alias(OPERATION_COLUMN));
            LogicalPlanBuilder::from(rows)
                .project(projection)?
                .build()?
        } else {
            rows
        }
    };
    let node = RowLevelWriteNode::new_delete(
        Arc::new(target_plan),
        mode,
        super::row_level::write_effects(write_rows),
        condition,
        info.target,
    )
    .with_expected_snapshot_id(Some(snapshot_id));
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}
