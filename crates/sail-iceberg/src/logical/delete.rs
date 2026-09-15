use std::sync::Arc;

use datafusion_common::{Result, plan_datafusion_err};
use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder, col, lit, when};
use sail_common_datafusion::datasource::{
    DeleteInfo, MERGE_FILE_COLUMN, OPERATION_COLUMN, RowLevelCommand, RowLevelOperationType,
    RowLevelWriteMode,
};
use sail_logical_plan::row_level::{RowLevelWriteNode, rewrite_row_level_target_condition};

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
    let target_plan = if mode == RowLevelWriteMode::CopyOnWrite {
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
    if mode == RowLevelWriteMode::CopyOnWrite {
        projection.push(col(MERGE_FILE_COLUMN));
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
        projections.push(
            when(predicate, lit(RowLevelOperationType::Delete.as_i32()))
                .otherwise(lit(RowLevelOperationType::Copy.as_i32()))?
                .alias(OPERATION_COLUMN),
        );
        let rows = LogicalPlanBuilder::from(target_plan.clone())
            .project(projections)?
            .build()?;
        super::row_level::select_copy_on_write_rows(rows)?
    } else {
        let predicate = condition.as_ref().ok_or_else(|| {
            plan_datafusion_err!("Iceberg equality-delete MOR DELETE requires a WHERE condition")
        })?;
        LogicalPlanBuilder::from(target_plan.clone())
            .filter(predicate.expr.clone())?
            .build()?
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
