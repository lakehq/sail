use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use datafusion_expr::logical_plan::Extension;
use sail_common_datafusion::datasource::{DeleteInfo, MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN};
use sail_logical_plan::row_level::{
    RowLevelWriteNode, rewrite_row_level_target_condition, validate_row_level_internal_columns,
};

pub fn expand_delete_node(info: DeleteInfo) -> Result<LogicalPlan> {
    validate_row_level_internal_columns(
        &info.input_schema,
        &info.resolved_target_field_names,
        MERGE_FILE_COLUMN,
        Some(MERGE_ROW_INDEX_COLUMN),
        info.case_sensitive,
    )?;
    let condition = rewrite_row_level_target_condition(
        info.condition,
        &info.input_schema,
        info.target_plan.schema(),
        &info.resolved_target_field_names,
    )?;
    let node = RowLevelWriteNode::new_delete(info.target_plan, condition, info.target);
    Ok(LogicalPlan::Extension(Extension {
        node: std::sync::Arc::new(node),
    }))
}
