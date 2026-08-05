use std::sync::Arc;

use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use datafusion_expr::logical_plan::Extension;
use sail_common_datafusion::datasource::{
    DeleteInfo, ROW_LEVEL_FILE_COLUMN, ROW_LEVEL_ROW_INDEX_COLUMN,
};
use sail_logical_plan::row_level::{RowLevelWriteNode, expand_delete};

use super::merge::{ensure_row_level_metadata_columns, row_level_target_supports_deletion_vectors};

pub fn expand_delete_node(mut info: DeleteInfo) -> Result<LogicalPlan> {
    let row_index_column = row_level_target_supports_deletion_vectors(&info.target_plan)?
        .then_some(ROW_LEVEL_ROW_INDEX_COLUMN);
    info.target_plan = Arc::new(ensure_row_level_metadata_columns(
        info.target_plan.as_ref().clone(),
        ROW_LEVEL_FILE_COLUMN,
        row_index_column,
    )?);
    let operation = expand_delete(info, ROW_LEVEL_FILE_COLUMN, row_index_column)?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(RowLevelWriteNode::new(operation)),
    }))
}
