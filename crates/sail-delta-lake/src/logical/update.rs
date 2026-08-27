use std::sync::Arc;

use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use datafusion_expr::logical_plan::Extension;
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, RowLevelWriteMode, UpdateInfo,
};
use sail_logical_plan::row_level::{RowLevelEffectRequirements, expand_update};

use super::merge::{ensure_row_level_metadata_columns, select_delta_row_level_write_mode};

pub fn expand_update_node(mut info: UpdateInfo) -> Result<LogicalPlan> {
    let mode = select_delta_row_level_write_mode(&info.target_plan)?;
    let row_index_column =
        matches!(mode, RowLevelWriteMode::MergeOnRead).then_some(MERGE_ROW_INDEX_COLUMN);
    let requirements = RowLevelEffectRequirements {
        touched_files: true,
        row_index_deletes: row_index_column.is_some(),
    };
    info.target_plan = Arc::new(ensure_row_level_metadata_columns(
        info.target_plan.as_ref().clone(),
        MERGE_FILE_COLUMN,
        row_index_column,
    )?);
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(expand_update(
            info,
            mode,
            requirements,
            MERGE_FILE_COLUMN,
            row_index_column,
        )?),
    }))
}
