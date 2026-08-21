use std::sync::Arc;

use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use datafusion_expr::logical_plan::Extension;
use sail_common_datafusion::datasource::{MERGE_FILE_COLUMN, UpdateInfo};
use sail_logical_plan::row_level::expand_update;

use super::merge::{ensure_row_level_metadata_columns, row_level_target_snapshot_id};

pub fn expand_update_node(mut info: UpdateInfo) -> Result<LogicalPlan> {
    let expected_snapshot_id = Some(row_level_target_snapshot_id(&info.target_plan)?);
    info.target_plan = Arc::new(ensure_row_level_metadata_columns(
        info.target_plan.as_ref().clone(),
        MERGE_FILE_COLUMN,
        None,
    )?);
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(
            expand_update(info, MERGE_FILE_COLUMN, None)?
                .with_expected_snapshot_id(expected_snapshot_id),
        ),
    }))
}
