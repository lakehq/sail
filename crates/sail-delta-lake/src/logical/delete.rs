use std::sync::Arc;

use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use datafusion_expr::logical_plan::Extension;
use sail_common_datafusion::datasource::{DeleteInfo, MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN};
use sail_logical_plan::row_level::{expand_delete, validate_row_level_internal_columns};

use super::merge::{ensure_row_level_metadata_columns, row_level_target_supports_deletion_vectors};

pub fn expand_delete_node(mut info: DeleteInfo) -> Result<LogicalPlan> {
    let row_index_column = row_level_target_supports_deletion_vectors(&info.target_plan)?
        .then_some(MERGE_ROW_INDEX_COLUMN);
    validate_row_level_internal_columns(
        &info.input_schema,
        &info.resolved_target_field_names,
        MERGE_FILE_COLUMN,
        row_index_column,
        info.case_sensitive,
    )?;
    info.target_plan = Arc::new(ensure_row_level_metadata_columns(
        info.target_plan.as_ref().clone(),
        MERGE_FILE_COLUMN,
        row_index_column,
    )?);
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(expand_delete(info, MERGE_FILE_COLUMN, row_index_column)?),
    }))
}
