use std::sync::Arc;

use datafusion_common::{Result, not_impl_err};
use datafusion_expr::{Extension, LogicalPlan};
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, RowLevelCommand, RowLevelWriteMode, UpdateInfo,
};
use sail_logical_plan::row_level::{RowLevelEffectRequirements, RowLevelWriteNode, expand_update};

use crate::logical::merge::ensure_merge_metadata_columns;
use crate::logical::row_level::{select_copy_on_write_rows, target_write_state, write_effects};

pub(crate) fn expand_update_node(mut info: UpdateInfo) -> Result<LogicalPlan> {
    let (mode, snapshot_id) = target_write_state(&info.target_plan, RowLevelCommand::Update)?;
    if mode == RowLevelWriteMode::MergeOnRead {
        return not_impl_err!(
            "Iceberg UPDATE with write.update.mode=merge-on-read is not supported"
        );
    }
    super::row_level::validate_row_level_columns(
        &info.input_schema,
        &info.resolved_target_field_names,
        info.case_sensitive,
    )?;
    info.target_plan = Arc::new(ensure_merge_metadata_columns(
        info.target_plan.as_ref().clone(),
        MERGE_FILE_COLUMN,
        None,
    )?);
    let expanded = expand_update(
        info,
        mode,
        RowLevelEffectRequirements::default(),
        MERGE_FILE_COLUMN,
        None,
    )?;
    let write_rows = expanded.effects().write_rows().ok_or_else(|| {
        datafusion_common::internal_datafusion_err!(
            "Iceberg UPDATE expansion is missing write rows"
        )
    })?;
    let node = RowLevelWriteNode::new_update(
        Arc::clone(expanded.raw_target()),
        mode,
        write_effects(select_copy_on_write_rows(write_rows.as_ref().clone())?),
        expanded.condition().cloned(),
        expanded.target().clone(),
        Arc::new(datafusion_common::DFSchema::empty()),
    )
    .with_expected_snapshot_id(Some(snapshot_id));
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}
