use std::sync::Arc;

use datafusion_common::{Result, not_impl_err};
use datafusion_expr::{Extension, LogicalPlan};
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, OPERATION_COLUMN, RowLevelCommand,
    RowLevelOperationType, RowLevelWriteMode, UpdateInfo,
};
use sail_logical_plan::row_level::{
    RowLevelEffectRequirements, RowLevelWriteNode, expand_update,
    rewrite_row_level_target_condition,
};

use crate::logical::merge::ensure_merge_metadata_columns;
use crate::logical::row_level::{select_copy_on_write_rows, target_write_state, write_effects};

pub(crate) fn expand_update_node(mut info: UpdateInfo) -> Result<LogicalPlan> {
    let (mode, snapshot_id) = target_write_state(&info.target_plan, RowLevelCommand::Update)?;
    if mode == RowLevelWriteMode::MergeOnRead
        && !super::row_level::target_scan(&info.target_plan)?.has_row_lineage()
    {
        return not_impl_err!(
            "Iceberg UPDATE with write.update.mode=merge-on-read requires format-version 3"
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
        (mode == RowLevelWriteMode::MergeOnRead).then_some(MERGE_ROW_INDEX_COLUMN),
    )?);
    if mode == RowLevelWriteMode::CopyOnWrite
        && let Some(predicate) = rewrite_row_level_target_condition(
            info.condition.clone(),
            &info.input_schema,
            info.target_plan.schema(),
            &info.resolved_target_field_names,
        )?
    {
        info.target_plan = Arc::new(super::row_level::select_copy_on_write_candidates(
            info.target_plan.as_ref().clone(),
            predicate.expr,
        )?);
    }
    let mut metadata_columns = super::row_level::lineage_columns(&info.target_plan)?.to_vec();
    if mode == RowLevelWriteMode::MergeOnRead {
        metadata_columns.extend([
            MERGE_ROW_INDEX_COLUMN,
            crate::row_level_metadata::MERGE_PARTITION_SPEC_ID_COLUMN,
            crate::row_level_metadata::MERGE_FILE_METADATA_COLUMN,
        ]);
    }
    let expanded = expand_update(
        info,
        mode,
        RowLevelEffectRequirements::default(),
        MERGE_FILE_COLUMN,
        None,
        &metadata_columns,
    )?;
    let write_rows = expanded.effects().write_rows().ok_or_else(|| {
        datafusion_common::internal_datafusion_err!(
            "Iceberg UPDATE expansion is missing write rows"
        )
    })?;
    let write_rows = if mode == RowLevelWriteMode::CopyOnWrite {
        select_copy_on_write_rows(write_rows.as_ref().clone())?
    } else {
        datafusion_expr::LogicalPlanBuilder::from(write_rows.as_ref().clone())
            .filter(
                datafusion_expr::col(OPERATION_COLUMN)
                    .eq(datafusion_expr::lit(RowLevelOperationType::Update.as_i32())),
            )?
            .build()?
    };
    let node = RowLevelWriteNode::new_update(
        Arc::clone(expanded.raw_target()),
        mode,
        write_effects(write_rows),
        expanded.condition().cloned(),
        expanded.target().clone(),
        Arc::new(datafusion_common::DFSchema::empty()),
    )
    .with_expected_snapshot_id(Some(snapshot_id));
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}
