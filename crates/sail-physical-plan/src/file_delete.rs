use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::Result;
use sail_common_datafusion::datasource::{
    RowLevelCommand, RowLevelTargetInfo, RowLevelWriteInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_delete::FileDeleteOptions;

/// Fallback physical planner for non-lakehouse DELETE (e.g. session planner).
/// Lakehouse DELETEs are handled via `RowLevelWriteNode` → `create_row_level_write_physical_plan`.
pub async fn create_file_delete_physical_plan(
    ctx: &SessionState,
    _planner: &dyn PhysicalPlanner,
    _schema: datafusion::common::DFSchemaRef,
    options: FileDeleteOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileDeleteOptions {
        table_name,
        path,
        format,
        condition,
        options,
    } = options;

    let info = RowLevelWriteInfo {
        command: RowLevelCommand::Delete,
        target: RowLevelTargetInfo {
            table_name,
            path,
            partition_by: Vec::new(),
            options,
        },
        condition,
        expanded_input: None,
        touched_file_plan: None,
        deletion_vector_plan: None,
        with_schema_evolution: false,
        operation_override: None,
        merge_strategy: Default::default(),
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry
        .get(&format)?
        .create_row_level_writer(ctx, info)
        .await
}
