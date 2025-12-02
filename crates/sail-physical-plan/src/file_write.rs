use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use sail_common_datafusion::datasource::{
    create_sort_order, PhysicalSinkMode, SinkInfo, SinkMode, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_write::FileWriteOptions;

pub async fn create_file_write_physical_plan(
    ctx: &SessionState,
    planner: &dyn PhysicalPlanner,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    options: FileWriteOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileWriteOptions {
        path,
        format,
        mode,
        partition_by,
        sort_by,
        bucket_by,
        options,
    } = options;
    let mode = match mode {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { condition } => {
            let condition =
                planner.create_physical_expr(&condition, logical_input.schema(), ctx)?;
            PhysicalSinkMode::OverwriteIf { condition }
        }
        SinkMode::OverwritePartitions => PhysicalSinkMode::OverwritePartitions,
    };
    let sort_order = create_sort_order(ctx, sort_by, logical_input.schema())?;
    let info = SinkInfo {
        input: physical_input,
        path,
        mode,
        partition_by,
        bucket_by,
        sort_order,
        // TODO: detect duplicated keys in each set of options
        options: options
            .into_iter()
            .map(|x| x.into_iter().collect())
            .collect(),
    };
    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_writer(ctx, info).await
}
