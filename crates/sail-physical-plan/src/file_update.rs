use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::Result;
use sail_common_datafusion::datasource::{TableFormatRegistry, UpdateInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_update::FileUpdateOptions;

pub async fn create_file_update_physical_plan(
    ctx: &SessionState,
    _planner: &dyn PhysicalPlanner,
    _schema: datafusion::common::DFSchemaRef,
    options: FileUpdateOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileUpdateOptions {
        table_name: _,
        path,
        format,
        condition,
        assignments,
        options,
    } = options;

    let info = UpdateInfo {
        path,
        condition,
        assignments,
        options: options
            .into_iter()
            .map(|x| x.into_iter().collect())
            .collect(),
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_updater(ctx, info).await
}
