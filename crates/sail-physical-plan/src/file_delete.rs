use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::Result;
use sail_common_datafusion::datasource::{DeleteInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_delete::FileDeleteOptions;

pub async fn create_file_delete_physical_plan(
    ctx: &SessionState,
    _planner: &dyn PhysicalPlanner,
    _schema: datafusion::common::DFSchemaRef,
    options: FileDeleteOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileDeleteOptions {
        table_name: _,
        path,
        format,
        condition,
        options,
    } = options;

    let info = DeleteInfo {
        path,
        condition,
        options: options
            .into_iter()
            .map(|x| x.into_iter().collect())
            .collect(),
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_deleter(ctx, info).await
}
