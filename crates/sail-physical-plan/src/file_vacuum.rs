use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common_datafusion::datasource::{TableFormatRegistry, VacuumInfo};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_vacuum::FileVacuumOptions;

pub async fn create_file_vacuum_physical_plan(
    ctx: &SessionState,
    options: FileVacuumOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileVacuumOptions {
        table_name,
        path,
        format,
        retention_hours,
        dry_run,
        options,
    } = options;

    let info = VacuumInfo {
        table_name,
        path,
        retention_hours,
        dry_run,
        options,
    };

    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_vacuum_writer(ctx, info).await
}
