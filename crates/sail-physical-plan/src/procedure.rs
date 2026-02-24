use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common_datafusion::datasource::{ProcedureInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::procedure::ProcedureOptions;

pub async fn create_procedure_physical_plan(
    ctx: &SessionState,
    options: ProcedureOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ProcedureOptions {
        format,
        procedure_name,
        positional_arguments,
        named_arguments,
        target_table,
        target_path,
    } = options;
    let info = ProcedureInfo {
        format: format.clone(),
        procedure_name,
        positional_arguments,
        named_arguments,
        target_table,
        target_path,
    };
    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry
        .get(&format)?
        .create_procedure_executor(ctx, info)
        .await
}
