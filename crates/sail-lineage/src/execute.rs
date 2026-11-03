use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;

use crate::options::LineageSessionOptions;
use crate::plan::LineagePlan;
use crate::service::LineageService;
use crate::stream::LineageStream;

/// Executes the physical plan with the session job runner, and tracks the
/// execution as an OpenLineage run if lineage is enabled for the session.
pub async fn execute_with_lineage(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    lineage: LineagePlan,
    options: &LineageSessionOptions,
) -> Result<SendableRecordBatchStream> {
    let service = ctx.extension::<JobService>()?;
    let run = ctx
        .extension::<LineageService>()
        .ok()
        .and_then(|x| x.start_run(&ctx.session_id(), options, lineage));
    let stream = match service.runner().execute(ctx, plan).await {
        Ok(x) => x,
        Err(e) => {
            if let Some(run) = run {
                run.fail(e.to_string());
            }
            return Err(e);
        }
    };
    Ok(match run {
        Some(run) => Box::pin(LineageStream::new(stream, run)),
        None => stream,
    })
}
