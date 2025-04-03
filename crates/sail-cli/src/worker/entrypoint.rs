use std::sync::Arc;

use sail_plan::runtime::RuntimeExtension;
use sail_telemetry::telemetry::init_telemetry;

pub fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let secondary_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let runtime_extension = Arc::new(RuntimeExtension::new(secondary_runtime.handle().clone()));

    runtime.block_on(sail_execution::run_worker(runtime_extension))?;

    fastrace::flush();

    Ok(())
}
