use std::sync::Arc;

use sail_common::config::AppConfig;
use sail_runtime::RuntimeManager;
use sail_telemetry::telemetry::init_telemetry;

pub fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    let config = Arc::new(AppConfig::load()?);
    let runtime_manager = RuntimeManager::try_new(&config.runtime, "Sail Worker")?;
    runtime_manager
        .handle()
        .primary()
        .block_on(sail_execution::run_worker(
            &config,
            runtime_manager.handle(),
        ))?;

    fastrace::flush();

    Ok(())
}
