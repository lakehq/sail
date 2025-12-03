use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry};

pub fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load()?;
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    runtime
        .handle()
        .primary()
        .block_on(async { init_telemetry(&config.telemetry) })?;

    runtime
        .handle()
        .primary()
        .block_on(sail_execution::run_worker(&config, runtime.handle()))?;

    shutdown_telemetry();

    Ok(())
}
