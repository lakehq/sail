use std::sync::Arc;

use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_session::session_factory::{SessionFactory, WorkerSessionFactory};
use sail_telemetry::telemetry::{ResourceOptions, init_telemetry, shutdown_telemetry};

pub fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = AppConfig::load()?;
    config.telemetry.configure_collector();
    let config = Arc::new(config);
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    runtime.handle().primary().block_on(async {
        let resource = ResourceOptions { kind: "worker" };
        init_telemetry(&config.telemetry, resource).await
    })?;

    let session = WorkerSessionFactory::new(config.clone(), runtime.handle()).create(())?;
    runtime
        .handle()
        .primary()
        .block_on(sail_execution::run_worker(
            &config,
            runtime.handle(),
            session,
        ))?;

    shutdown_telemetry();

    Ok(())
}
