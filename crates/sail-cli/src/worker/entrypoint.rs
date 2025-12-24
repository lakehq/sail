use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_session::session_factory::{SessionFactory, WorkerSessionFactory};
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};

pub fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load()?;
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    runtime.handle().primary().block_on(async {
        let resource = ResourceOptions { kind: "worker" };
        init_telemetry(&config.telemetry, resource)
    })?;

    let session = WorkerSessionFactory::new(&runtime.handle()).create(())?;
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
