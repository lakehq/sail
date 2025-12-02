use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::actor::ActorSystem;
use sail_telemetry::common::ContextPropagationEnv;

use crate::worker::{WorkerActor, WorkerOptions};

pub async fn run_worker(
    config: &AppConfig,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut system = ActorSystem::new();
    let options = WorkerOptions::try_new(config, runtime)?;
    let options = match std::env::var(ContextPropagationEnv::TRACEPARENT) {
        Ok(traceparent) => options.with_tracing(traceparent),
        Err(std::env::VarError::NotPresent) => options,
        Err(e) => {
            return Err(Box::new(e));
        }
    };
    let _handle = system.spawn::<WorkerActor>(options);
    system.join().await;
    Ok(())
}
