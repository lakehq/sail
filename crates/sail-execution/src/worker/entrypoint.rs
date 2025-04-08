use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::actor::ActorSystem;

use crate::worker::{WorkerActor, WorkerOptions};

pub async fn run_worker(
    config: &AppConfig,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut system = ActorSystem::new();
    let options = WorkerOptions::try_new(config, runtime)?;
    let _handle = system.spawn::<WorkerActor>(options);
    system.join().await;
    Ok(())
}
