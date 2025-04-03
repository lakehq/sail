use sail_common::config::AppConfig;
use sail_plan::runtime::RuntimeExtension;
use sail_server::actor::ActorSystem;

use crate::worker::{WorkerActor, WorkerOptions};

pub async fn run_worker(
    runtime_extension: RuntimeExtension,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load()?;
    let mut system = ActorSystem::new();
    let options = WorkerOptions::try_new(&config, runtime_extension)?;
    let _handle = system.spawn::<WorkerActor>(options);
    system.join().await;
    Ok(())
}
