use sail_common::config::AppConfig;
use sail_server::actor::ActorSystem;

use crate::worker::WorkerActor;

pub async fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load()?;
    let mut system = ActorSystem::new();
    let options = (&config).try_into()?;
    let _handle = system.spawn::<WorkerActor>(options);
    system.join().await;
    Ok(())
}
