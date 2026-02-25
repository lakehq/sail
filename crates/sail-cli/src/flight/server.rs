use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use log::info;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};
use tokio::net::TcpListener;

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the Flight SQL server...");
}

pub fn run_flight_server(ip: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);

    let resource = ResourceOptions {
        kind: "flight-server",
    };

    let runtime_manager = RuntimeManager::try_new(&config.runtime)?;

    runtime_manager
        .handle()
        .primary()
        .block_on(async { init_telemetry(&config.telemetry, resource) })?;

    runtime_manager.handle().primary().block_on(async {
        let address = SocketAddr::new(ip, port);
        let listener = TcpListener::bind(address).await?;
        let local_addr = listener.local_addr()?;

        info!("Starting the Flight SQL server on {}...", local_addr);

        sail_flight::serve(
            listener,
            shutdown(),
            config,
            runtime_manager.handle().clone(),
        )
        .await?;

        info!("The Flight SQL server has stopped.");
        <Result<(), Box<dyn std::error::Error>>>::Ok(())
    })?;

    shutdown_telemetry();

    Ok(())
}
