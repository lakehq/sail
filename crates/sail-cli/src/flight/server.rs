use std::net::IpAddr;
use std::sync::Arc;

use log::info;
use sail_common::config::AppConfig;
use sail_flight::config::FlightSqlServerConfig;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};

/// Handles graceful shutdown by waiting for a `SIGINT` signal.
async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the Flight SQL server...");
}

pub fn run_flight_server(
    ip: IpAddr,
    port: u16,
    max_rows: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);

    // Initialize telemetry
    let resource = ResourceOptions {
        kind: "flight-server",
    };

    // Create a simple runtime for the Flight server
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async { init_telemetry(&config.telemetry, resource) })?;

    runtime.block_on(async {
        let mut server_config = FlightSqlServerConfig::default();
        server_config.server.host = ip.to_string();
        server_config.server.port = port;
        server_config.limits.max_rows = max_rows;

        info!("Starting the Flight SQL server on {}:{}...", ip, port);

        sail_flight::serve_with_shutdown(server_config, shutdown()).await?;
        info!("The Flight SQL server has stopped.");
        <Result<(), Box<dyn std::error::Error>>>::Ok(())
    })?;

    shutdown_telemetry();

    Ok(())
}
