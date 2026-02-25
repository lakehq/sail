use std::net::IpAddr;
use std::sync::Arc;

use log::info;
use sail_common::config::AppConfig;
use sail_flight::config::FlightSqlServerOptions;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the Flight SQL server...");
}

pub fn run_flight_server(ip: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);

    let resource = ResourceOptions {
        kind: "flight-server",
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async { init_telemetry(&config.telemetry, resource) })?;

    runtime.block_on(async {
        let options = FlightSqlServerOptions {
            host: ip.to_string(),
            port,
        };

        sail_flight::serve_with_shutdown(options, shutdown()).await?;
        <Result<(), Box<dyn std::error::Error>>>::Ok(())
    })?;

    shutdown_telemetry();

    Ok(())
}
