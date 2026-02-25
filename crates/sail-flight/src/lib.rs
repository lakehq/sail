pub mod config;
pub mod error;
pub mod service;
pub mod session;

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use config::FlightSqlServerOptions;
use log::info;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_server::{ServerBuilder, ServerBuilderOptions};
use service::SailFlightSqlService;
use session::create_flight_session_manager;
use tokio::net::TcpListener;

pub async fn serve_with_shutdown<F>(
    options: FlightSqlServerOptions,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let app_config = Arc::new(AppConfig::load()?);
    let max_rows = app_config.flight.max_rows;

    info!("Starting Sail Arrow Flight SQL Server...");
    info!("Configuration:");
    info!("  Host: {}", options.host);
    info!("  Port: {}", options.port);
    info!(
        "  Max rows: {}",
        if max_rows == 0 {
            "unlimited".to_string()
        } else {
            max_rows.to_string()
        }
    );

    let runtime = RuntimeManager::try_new(&app_config.runtime)?;
    let session_manager =
        create_flight_session_manager(app_config, runtime.handle().clone())?;

    let addr = options.bind_address()?;
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    info!("Server listening on {}", local_addr);
    info!("Connection strings:");
    info!("  JDBC: jdbc:arrow-flight-sql://{}", local_addr);
    info!(
        "  Python: adbc_driver_flightsql.dbapi.connect(\"grpc://{}\")",
        local_addr
    );
    info!("");
    info!("Press Ctrl+C to stop the server");

    let service = SailFlightSqlService::new(session_manager, max_rows);
    let flight_service = FlightServiceServer::new(service);

    let builder = ServerBuilder::new("flight-sql", ServerBuilderOptions::default())
        .add_service(flight_service, None)
        .await;

    builder.serve(listener, shutdown).await?;

    info!("Server stopped.");
    Ok(())
}
