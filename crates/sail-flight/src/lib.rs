pub mod config;
pub mod service;
pub mod session;

use arrow_flight::flight_service_server::FlightServiceServer;
use config::FlightSqlServerConfig;
use log::info;
use sail_server::{ServerBuilder, ServerBuilderOptions};
use service::SailFlightSqlService;
use tokio::net::TcpListener;

/// Serve the Flight SQL server
///
/// This is the main entry point for running the Flight SQL server.
/// It can be called from sail-cli or run directly via the sail-flight binary.
pub async fn serve(config: FlightSqlServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    serve_with_shutdown(config, std::future::pending()).await
}

/// Serve the Flight SQL server with graceful shutdown
///
/// This variant allows passing a shutdown signal for graceful termination.
/// Uses ServerBuilder from sail-server for consistent configuration with other Sail servers.
pub async fn serve_with_shutdown<F>(
    config: FlightSqlServerConfig,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    info!("Starting Sail Arrow Flight SQL Server...");
    info!("Configuration:");
    info!("  Host: {}", config.server.host);
    info!("  Port: {}", config.server.port);
    info!(
        "  Max rows: {}",
        if config.limits.max_rows == 0 {
            "unlimited".to_string()
        } else {
            config.limits.max_rows.to_string()
        }
    );

    let addr = config.bind_address()?;
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    info!("✓ Server listening on {}", local_addr);
    info!("Connection strings:");
    info!("  JDBC: jdbc:arrow-flight-sql://{}", local_addr);
    info!(
        "  Python: adbc_driver_flightsql.dbapi.connect(\"grpc://{}\")",
        local_addr
    );
    info!("");
    info!("Press Ctrl+C to stop the server");

    let service = SailFlightSqlService::new(config.limits.max_rows);
    let flight_service = FlightServiceServer::new(service);

    // Use ServerBuilder for consistent server configuration
    // This provides: health checks, reflection, tracing, and optimized TCP settings
    let builder = ServerBuilder::new("flight-sql", ServerBuilderOptions::default())
        .add_service(flight_service, None)
        .await;

    builder.serve(listener, shutdown).await?;

    info!("Server stopped.");
    Ok(())
}
