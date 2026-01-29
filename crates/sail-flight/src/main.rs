mod cache;
mod config;
mod service;
mod session;

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::{Parser, Subcommand};
use config::FlightSqlServerConfig;
use log::{error, info};
use sail_common::config::AppConfig;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};
use service::SailFlightSqlService;
use tonic::transport::Server;

#[derive(Parser)]
#[command(name = "sail-flight")]
#[command(about = "Sail Arrow Flight SQL Server - Apache Arrow Flight SQL interface for Sail")]
#[command(version)]
#[command(long_about = r#"
Sail Arrow Flight SQL Server

This server provides an Apache Arrow Flight SQL interface to Sail's query engine.
It follows the same execution pipeline as spark-connect but exposes it via
the standard Arrow Flight SQL protocol.

Connection examples:
  - JDBC: jdbc:arrow-flight-sql://localhost:32010
  - Python (ADBC): adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010")
  - DBeaver: Use Arrow Flight SQL driver

References:
  - Arrow Flight SQL: https://arrow.apache.org/docs/format/FlightSql.html
  - Issue #520: https://github.com/lakehq/sail/issues/520
"#)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Arrow Flight SQL server
    Server {
        /// Port to listen on
        #[arg(short, long, default_value = "32010")]
        port: u16,

        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Maximum cache size in MB (default: 1024 = 1 GB)
        #[arg(long, default_value = "1024")]
        cache_size_mb: usize,

        /// Disable cache statistics logging
        #[arg(long, default_value = "false")]
        disable_cache_stats: bool,

        /// Maximum rows to return per query (0 = unlimited, default: 0)
        #[arg(long, default_value = "0")]
        max_rows: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize telemetry/tracing
    let app_config = Arc::new(AppConfig::load()?);
    let resource = ResourceOptions {
        kind: "flight-server",
    };
    init_telemetry(&app_config.telemetry, resource)?;

    let cli = Cli::parse();

    match &cli.command {
        Commands::Server {
            port,
            host,
            cache_size_mb,
            disable_cache_stats,
            max_rows,
        } => {
            let mut config = FlightSqlServerConfig::default();
            config.server.host = host.clone();
            config.server.port = *port;
            config.cache.max_size_bytes = cache_size_mb * 1024 * 1024; // Convert MB to bytes
            config.cache.enable_stats = !disable_cache_stats;
            config.limits.max_rows = *max_rows;

            if let Err(e) = run_flight_server(config).await {
                error!("Server error: {}", e);
                return Err(e);
            }
        }
    }

    shutdown_telemetry();
    Ok(())
}

async fn run_flight_server(
    config: FlightSqlServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Sail Arrow Flight SQL Server...");
    info!("Configuration:");
    info!("  Host: {}", config.server.host);
    info!("  Port: {}", config.server.port);
    info!(
        "  Cache max size: {} MB",
        config.cache.max_size_bytes / (1024 * 1024)
    );
    info!(
        "  Cache stats: {}",
        if config.cache.enable_stats {
            "enabled"
        } else {
            "disabled"
        }
    );
    info!(
        "  Max rows: {}",
        if config.limits.max_rows == 0 {
            "unlimited".to_string()
        } else {
            config.limits.max_rows.to_string()
        }
    );

    let addr = config.bind_address()?;
    let service = SailFlightSqlService::new(
        config.cache.max_size_bytes,
        config.cache.enable_stats,
        config.limits.max_rows,
    );

    info!("✓ Server listening on {}", addr);
    info!("Connection strings:");
    info!("  JDBC: jdbc:arrow-flight-sql://{}", addr);
    info!(
        "  Python: adbc_driver_flightsql.dbapi.connect(\"grpc://{}\")",
        addr
    );
    info!("");
    info!("Press Ctrl+C to stop the server");

    // Set up gRPC health check service
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<FlightServiceServer<SailFlightSqlService>>()
        .await;

    Server::builder()
        .add_service(health_service)
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    info!("Server stopped.");
    Ok(())
}
