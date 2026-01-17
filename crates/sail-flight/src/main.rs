mod config;
mod service;
mod session;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::{Parser, Subcommand};
use config::FlightSqlServerConfig;
use log::{error, info};
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
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize env_logger with default level INFO
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Server { port, host } => {
            let mut config = FlightSqlServerConfig::default();
            config.server.host = host.clone();
            config.server.port = *port;

            if let Err(e) = run_flight_server(config).await {
                error!("Server error: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

async fn run_flight_server(
    config: FlightSqlServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Sail Arrow Flight SQL Server...");
    info!("Configuration:");
    info!("  Host: {}", config.server.host);
    info!("  Port: {}", config.server.port);

    let addr = config.bind_address()?;
    let service = SailFlightSqlService::new();

    info!("✓ Server listening on {}", addr);
    info!("Connection strings:");
    info!("  JDBC: jdbc:arrow-flight-sql://{}", addr);
    info!(
        "  Python: adbc_driver_flightsql.dbapi.connect(\"grpc://{}\")",
        addr
    );
    info!("");
    info!("Press Ctrl+C to stop the server");

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
