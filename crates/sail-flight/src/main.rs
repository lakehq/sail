use std::sync::Arc;

use clap::{Parser, Subcommand};
use log::error;
use sail_common::config::AppConfig;
use sail_flight::config::FlightSqlServerConfig;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};

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
            max_rows,
        } => {
            let mut config = FlightSqlServerConfig::default();
            config.server.host = host.clone();
            config.server.port = *port;
            config.limits.max_rows = *max_rows;

            if let Err(e) = sail_flight::serve(config).await {
                error!("Server error: {}", e);
                return Err(e);
            }
        }
    }

    shutdown_telemetry();
    Ok(())
}
