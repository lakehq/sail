mod service;
mod session;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::{Parser, Subcommand};
use service::SailFlightSqlService;
use tonic::transport::Server;

#[derive(Parser)]
#[command(name = "sail-flight")]
#[command(about = "Sail Arrow Flight SQL Server")]
#[command(version)]
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
    let cli = Cli::parse();

    match &cli.command {
        Commands::Server { port, host } => {
            run_flight_server(host, *port).await?;
        }
    }

    Ok(())
}

async fn run_flight_server(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Sail Arrow Flight SQL Server...");

    let addr = format!("{}:{}", host, port).parse()?;
    let service = SailFlightSqlService::new();

    println!("Server listening on {}", addr);
    println!("JDBC connection: jdbc:arrow-flight-sql://{}:{}", host, port);

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
