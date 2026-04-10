pub mod error;
pub mod service;
pub mod session;

use std::future::Future;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::{ServerBuilder, ServerBuilderOptions};
use service::{SailFlightSqlOptions, SailFlightSqlService};
use session::create_flight_session_manager;
use tokio::net::TcpListener;

pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let options = SailFlightSqlOptions {
        max_rows: config.flight.max_rows,
    };
    let session_manager = create_flight_session_manager(config, runtime)?;
    let service = SailFlightSqlService::new(session_manager, options);
    let flight_service = FlightServiceServer::new(service);

    let builder = ServerBuilder::new("flight-sql", ServerBuilderOptions::default())
        .add_service(flight_service, None)
        .await;

    builder.serve(listener, signal).await?;
    Ok(())
}
