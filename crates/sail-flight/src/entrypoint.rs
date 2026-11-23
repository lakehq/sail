// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use sail_common::actor::ActorSystem;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common::server::{ServerBuilder, ServerBuilderOptions};
use tokio::net::TcpListener;

use crate::service::SailFlightSqlService;
use crate::session::create_flight_session_manager;

pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let mut system = ActorSystem::new();
    let session_manager = create_flight_session_manager(config, runtime, &mut system).await?;
    let builder = {
        let service = SailFlightSqlService::new(session_manager.clone());
        let flight_service = FlightServiceServer::new(service);

        ServerBuilder::new("flight_sql", ServerBuilderOptions::default())
            .add_service(flight_service, None)
            .await
    };
    let result = session_manager.serve(builder, listener, signal).await;
    system.join().await;
    result.map_err(Into::into)
}
