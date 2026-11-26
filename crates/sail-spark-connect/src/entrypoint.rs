// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::Arc;

use sail_common::actor::ActorSystem;
use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_common::runtime::RuntimeHandle;
use sail_common::server::{CONNECTION_GRACE_PERIOD, ServerBuilder};
pub use sail_session::session_manager::{ServerSessionFactoryFn, SessionManagerOptions};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::server::SparkConnectServer;
use crate::session_manager::{
    create_spark_session_factory, create_spark_session_manager_with_factory,
};
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

/// The meat of the gRPC server.
pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    serve_with_session_factory(
        listener,
        signal,
        config,
        runtime,
        create_spark_session_factory,
    )
    .await
}

/// The same as [`serve`], with the session factory chosen by the caller.
pub async fn serve_with_session_factory<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    session_factory_fn: ServerSessionFactoryFn,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let mut system = ActorSystem::new();
    let session_manager =
        create_spark_session_manager_with_factory(config, runtime, &mut system, session_factory_fn)
            .await?;
    let builder = {
        let server = SparkConnectServer::new(session_manager.clone());
        let service = SparkConnectServiceServer::new(server)
            // The original Spark Connect server seems to have configuration for inbound (decoding) message size only.
            // .max_encoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);
        ServerBuilder::new("sail_spark_connect", Default::default())
            .with_shutdown_grace_period(CONNECTION_GRACE_PERIOD)
            .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
            .await
    };
    let result = session_manager.serve(builder, listener, signal).await;
    system.join().await;
    result.map_err(Into::into)
}
