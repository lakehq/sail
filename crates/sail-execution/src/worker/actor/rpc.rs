use arrow_flight::flight_service_server::FlightServiceServer;
use sail_common::actor::ActorHandle;
use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
use sail_common::server::ServerBuilder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::oneshot::Sender;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskStreamKey;
use crate::stream::reader::TaskStreamSource;
use crate::stream::service::{TaskStreamFetcher, TaskStreamFlightServer};
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};
use crate::worker::WorkerMessage;
use crate::worker::actor::WorkerActor;
use crate::worker::r#gen::worker_service_server::WorkerServiceServer;
use crate::worker::server::WorkerServer;

struct WorkerTaskStreamFetcher {
    handle: ActorHandle<TaskRunnerActor>,
}

#[async_trait]
impl TaskStreamFetcher<TaskStreamKey> for WorkerTaskStreamFetcher {
    async fn fetch(
        &self,
        key: TaskStreamKey,
        sender: Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ExecutionResult<()> {
        let message = TaskRunnerMessage::FetchLocalStream {
            key,
            result: sender,
        };
        self.handle
            .send(message)
            .await
            .map_err(ExecutionError::from)
    }
}

impl WorkerActor {
    pub(super) async fn serve(
        handle: ActorHandle<WorkerActor>,
        task_runner: ActorHandle<TaskRunnerActor>,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
        addr: impl ToSocketAddrs,
    ) -> ExecutionResult<()> {
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let server = WorkerServer::new(handle.clone(), task_runner.clone(), context);
        let service = WorkerServiceServer::new(server)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let flight_server =
            TaskStreamFlightServer::<TaskStreamKey>::new(Box::new(WorkerTaskStreamFetcher {
                handle: task_runner,
            }));
        let flight_service = FlightServiceServer::new(flight_server)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        handle
            .send(WorkerMessage::ServerReady { port, signal: tx })
            .await?;

        ServerBuilder::new("sail_worker", Default::default())
            .add_service(service, Some(crate::worker::r#gen::FILE_DESCRIPTOR_SET))
            .await
            .add_service(flight_service, None)
            .await
            .serve(listener, async {
                let _ = rx.await;
            })
            .await
            .map_err(|e| ExecutionError::InternalError(e.to_string()))
    }
}
