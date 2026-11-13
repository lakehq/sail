use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::TryStreamExt;
use prost::Message;
use tokio::sync::oneshot;

use super::*;
use crate::id::{JobId, TaskStreamKey};
use crate::shuffle::ShuffleCompression;
use crate::stream::r#gen::TaskStreamTicket;
use crate::stream::service::{TaskStreamFetcher, TaskStreamFlightServer};

struct Fetcher(Arc<ReplayStream>);
#[tonic::async_trait]
impl TaskStreamFetcher<TaskStreamKey> for Fetcher {
    async fn fetch(
        &self,
        _key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ExecutionResult<()> {
        let _ = result.send(self.0.subscribe());
        Ok(())
    }
}

fn runtime() -> datafusion::common::Result<Arc<RuntimeEnv>> {
    RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024)))
        .build_arc()
}
fn batch() -> datafusion::common::Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..1024))],
    )?)
}
fn id() -> ReplayId {
    ReplayId {
        scope: Default::default(),
        ordinal: 0,
    }
}

#[tokio::test]
async fn replay_flight_rpc_independent_consumers_and_reopen()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let runtime = runtime()?;
        let expected = batch()?;
        let (stream, mut sink) = ReplayStream::create(id(), expected.schema(), runtime.clone(), 0)?;
        assert!(stream.subscribe().is_err());
        for _ in 0..16 {
            sink.write(expected.clone()).await?;
        }
        sink.commit().await?;
        let stream = Arc::new(stream);
        let server = TaskStreamFlightServer::new(
            Box::new(Fetcher(stream.clone())),
            ShuffleCompression::None,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let (shutdown, signal) = oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(server))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = signal.await;
                }),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
            .connect()
            .await?;
        let mut client = FlightServiceClient::new(channel);
        let key = TaskStreamKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 0,
            attempt: 3,
            channel: 0,
        };
        let ticket = Ticket {
            ticket: TaskStreamTicket {
                job_id: key.job_id.into(),
                stage: key.stage as u64,
                partition: 0,
                attempt: 3,
                channel: 0,
            }
            .encode_to_vec()
            .into(),
        };
        let response = client.do_get(ticket.clone()).await?;
        let mut first = FlightRecordBatchStream::new_from_flight_data(
            response.into_inner().map_err(FlightError::from),
        );
        assert_eq!(first.next().await.transpose()?, Some(expected.clone()));
        // A second complete reader does not depend on draining the first.
        for _ in 0..2 {
            let response = client.do_get(ticket.clone()).await?;
            let mut reader = FlightRecordBatchStream::new_from_flight_data(
                response.into_inner().map_err(FlightError::from),
            );
            let mut batches = 0;
            while let Some(batch) = reader.next().await.transpose()? {
                assert_eq!(batch, expected);
                batches += 1;
            }
            assert_eq!(batches, 16);
        }
        drop(first);
        stream.retire();
        assert!(client.do_get(ticket).await.is_err());
        drop(client);
        let _ = shutdown.send(());
        server.await??;
        drop(stream);
        assert_eq!(runtime.memory_pool.reserved(), 0);
        assert_eq!(runtime.disk_manager.used_disk_space(), 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?
}

#[tokio::test]
async fn replay_local_retirement_closes_publication_and_existing_readers()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for sealed in [false, true] {
        let runtime = runtime()?;
        let batch = batch()?;
        let (stream, mut sink) = ReplayStream::create(id(), batch.schema(), runtime.clone(), 0)?;
        sink.write(batch).await?;
        if sealed {
            sink.commit().await?;
            let mut reader = stream.subscribe()?;
            stream.retire();
            assert!(reader.next().await.transpose().is_err());
            drop(reader);
        } else {
            stream.retire();
            assert!(sink.commit().await.is_err());
        }
        assert!(stream.subscribe().is_err());
        drop(stream);
        assert_eq!(runtime.memory_pool.reserved(), 0);
        assert_eq!(runtime.disk_manager.used_disk_space(), 0);
    }
    Ok(())
}
