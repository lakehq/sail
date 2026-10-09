use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use datafusion::arrow::datatypes::SchemaRef;
use futures::TryStreamExt;
use prost::Message;
use sail_common_datafusion::array::record_batch::cast_record_batch_positionally;
use tokio::sync::OnceCell;

use crate::error::ExecutionResult;
use crate::id::{DriverId, TaskStreamKey};
use crate::rpc::{ClientBuilder, ClientOptions};
use crate::stream::error::TaskStreamError;
use crate::stream::r#gen::{DriverTaskStreamTicket, TaskStreamTicket};
use crate::stream::reader::TaskStreamSource;
use crate::stream::service::transport::FlightClient;

#[derive(Clone, Copy, Debug)]
pub enum TaskStreamOwner {
    Driver { driver_id: DriverId },
    Worker,
}

#[derive(Clone)]
pub struct TaskStreamFlightClient {
    inner: Arc<TaskStreamFlightClientInner>,
}

struct TaskStreamFlightClientInner {
    options: ClientOptions,
    client: OnceCell<FlightClient>,
    owner: TaskStreamOwner,
}

impl TaskStreamFlightClient {
    pub fn new(options: ClientOptions, owner: TaskStreamOwner) -> Self {
        Self {
            inner: Arc::new(TaskStreamFlightClientInner {
                options,
                client: OnceCell::new(),
                owner,
            }),
        }
    }

    pub async fn fetch_task_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> ExecutionResult<TaskStreamSource> {
        let ticket = TaskStreamTicket {
            job_id: key.job_id.into(),
            stage: key.stage as u64,
            partition: key.partition as u64,
            attempt: key.attempt as u64,
            channel: key.channel as u64,
        };
        let ticket = match self.inner.owner {
            TaskStreamOwner::Driver { driver_id } => {
                let ticket = DriverTaskStreamTicket {
                    driver_id: driver_id.into(),
                    stream: Some(ticket),
                };
                ticket.encode_to_vec()
            }
            TaskStreamOwner::Worker => ticket.encode_to_vec(),
        };
        let request = arrow_flight::Ticket {
            ticket: ticket.into(),
        };
        let mut client = self
            .inner
            .client
            .get_or_try_init(|| FlightClient::connect(&self.inner.options))
            .await?
            .clone();
        let response = client.do_get(request).await?;
        let stream = response.into_inner().map_err(|e| e.into());
        let stream = FlightRecordBatchStream::new_from_flight_data(stream).map_err(|e| e.into());
        // The Flight data encoder may have issue with the `LargeList` data type, causing
        // schema mismatch. As a workaround, here we cast the record batch to the expected schema.
        // https://github.com/apache/arrow-rs/issues/10291
        let stream = stream.and_then(move |batch| {
            let result = if batch.schema().as_ref() == schema.as_ref() {
                Ok(batch)
            } else {
                cast_record_batch_positionally(batch, schema.clone())
                    .map_err(|e| TaskStreamError::External(Arc::new(e)))
            };
            futures::future::ready(result)
        });
        Ok(Box::pin(stream) as TaskStreamSource)
    }
}
