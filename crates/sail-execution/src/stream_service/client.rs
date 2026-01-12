use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::datatypes::SchemaRef;
use futures::TryStreamExt;
use prost::Message;

use crate::error::ExecutionResult;
use crate::id::TaskStreamKey;
use crate::rpc::{ClientHandle, ClientOptions, ClientService};
use crate::stream::gen::TaskStreamTicket;
use crate::stream::reader::TaskStreamSource;

#[derive(Clone)]
pub struct TaskStreamFlightClient {
    inner: ClientHandle<FlightServiceClient<ClientService>>,
}

impl TaskStreamFlightClient {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            // TODO: share connection with driver/worker client
            inner: ClientHandle::new(options),
        }
    }

    pub async fn fetch_task_stream(
        &self,
        key: TaskStreamKey,
        // The schema is unused for now since we have only in-memory streams.
        // The schema may be needed if we have on-disk streams.
        _schema: SchemaRef,
    ) -> ExecutionResult<TaskStreamSource> {
        let ticket = TaskStreamTicket {
            job_id: key.job_id.into(),
            stage: key.stage as u64,
            partition: key.partition as u64,
            attempt: key.attempt as u64,
            channel: key.channel as u64,
        };
        let ticket = {
            let mut buf = Vec::with_capacity(ticket.encoded_len());
            ticket.encode(&mut buf)?;
            buf
        };
        let request = arrow_flight::Ticket {
            ticket: ticket.into(),
        };
        let response = self.inner.get().await?.do_get(request).await?;
        let stream = response.into_inner().map_err(|e| e.into());
        let stream = FlightRecordBatchStream::new_from_flight_data(stream).map_err(|e| e.into());
        Ok(Box::pin(stream) as TaskStreamSource)
    }
}
