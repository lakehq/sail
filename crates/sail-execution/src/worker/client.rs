use arrow::datatypes::SchemaRef;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::common::exec_datafusion_err;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use prost::Message;
use tonic::transport::Channel;

use crate::error::ExecutionResult;
use crate::id::TaskId;
use crate::rpc::{ClientHandle, ClientOptions};
use crate::worker::gen::worker_service_client::WorkerServiceClient;
use crate::worker::gen::{RunTaskRequest, RunTaskResponse, TaskStreamTicket};

#[derive(Clone)]
pub struct WorkerClient {
    client: ClientHandle<WorkerServiceClient<Channel>>,
    flight_client: ClientHandle<FlightServiceClient<Channel>>,
}

impl WorkerClient {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            // TODO: share connection for the clients
            client: ClientHandle::new(options.clone()),
            flight_client: ClientHandle::new(options),
        }
    }
}

impl WorkerClient {
    pub async fn run_task(
        &self,
        task_id: TaskId,
        attempt: usize,
        plan: Vec<u8>,
        partition: usize,
    ) -> ExecutionResult<()> {
        let request = RunTaskRequest {
            task_id: task_id.into(),
            attempt: attempt as u64,
            plan,
            partition: partition as u64,
        };
        let response = self.client.lock().await?.run_task(request).await?;
        let RunTaskResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn fetch_task_stream(
        &self,
        task_id: TaskId,
        attempt: usize,
        schema: SchemaRef,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let ticket = TaskStreamTicket {
            task_id: task_id.into(),
            attempt: attempt as u64,
        };
        let ticket = {
            let mut buf = Vec::with_capacity(ticket.encoded_len());
            ticket.encode(&mut buf)?;
            buf
        };
        let request = arrow_flight::Ticket {
            ticket: ticket.into(),
        };
        let response = self.flight_client.lock().await?.do_get(request).await?;
        let stream = response.into_inner().map_err(|e| e.into());
        let stream = FlightRecordBatchStream::new_from_flight_data(stream)
            .map_err(|e| exec_datafusion_err!("{e}"));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
