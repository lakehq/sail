use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_datafusion_err;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use prost::Message;
use tonic::transport::Channel;

use crate::error::ExecutionResult;
use crate::id::TaskId;
use crate::rpc::{ClientHandle, ClientOptions};
use crate::stream::ChannelName;
use crate::worker::gen::worker_service_client::WorkerServiceClient;
use crate::worker::gen::{
    RemoveStreamRequest, RemoveStreamResponse, RunTaskRequest, RunTaskResponse, StopTaskRequest,
    StopTaskResponse, StopWorkerRequest, StopWorkerResponse, TaskStreamTicket,
};

#[derive(Debug, Clone)]
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
        channel: Option<ChannelName>,
    ) -> ExecutionResult<()> {
        let request = RunTaskRequest {
            task_id: task_id.into(),
            attempt: attempt as u64,
            plan,
            partition: partition as u64,
            channel: channel.map(|x| x.into()),
        };
        let response = self.client.get().await?.run_task(request).await?;
        let RunTaskResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn stop_task(&self, task_id: TaskId, attempt: usize) -> ExecutionResult<()> {
        let request = StopTaskRequest {
            task_id: task_id.into(),
            attempt: attempt as u64,
        };
        let response = self.client.get().await?.stop_task(request).await?;
        let StopTaskResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn fetch_task_stream(
        &self,
        channel: ChannelName,
        schema: SchemaRef,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let ticket = TaskStreamTicket {
            channel: channel.into(),
        };
        let ticket = {
            let mut buf = Vec::with_capacity(ticket.encoded_len());
            ticket.encode(&mut buf)?;
            buf
        };
        let request = arrow_flight::Ticket {
            ticket: ticket.into(),
        };
        let response = self.flight_client.get().await?.do_get(request).await?;
        let stream = response.into_inner().map_err(|e| e.into());
        let stream = FlightRecordBatchStream::new_from_flight_data(stream)
            .map_err(|e| exec_datafusion_err!("{e}"));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    pub async fn remove_stream(&self, channel_prefix: String) -> ExecutionResult<()> {
        let request = RemoveStreamRequest { channel_prefix };
        let response = self.client.get().await?.remove_stream(request).await?;
        let RemoveStreamResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn stop_worker(&self) -> ExecutionResult<()> {
        let request = StopWorkerRequest {};
        let response = self.client.get().await?.stop_worker(request).await?;
        let StopWorkerResponse {} = response.into_inner();
        Ok(())
    }
}
