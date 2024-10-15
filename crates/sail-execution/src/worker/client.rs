use tonic::transport::Channel;

use crate::error::ExecutionResult;
use crate::worker::rpc::worker_service_client::WorkerServiceClient;

pub(crate) struct WorkerHandle {
    id: String,
    host: String,
    port: u16,
    client: WorkerServiceClient<Channel>,
}

impl WorkerHandle {
    pub async fn connect(id: String, host: String, port: u16) -> ExecutionResult<Self> {
        let client = WorkerServiceClient::connect(format!("http://{}:{}", host, port)).await?;
        Ok(Self {
            id,
            host,
            port,
            client,
        })
    }
}
