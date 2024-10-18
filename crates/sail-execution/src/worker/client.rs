use tonic::transport::Channel;

use crate::error::ExecutionResult;
use crate::worker::rpc::worker_service_client::WorkerServiceClient;

pub struct WorkerClient {
    inner: WorkerServiceClient<Channel>,
}

impl WorkerClient {
    pub async fn connect(host: &str, port: u16, tls: bool) -> ExecutionResult<Self> {
        let scheme = if tls { "https" } else { "http" };
        let url = format!("{}://{}:{}", scheme, host, port);
        let inner = WorkerServiceClient::connect(url).await?;
        Ok(Self { inner })
    }
}
