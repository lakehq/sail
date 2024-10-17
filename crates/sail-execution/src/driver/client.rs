use tonic::transport::Channel;

use crate::driver::rpc::driver_service_client::DriverServiceClient;
use crate::driver::rpc::RegisterWorkerRequest;
use crate::error::ExecutionResult;
use crate::id::WorkerId;

pub struct DriverClient {
    inner: DriverServiceClient<Channel>,
}

impl DriverClient {
    pub async fn connect(host: &str, port: u16, tls: bool) -> ExecutionResult<Self> {
        let scheme = if tls { "https" } else { "http" };
        let url = format!("{}://{}:{}", scheme, host, port);
        let inner = DriverServiceClient::connect(url).await?;
        Ok(Self { inner })
    }

    pub async fn register_worker(
        &mut self,
        worker_id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<()> {
        let request = tonic::Request::new(RegisterWorkerRequest {
            worker_id: worker_id.into(),
            host,
            port: port as u32,
        });
        self.inner.register_worker(request).await?;
        Ok(())
    }
}
