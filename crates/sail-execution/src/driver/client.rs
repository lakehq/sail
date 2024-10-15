use tonic::transport::Channel;

use crate::driver::rpc::driver_service_client::DriverServiceClient;
use crate::driver::rpc::RegisterWorkerRequest;
use crate::error::ExecutionResult;

pub(crate) struct DriverHandle {
    host: String,
    port: u16,
    client: DriverServiceClient<Channel>,
}

impl DriverHandle {
    pub async fn connect(host: String, port: u16) -> ExecutionResult<Self> {
        let client = DriverServiceClient::connect(format!("http://{}:{}", host, port)).await?;
        Ok(Self { host, port, client })
    }

    pub async fn register_worker(
        &mut self,
        worker_id: &str,
        host: &str,
        port: u16,
    ) -> ExecutionResult<()> {
        let request = tonic::Request::new(RegisterWorkerRequest {
            worker_id: worker_id.to_string(),
            host: host.to_string(),
            port: port as i32,
        });
        self.client.register_worker(request).await?;
        Ok(())
    }
}
