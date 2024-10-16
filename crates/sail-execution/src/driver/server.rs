use std::sync::{Arc, Mutex, MutexGuard};

use log::info;
use tonic::{Request, Response, Status};

use crate::driver::rpc::driver_service_server::DriverService;
use crate::driver::rpc::{RegisterWorkerRequest, RegisterWorkerResponse};
use crate::driver::state::DriverState;
use crate::error::ExecutionResult;
use crate::worker::WorkerHandle;

pub struct DriverServer {
    state: Arc<Mutex<DriverState>>,
}

impl DriverServer {
    pub fn new(state: Arc<Mutex<DriverState>>) -> Self {
        Self { state }
    }

    fn state(&self) -> ExecutionResult<MutexGuard<'_, DriverState>> {
        Ok(self.state.lock()?)
    }
}

#[tonic::async_trait]
impl DriverService for DriverServer {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let RegisterWorkerRequest {
            worker_id,
            host,
            port,
        } = request.into_inner();
        let port = u16::try_from(port).map_err(|_| {
            Status::invalid_argument("port must be a valid 16-bit unsigned integer")
        })?;
        info!("registering worker: {}", worker_id);
        let handle = WorkerHandle::connect(worker_id, host, port).await?;
        let mut state = self.state()?;
        state.add_worker(handle);
        Ok(Response::new(RegisterWorkerResponse {}))
    }
}
