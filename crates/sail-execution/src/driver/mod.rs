mod actor;
mod client;
mod event;
pub(super) mod job_scheduler;
mod options;
pub(super) mod output;
mod planner;
mod server;
pub(super) mod worker_pool;

#[allow(clippy::all)]
mod gen {
    tonic::include_proto!("sail.driver");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_driver_descriptor");
}

pub(crate) use actor::DriverActor;
pub(crate) use client::DriverClient;
pub(crate) use event::{DriverEvent, TaskStatus};
pub(crate) use gen::driver_service_client::DriverServiceClient;
pub use options::DriverOptions;
pub(crate) use options::WorkerManagerOptions;
