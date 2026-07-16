mod actor;
mod client;
mod event;
mod gateway;
pub(super) mod job_scheduler;
mod options;
pub(super) mod output;
mod registry;
mod server;
mod task_assigner;
pub(super) mod worker_pool;

#[expect(clippy::allow_attributes)]
mod r#gen {
    tonic::include_proto!("sail.driver");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_driver_descriptor");
}

pub(crate) use actor::DriverActor;
pub(crate) use client::DriverClientSet;
pub(crate) use event::{DriverEvent, TaskStatus};
pub use gateway::{DriverGateway, DriverGatewayOptions};
pub(crate) use r#gen::driver_service_client::DriverServiceClient;
pub use options::DriverOptions;
pub use registry::{DriverHandle, DriverRegistry, DriverRegistryAccessor};
