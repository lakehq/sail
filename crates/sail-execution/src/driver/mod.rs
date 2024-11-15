mod actor;
mod client;
mod event;
mod options;
mod planner;
mod server;
pub(crate) mod state;

#[allow(clippy::all)]
mod gen {
    tonic::include_proto!("sail.driver");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_driver_descriptor");
}

pub(crate) use actor::DriverActor;
pub(crate) use client::DriverClient;
pub(crate) use event::DriverEvent;
pub(crate) use gen::driver_service_client::DriverServiceClient;
pub(crate) use options::{DriverOptions, WorkerManagerOptions};
