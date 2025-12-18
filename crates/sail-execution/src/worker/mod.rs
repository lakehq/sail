mod actor;
mod client;
mod debug;
pub(crate) mod entrypoint;
mod event;
mod flight_server;
mod options;
mod server;

#[allow(clippy::all)]
mod gen {
    tonic::include_proto!("sail.worker");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_worker_descriptor");
}

pub(crate) use actor::WorkerActor;
pub(crate) use client::WorkerClient;
pub(crate) use event::{WorkerEvent, WorkerLocation};
pub(crate) use gen::worker_service_client::WorkerServiceClient;
pub(crate) use options::WorkerOptions;
