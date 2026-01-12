mod actor;
mod client;
mod debug;
pub(crate) mod entrypoint;
mod event;
mod options;
mod peer_tracker;
mod server;

#[allow(clippy::all)]
mod gen {
    tonic::include_proto!("sail.worker");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_worker_descriptor");
}

pub(crate) use actor::WorkerActor;
pub(crate) use client::WorkerClientSet;
pub(crate) use event::{WorkerEvent, WorkerLocation, WorkerStreamOwner};
pub(crate) use gen::worker_service_client::WorkerServiceClient;
pub(crate) use options::WorkerOptions;
