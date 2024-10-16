mod client;
mod engine;
mod flight_server;
mod server;
mod state;

#[allow(clippy::all)]
mod rpc {
    tonic::include_proto!("sail.worker");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_worker_descriptor");
}

pub(crate) use client::WorkerHandle;
pub use engine::WorkerEngine;
