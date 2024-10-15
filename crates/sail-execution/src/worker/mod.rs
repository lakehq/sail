pub(crate) mod client;
pub(crate) mod engine;
pub(crate) mod server;
mod state;

#[allow(clippy::all)]
pub(crate) mod rpc {
    tonic::include_proto!("sail.worker");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_worker_descriptor");
}
