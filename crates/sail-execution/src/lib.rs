pub(crate) mod distributed;
pub mod error;
pub mod job;

#[allow(clippy::all)]
pub(crate) mod rpc {
    tonic::include_proto!("sail");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("sail_descriptor");
}
