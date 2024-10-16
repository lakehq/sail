mod client;
mod engine;
mod server;
mod state;

#[allow(clippy::all)]
mod rpc {
    tonic::include_proto!("sail.driver");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_driver_descriptor");
}

pub(crate) use client::DriverHandle;
pub use engine::DriverEngine;
