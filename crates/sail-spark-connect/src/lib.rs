use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod config;
mod debug;
pub mod entrypoint;
mod error;
mod executor;
mod proto;
mod schema;
pub mod server;
mod service;
mod session;

const SPARK_VERSION: &str = "3.5.1";

pub mod spark {
    #[allow(clippy::all)]
    pub mod connect {
        tonic::include_proto!("spark.connect");
        tonic::include_proto!("spark.connect.serde");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("spark_connect_descriptor");
    }

    #[allow(clippy::doc_markdown)]
    pub mod config {
        include!(concat!(env!("OUT_DIR"), "/spark_config.rs"));
    }
}
