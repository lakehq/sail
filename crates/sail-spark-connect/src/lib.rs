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
mod session_manager;

const SPARK_VERSION: &str = "3.5.4";

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
