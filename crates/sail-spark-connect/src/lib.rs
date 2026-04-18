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
mod streaming;

pub use proto::data_type_json::JsonDataType;

pub mod spark {
    #[expect(clippy::all, clippy::allow_attributes)]
    pub mod connect {
        tonic::include_proto!("spark.connect");
        tonic::include_proto!("spark.connect.serde");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("spark_connect_descriptor");
    }

    #[expect(clippy::doc_markdown)]
    pub mod config {
        include!(concat!(env!("OUT_DIR"), "/spark_config.rs"));
    }
}
