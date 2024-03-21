mod error;
mod executor;
mod expression;
mod extension;
mod plan;
mod schema;
pub mod server;
mod service;
mod session;

const SPARK_VERSION: &str = "3.5.0";

pub mod spark {
    pub mod connect {
        tonic::include_proto!("spark.connect");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("spark_connect_descriptor");
    }
}
