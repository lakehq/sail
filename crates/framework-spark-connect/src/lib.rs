mod error;
mod executor;
mod proto;
mod schema;
pub mod server;
mod service;
mod session;

const SPARK_VERSION: &str = "3.5.1";

pub mod spark {
    pub mod connect {
        tonic::include_proto!("spark.connect");
        tonic::include_proto!("spark.connect.serde");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("spark_connect_descriptor");
    }
}
