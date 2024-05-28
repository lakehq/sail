mod error;
mod executor;
mod expression;
mod extension;
mod plan;
mod proto;
mod schema;
pub mod server;
mod service;
mod session;
mod sql;
#[cfg(test)]
mod tests;

const SPARK_VERSION: &str = "3.5.1";

pub mod spark {
    pub mod connect {
        tonic::include_proto!("spark.connect");
        tonic::include_proto!("spark.connect.serde");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("spark_connect_descriptor");
    }
}
