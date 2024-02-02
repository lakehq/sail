mod error;
mod expression;
mod plan;
mod schema;
pub mod server;
mod session;

pub mod spark {
    pub mod connect {
        tonic::include_proto!("spark.connect");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("spark_connect_descriptor");
    }
}
