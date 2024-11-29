mod application;
mod cli;

// Same default as Spark
// https://github.com/apache/spark/blob/9cec3c4f7c1b467023f0eefff69e8b7c5105417d/python/pyspark/sql/connect/client/core.py#L126
pub const GRPC_MAX_MESSAGE_LENGTH_DEFAULT: usize = 128 * 1024 * 1024;

pub use application::*;
pub use cli::*;
