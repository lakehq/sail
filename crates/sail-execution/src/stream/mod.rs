pub mod error;
pub mod merge;
pub mod reader;
pub mod writer;

pub mod r#gen {
    tonic::include_proto!("sail.stream");
}
