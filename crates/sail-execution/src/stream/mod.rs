pub mod error;
pub mod merge;
pub mod reader;
pub mod writer;

#[allow(clippy::all)]
pub mod gen {
    tonic::include_proto!("sail.stream");
}
