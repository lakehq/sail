pub(crate) mod accessor;
pub(crate) mod celeborn;
pub mod error;
pub(crate) mod local;
pub mod merge;
pub mod reader;
pub(crate) mod service;
pub(crate) mod storage;
pub mod writer;

pub mod r#gen {
    tonic::include_proto!("sail.stream");
}
