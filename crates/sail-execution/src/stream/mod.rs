pub mod error;
pub mod merge;
pub mod reader;
pub mod writer;

#[allow(clippy::all)]
mod gen {
    tonic::include_proto!("sail.stream");
}

pub(crate) use gen::TaskStreamTicket;
