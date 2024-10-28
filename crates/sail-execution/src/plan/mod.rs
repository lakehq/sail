mod shuffle_read;
mod shuffle_write;

pub(crate) use shuffle_read::{ShuffleReadExec, ShuffleReadLocation};
pub(crate) use shuffle_write::{ShuffleWriteExec, ShuffleWriteLocation};

#[allow(clippy::all)]
pub(crate) mod gen {
    tonic::include_proto!("sail.plan");
}
