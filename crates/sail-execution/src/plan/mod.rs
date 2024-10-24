mod shuffle_read;
mod shuffle_write;

#[allow(unused_imports)]
pub(crate) use shuffle_read::ShuffleReadExec;
#[allow(unused_imports)]
pub(crate) use shuffle_write::ShuffleWriteExec;

#[allow(clippy::all)]
pub(crate) mod gen {
    tonic::include_proto!("sail.plan");
}
