mod shuffle_read;
mod shuffle_write;
mod stage_input;

pub(crate) use shuffle_read::ShuffleReadExec;
pub(crate) use shuffle_write::{ShufflePartitioning, ShuffleWriteExec};
pub(crate) use stage_input::StageInputExec;

pub(crate) mod r#gen {
    tonic::include_proto!("sail.plan");
}

/// The way in which a shuffle stream is consumed by downstream tasks.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ShuffleConsumption {
    /// Each shuffle stream is consumed by a single downstream tasks.
    Single,
    /// Each shuffle stream is consumed by multiple downstream tasks.
    Multiple,
}
