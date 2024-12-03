mod shuffle_read;
mod shuffle_write;

use std::fmt::Display;

pub(crate) use shuffle_read::ShuffleReadExec;
pub(crate) use shuffle_write::ShuffleWriteExec;

#[allow(clippy::all)]
pub(crate) mod gen {
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

fn write_list_of_lists<T: Display>(
    f: &mut std::fmt::Formatter,
    data: &[Vec<T>],
) -> std::fmt::Result {
    write!(f, "[")?;
    for (i, list) in data.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "[")?;
        for (j, item) in list.iter().enumerate() {
            if j > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, "]")?;
    }
    write!(f, "]")
}
