mod shuffle_read;
mod shuffle_write;
mod stage_input;

use std::fmt::Display;

pub(crate) use shuffle_read::ShuffleReadExec;
pub(crate) use shuffle_write::ShuffleWriteExec;
pub(crate) use stage_input::StageInputExec;

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

struct ListListDisplay<'a, T: Display>(pub &'a [Vec<T>]);

impl<'a, T: Display> Display for ListListDisplay<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, list) in self.0.iter().enumerate() {
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
}
