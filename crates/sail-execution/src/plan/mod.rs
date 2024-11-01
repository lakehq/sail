mod shuffle_read;
mod shuffle_write;

use std::fmt::Display;

pub(crate) use shuffle_read::ShuffleReadExec;
pub(crate) use shuffle_write::ShuffleWriteExec;

#[allow(clippy::all)]
pub(crate) mod gen {
    tonic::include_proto!("sail.plan");
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
