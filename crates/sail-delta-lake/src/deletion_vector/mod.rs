//! Deletion Vector support for Delta Lake Merge-on-Read operations.

mod bitmap;
mod resolve;
mod storage;
pub(crate) mod z85;

pub use bitmap::DeletionVectorBitmap;
pub use resolve::resolve_dv_absolute_path;
pub use storage::{read_deletion_vector, write_deletion_vector, DeletionVectorWriter};
