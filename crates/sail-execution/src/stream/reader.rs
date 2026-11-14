use std::fmt;
use std::pin::Pin;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use futures::Stream;

use crate::stream::error::TaskStreamResult;

#[tonic::async_trait]
pub trait TaskStreamReader: fmt::Debug + Send + Sync {
    async fn open(&self, partition: usize) -> Result<TaskStreamSource>;

    /// Whether a partition that is dropped before it has been opened must still be
    /// opened and closed. This is required when the tasks that produce the partition
    /// keep its data for a reader, and only learn from a closed reader that the data
    /// is not needed.
    fn close_unopened(&self) -> bool {
        false
    }
}

pub type TaskStreamSource = Pin<Box<dyn Stream<Item = TaskStreamResult<RecordBatch>> + Send>>;
