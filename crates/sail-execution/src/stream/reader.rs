use std::fmt;
use std::pin::Pin;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use futures::Stream;

use crate::stream::error::TaskStreamResult;

#[tonic::async_trait]
pub trait TaskStreamReader: fmt::Debug + Send + Sync {
    async fn open(&self, partition: usize) -> Result<TaskStreamSource>;

    /// Gives up a partition that will not be read, so that its producers see
    /// the channel as closed instead of holding its data until stage cleanup.
    async fn release(&self, _partition: usize) -> Result<()> {
        Ok(())
    }
}

pub type TaskStreamSource = Pin<Box<dyn Stream<Item = TaskStreamResult<RecordBatch>> + Send>>;
