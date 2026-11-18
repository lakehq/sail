// SPDX-License-Identifier: Apache-2.0

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
    /// released. This is required when the tasks that produce the partition keep its
    /// data for a reader, and only learn from the reader that the data is not needed.
    fn close_unopened(&self) -> bool {
        false
    }

    /// Releases a partition that will not be opened. No data is fetched for it.
    async fn release(&self, _partition: usize) -> Result<()> {
        Ok(())
    }
}

pub type TaskStreamSource = Pin<Box<dyn Stream<Item = TaskStreamResult<RecordBatch>> + Send>>;
