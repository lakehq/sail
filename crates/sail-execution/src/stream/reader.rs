use std::fmt;
use std::pin::Pin;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use futures::Stream;

use crate::stream::error::TaskStreamResult;

#[tonic::async_trait]
pub trait TaskStreamReader: fmt::Debug + Send + Sync {
    async fn open(&self, partition: usize) -> Result<TaskStreamSource>;
}

pub type TaskStreamSource = Pin<Box<dyn Stream<Item = TaskStreamResult<RecordBatch>> + Send>>;
