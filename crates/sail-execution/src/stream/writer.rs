use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStreamWriteState {
    Active,
    Closed,
}

#[tonic::async_trait]
pub trait TaskStreamWriter: fmt::Debug + Send + Sync {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>>;
}

/// A sink for one shuffle task partition and all of its output channels.
#[tonic::async_trait]
pub trait TaskStreamSink: Send {
    async fn write(&mut self, channel: usize, batch: RecordBatch) -> Result<TaskStreamWriteState>;
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn abort(self: Box<Self>) -> Result<()>;

    async fn fail(self: Box<Self>, _error: Arc<DataFusionError>) -> Result<()> {
        self.abort().await
    }
}

/// A physical sink for exactly one channel of the task stream.
#[tonic::async_trait]
pub trait TaskStreamChannelSink: Send {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState>;
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn abort(self: Box<Self>) -> Result<()>;

    async fn fail(self: Box<Self>, _error: Arc<DataFusionError>) -> Result<()> {
        self.abort().await
    }
}
