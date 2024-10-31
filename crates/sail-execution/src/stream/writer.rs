use std::fmt::{Debug, Display};
use std::io::Write;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use tokio::sync::mpsc;

use crate::stream::ChannelName;

#[derive(Debug, Clone)]
pub enum TaskWriteLocation {
    Memory { channel: ChannelName },
    Disk { channel: ChannelName },
    Remote { uri: String },
}

impl Display for TaskWriteLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TaskWriteLocation::Memory { channel } => write!(f, "Memory({})", channel),
            TaskWriteLocation::Disk { channel } => write!(f, "Disk({})", channel),
            TaskWriteLocation::Remote { uri } => write!(f, "Remote({})", uri),
        }
    }
}

#[tonic::async_trait]
pub trait TaskStreamWriter: Debug + Send + Sync {
    async fn open(
        &self,
        location: &TaskWriteLocation,
        schema: SchemaRef,
    ) -> Result<Box<dyn RecordBatchStreamWriter>>;
}

#[tonic::async_trait]
pub trait RecordBatchStreamWriter: Send {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn close(self: Box<Self>) -> Result<()>;
}

#[tonic::async_trait]
impl<W: Write + Send> RecordBatchStreamWriter for StreamWriter<W> {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        Ok(self.write(batch)?)
    }

    fn close(mut self: Box<Self>) -> Result<()> {
        Ok(self.finish()?)
    }
}

#[tonic::async_trait]
impl RecordBatchStreamWriter for mpsc::Sender<RecordBatch> {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.send(batch.clone())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
