use std::fmt::{Debug, Display};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use tokio::sync::mpsc;

use crate::stream::channel::ChannelName;
use crate::stream::error::TaskStreamResult;

#[derive(Debug, Clone)]
pub enum TaskWriteLocation {
    Local {
        channel: ChannelName,
        storage: LocalStreamStorage,
    },
    Remote {
        uri: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum LocalStreamStorage {
    Ephemeral,
    Memory,
    Disk,
}

impl Display for TaskWriteLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TaskWriteLocation::Local { channel, storage } => {
                write!(f, "Local({}, {})", channel, storage)
            }
            TaskWriteLocation::Remote { uri } => write!(f, "Remote({})", uri),
        }
    }
}

impl Display for LocalStreamStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Ephemeral => write!(f, "Ephemeral"),
            Self::Memory => write!(f, "Memory"),
            Self::Disk => write!(f, "Disk"),
        }
    }
}

#[tonic::async_trait]
pub trait TaskStreamWriter: Debug + Send + Sync {
    async fn open(
        &self,
        location: &TaskWriteLocation,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>>;
}

#[tonic::async_trait]
pub trait TaskStreamSink: Send {
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> Result<()>;
    // TODO: close the sink on drop
    fn close(self: Box<Self>) -> Result<()>;
}

#[tonic::async_trait]
impl TaskStreamSink for mpsc::Sender<TaskStreamResult<RecordBatch>> {
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> Result<()> {
        self.send(batch)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
