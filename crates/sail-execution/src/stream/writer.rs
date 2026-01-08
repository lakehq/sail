use std::fmt;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use tokio::sync::mpsc;

use crate::id::{TaskStreamKey, TaskStreamKeyDenseDisplay};
use crate::stream::error::TaskStreamResult;
#[derive(Debug, Clone)]
pub enum TaskWriteLocation {
    Local {
        storage: LocalStreamStorage,
        key: TaskStreamKey,
    },
    Remote {
        uri: String,
        key: TaskStreamKey,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum LocalStreamStorage {
    Memory {
        replicas: usize,
    },
    #[expect(unused)]
    Disk,
}

impl fmt::Display for TaskWriteLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TaskWriteLocation::Local { key, storage } => {
                write!(f, "Local({storage}, {})", TaskStreamKeyDenseDisplay(key))
            }
            TaskWriteLocation::Remote { uri, key } => {
                write!(f, "Remote({uri}, {})", TaskStreamKeyDenseDisplay(key))
            }
        }
    }
}

impl fmt::Display for LocalStreamStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Memory { replicas } => write!(f, "Memory({replicas})"),
            Self::Disk => write!(f, "Disk"),
        }
    }
}

#[tonic::async_trait]
pub trait TaskStreamWriter: fmt::Debug + Send + Sync {
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
