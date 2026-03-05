use std::fmt;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::error::DataFusionError;

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

/// A sink that can write record batches to a task stream.
#[tonic::async_trait]
pub trait TaskStreamSink: Send {
    /// Write a record batch or an error to the sink.
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> TaskStreamSinkState;
    // TODO: Are we required to call `close` when the user encounters an error and wants to
    //   abort the sink? Should we have a separate `abort` method?
    /// Flush all data and close the sink.
    /// Since the operation may be async, the user must call this method before
    /// dropping the sink, unless the user has received [`TaskStreamSinkState::Error`] or
    /// [`TaskStreamSinkState::Closed`] state from [`write`](TaskStreamSink::write).
    /// No implicit finalization will be done when the sink is dropped.
    async fn close(self: Box<Self>) -> Result<()>;
}

pub enum TaskStreamSinkState {
    /// The sink is ready to accept more writes.
    Ok,
    /// The sink has encountered an error and no further writes should be attempted.
    #[expect(unused)]
    Error(DataFusionError),
    /// The sink has been closed and no further writes should be attempted.
    Closed,
}
