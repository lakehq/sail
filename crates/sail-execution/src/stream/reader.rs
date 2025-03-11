use std::fmt::{Debug, Display};
use std::pin::Pin;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;

use crate::id::WorkerId;
use crate::stream::channel::ChannelName;
use crate::stream::common::TaskStream;

#[derive(Debug, Clone)]
pub enum TaskReadLocation {
    Worker {
        worker_id: WorkerId,
        host: String,
        port: u16,
        channel: ChannelName,
    },
    Remote {
        uri: String,
    },
}

impl Display for TaskReadLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TaskReadLocation::Worker {
                worker_id,
                host,
                port,
                channel,
            } => write!(f, "Worker({}, {}:{}, {})", worker_id, host, port, channel),
            TaskReadLocation::Remote { uri } => write!(f, "Remote({})", uri),
        }
    }
}

#[tonic::async_trait]
pub trait TaskStreamReader: Debug + Send + Sync {
    async fn open(
        &self,
        location: &TaskReadLocation,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource>;
}

/// This is similar to [`SendableRecordBatchStream`].
///
/// [`SendableRecordBatchStream`]: datafusion::execution::SendableRecordBatchStream
pub type TaskStreamSource = Pin<Box<dyn TaskStream + Send>>;
