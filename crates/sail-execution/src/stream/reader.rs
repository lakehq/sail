use std::fmt::{Debug, Display};
use std::pin::Pin;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use futures::Stream;

use crate::id::WorkerId;
use crate::stream::channel::ChannelName;
use crate::stream::error::TaskStreamResult;

#[derive(Debug, Clone)]
pub enum TaskReadLocation {
    Worker {
        worker_id: WorkerId,
        channel: ChannelName,
    },
    Remote {
        uri: String,
    },
}

impl Display for TaskReadLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TaskReadLocation::Worker { worker_id, channel } => {
                write!(f, "Worker({worker_id}, {channel})")
            }
            TaskReadLocation::Remote { uri } => write!(f, "Remote({uri})"),
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

pub type TaskStreamSource = Pin<Box<dyn Stream<Item = TaskStreamResult<RecordBatch>> + Send>>;
