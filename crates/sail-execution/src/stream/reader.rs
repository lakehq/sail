use std::fmt;
use std::pin::Pin;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use futures::Stream;

use crate::id::{TaskStreamKey, TaskStreamKeyDenseDisplay, WorkerId};
use crate::stream::error::TaskStreamResult;
#[derive(Debug, Clone)]
pub enum TaskReadLocation {
    Driver {
        key: TaskStreamKey,
    },
    Worker {
        worker_id: WorkerId,
        key: TaskStreamKey,
    },
    Remote {
        uri: String,
        key: TaskStreamKey,
    },
}

impl fmt::Display for TaskReadLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TaskReadLocation::Driver { key } => {
                write!(f, "Driver({})", TaskStreamKeyDenseDisplay(key))
            }
            TaskReadLocation::Worker { worker_id, key } => {
                write!(f, "Worker({worker_id}, {})", TaskStreamKeyDenseDisplay(key))
            }
            TaskReadLocation::Remote { uri, key } => {
                write!(f, "Remote({uri}, {})", TaskStreamKeyDenseDisplay(key))
            }
        }
    }
}

#[tonic::async_trait]
pub trait TaskStreamReader: fmt::Debug + Send + Sync {
    async fn open(
        &self,
        location: &TaskReadLocation,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource>;
}

pub type TaskStreamSource = Pin<Box<dyn Stream<Item = TaskStreamResult<RecordBatch>> + Send>>;
