use std::fmt::Debug;

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;

use crate::id::WorkerId;
use crate::stream::ChannelName;

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

#[tonic::async_trait]
pub trait TaskStreamReader: Debug + Send + Sync {
    async fn open(
        &self,
        location: &TaskReadLocation,
        schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream>;
}
