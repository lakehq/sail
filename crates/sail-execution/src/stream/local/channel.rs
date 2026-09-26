use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;

use crate::stream::error::TaskStreamResult;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// A single-consumer stream that retains only unread batches.
pub(super) struct ChannelStreamWriter {
    sender: mpsc::UnboundedSender<TaskStreamResult<RecordBatch>>,
}

impl ChannelStreamWriter {
    pub fn new() -> (Self, TaskStreamSource) {
        // A writer must not block on a channel that its consumer may leave unread
        // (for example, a join with an empty build side). This has the same unread
        // buffering behavior as the previous in-memory overflow queues.
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self { sender },
            Box::pin(UnboundedReceiverStream::new(receiver)),
        )
    }
}

#[tonic::async_trait]
impl TaskStreamChannelSink for ChannelStreamWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        Ok(if self.sender.send(Ok(batch)).is_ok() {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
