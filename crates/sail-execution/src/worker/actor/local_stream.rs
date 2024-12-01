use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::RecordBatchStreamWriter;

pub(super) trait LocalStream: Send {
    fn publish(&mut self) -> ExecutionResult<Box<dyn RecordBatchStreamWriter>>;
    fn subscribe(&mut self) -> ExecutionResult<SendableRecordBatchStream>;
}

pub(super) struct EphemeralStream {
    tx: Option<mpsc::Sender<RecordBatch>>,
    rx: Option<mpsc::Receiver<RecordBatch>>,
    schema: SchemaRef,
}

impl EphemeralStream {
    pub fn new(buffer: usize, schema: SchemaRef) -> Self {
        let (tx, rx) = mpsc::channel(buffer);
        Self {
            tx: Some(tx),
            rx: Some(rx),
            schema,
        }
    }
}

impl LocalStream for EphemeralStream {
    fn publish(&mut self) -> ExecutionResult<Box<dyn RecordBatchStreamWriter>> {
        let tx = self.tx.take().ok_or_else(|| {
            ExecutionError::InternalError("ephemeral stream can only be written once".to_string())
        })?;
        Ok(Box::new(tx))
    }

    fn subscribe(&mut self) -> ExecutionResult<SendableRecordBatchStream> {
        let rx = self.rx.take().ok_or_else(|| {
            ExecutionError::InternalError("ephemeral stream can only be read once".to_string())
        })?;
        let stream =
            RecordBatchStreamAdapter::new(self.schema.clone(), ReceiverStream::new(rx).map(Ok));
        Ok(Box::pin(stream))
    }
}
