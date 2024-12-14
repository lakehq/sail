use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::{select_all, SelectAll};
use futures::Stream;

pub struct MergedRecordBatchStream {
    schema: SchemaRef,
    stream: Pin<Box<SelectAll<SendableRecordBatchStream>>>,
}

impl MergedRecordBatchStream {
    pub fn new(schema: SchemaRef, streams: Vec<SendableRecordBatchStream>) -> Self {
        Self {
            schema,
            stream: Box::pin(select_all(streams)),
        }
    }
}

impl Stream for MergedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for MergedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
