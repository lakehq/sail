use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use futures::stream::{SelectAll, select_all};
use futures::{Stream, StreamExt};

use crate::stream::reader::TaskStreamSource;

pub struct MergedRecordBatchStream {
    schema: SchemaRef,
    stream: Pin<Box<SelectAll<TaskStreamSource>>>,
}

impl MergedRecordBatchStream {
    pub fn new(schema: SchemaRef, streams: Vec<TaskStreamSource>) -> Self {
        Self {
            schema,
            stream: Box::pin(select_all(streams)),
        }
    }
}

impl Stream for MergedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream
            .as_mut()
            .poll_next(cx)
            .map(|x| x.map(|item| item.map_err(|e| DataFusionError::External(Box::new(e)))))
    }
}

impl RecordBatchStream for MergedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub(crate) fn merged_stream(schema: SchemaRef, streams: Vec<TaskStreamSource>) -> TaskStreamSource {
    Box::pin(MergedRecordBatchStream::new(schema, streams).map(|item| {
        item.map_err(|error| crate::stream::error::TaskStreamError::External(Arc::new(error)))
    }))
}
