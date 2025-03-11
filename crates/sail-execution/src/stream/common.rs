use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures::Stream;
use pin_project_lite::pin_project;

use crate::stream::error::TaskStreamResult;

pub trait TaskStream: Stream<Item = TaskStreamResult<RecordBatch>> {
    #[allow(dead_code)]
    fn schema(&self) -> SchemaRef;
}

pin_project! {
    /// This is similar to [`RecordBatchStreamAdapter`].
    ///
    /// [`RecordBatchStreamAdapter`]: datafusion::physical_plan::stream::RecordBatchStreamAdapter
    pub struct TaskStreamAdapter<S> {
        schema: SchemaRef,
        #[pin]
        stream: S,
    }
}

impl<S> TaskStreamAdapter<S> {
    pub fn new(schema: SchemaRef, stream: S) -> Self {
        Self { schema, stream }
    }
}

impl<S> Stream for TaskStreamAdapter<S>
where
    S: Stream<Item = TaskStreamResult<RecordBatch>>,
{
    type Item = TaskStreamResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> TaskStream for TaskStreamAdapter<S>
where
    S: Stream<Item = TaskStreamResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
