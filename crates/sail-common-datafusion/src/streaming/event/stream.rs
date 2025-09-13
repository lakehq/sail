use std::pin::Pin;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use futures::Stream;
use pin_project_lite::pin_project;

use crate::streaming::event::FlowEvent;

pub trait FlowEventStream: Stream<Item = Result<FlowEvent>> {
    /// The schema of the data batches in the stream.
    fn schema(&self) -> SchemaRef;
}

pub type SendableFlowEventStream = Pin<Box<dyn FlowEventStream + Send>>;

pin_project! {
    pub struct FlowEventStreamAdapter<S> {
        schema: SchemaRef,
        #[pin]
        stream: S,
    }
}

impl<S> FlowEventStreamAdapter<S> {
    pub fn new(schema: SchemaRef, stream: S) -> Self {
        Self { schema, stream }
    }
}

impl<S> FlowEventStream for FlowEventStreamAdapter<S>
where
    S: Stream<Item = Result<FlowEvent>> + Send + 'static,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl<S> Stream for FlowEventStreamAdapter<S>
where
    S: Stream<Item = Result<FlowEvent>> + Send + 'static,
{
    type Item = Result<FlowEvent>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
