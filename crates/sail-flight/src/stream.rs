use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use sail_telemetry::metrics::{MetricAttribute, MetricRegistry};

pub struct MetricContext {
    pub statement_type: &'static str,
}

pub struct MetricsRecordingStream {
    inner: SendableRecordBatchStream,
    metrics: Arc<MetricRegistry>,
    context: MetricContext,
    start: Instant,
    rows: u64,
    status: &'static str,
}

impl MetricsRecordingStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        metrics: Arc<MetricRegistry>,
        context: MetricContext,
    ) -> Self {
        let type_attr = (
            MetricAttribute::FLIGHT_STATEMENT_TYPE,
            Cow::Borrowed(context.statement_type),
        );
        metrics
            .flight_statement_active_count
            .adder(1i64)
            .with_attribute(type_attr)
            .emit();
        Self {
            inner,
            metrics,
            context,
            start: Instant::now(),
            rows: 0,
            status: "incomplete",
        }
    }
}

impl Drop for MetricsRecordingStream {
    fn drop(&mut self) {
        let type_attr = (
            MetricAttribute::FLIGHT_STATEMENT_TYPE,
            Cow::Borrowed(self.context.statement_type),
        );
        let status_attr = (
            MetricAttribute::FLIGHT_STATEMENT_STATUS,
            Cow::Borrowed(self.status),
        );
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics
            .flight_statement_active_count
            .adder(-1i64)
            .with_attribute(type_attr.clone())
            .emit();
        self.metrics
            .flight_statement_total_count
            .adder(1u64)
            .with_attribute(type_attr.clone())
            .emit();
        self.metrics
            .flight_statement_duration
            .recorder(duration)
            .with_attribute(type_attr.clone())
            .with_attribute(status_attr)
            .emit();
        self.metrics
            .flight_statement_row_count
            .recorder(self.rows)
            .with_attribute(type_attr)
            .emit();
    }
}

impl Stream for MetricsRecordingStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: `MetricsRecordingStream` is `Unpin` because all its fields are `Unpin`
        // (`Pin<Box<T>>` is always `Unpin`), so using `mut self` and calling `as_mut()` on
        // the inner `Pin<Box<...>>` is sound without requiring pin projection.
        match self.inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.status = "success";
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                self.status = "error";
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Some(Ok(batch))) => {
                self.rows += batch.num_rows() as u64;
                Poll::Ready(Some(Ok(batch)))
            }
        }
    }
}

impl RecordBatchStream for MetricsRecordingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
