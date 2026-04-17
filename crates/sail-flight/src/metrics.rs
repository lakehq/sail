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

#[derive(Debug, Clone, Copy)]
pub enum StatementType {
    Query,
    Command,
}

impl StatementType {
    pub fn name(&self) -> &'static str {
        match self {
            StatementType::Query => "query",
            StatementType::Command => "command",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StatementStatus {
    Incomplete,
    Success,
    Error,
}

impl StatementStatus {
    pub fn name(&self) -> &'static str {
        match self {
            StatementStatus::Incomplete => "incomplete",
            StatementStatus::Success => "success",
            StatementStatus::Error => "error",
        }
    }
}

pub struct MetricsRecordingContext {
    pub statement_type: StatementType,
}

pub struct MetricsRecordingStream {
    inner: SendableRecordBatchStream,
    metrics: Arc<MetricRegistry>,
    context: MetricsRecordingContext,
    start: Instant,
    rows: u64,
    status: StatementStatus,
}

impl MetricsRecordingStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        metrics: Arc<MetricRegistry>,
        context: MetricsRecordingContext,
    ) -> Self {
        let type_attr = (
            MetricAttribute::FLIGHT_STATEMENT_TYPE,
            Cow::Borrowed(context.statement_type.name()),
        );
        metrics
            .flight_statement_active_count
            .adder(1i64)
            .with_attribute(type_attr.clone())
            .emit();
        metrics
            .flight_statement_total_count
            .adder(1u64)
            .with_attribute(type_attr)
            .emit();
        Self {
            inner,
            metrics,
            context,
            start: Instant::now(),
            rows: 0,
            status: StatementStatus::Incomplete,
        }
    }
}

impl Drop for MetricsRecordingStream {
    fn drop(&mut self) {
        let type_attr = (
            MetricAttribute::FLIGHT_STATEMENT_TYPE,
            Cow::Borrowed(self.context.statement_type.name()),
        );
        let status_attr = (
            MetricAttribute::FLIGHT_STATEMENT_STATUS,
            Cow::Borrowed(self.status.name()),
        );
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics
            .flight_statement_active_count
            .adder(-1i64)
            .with_attribute(type_attr.clone())
            .emit();
        self.metrics
            .flight_statement_duration
            .recorder(duration)
            .with_attribute(type_attr.clone())
            .with_attribute(status_attr.clone())
            .emit();
        self.metrics
            .flight_statement_row_count
            .recorder(self.rows)
            .with_attribute(type_attr)
            .with_attribute(status_attr)
            .emit();
    }
}

impl Stream for MetricsRecordingStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                if matches!(self.status, StatementStatus::Incomplete) {
                    self.status = StatementStatus::Success;
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                if matches!(self.status, StatementStatus::Incomplete) {
                    self.status = StatementStatus::Error;
                }
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
