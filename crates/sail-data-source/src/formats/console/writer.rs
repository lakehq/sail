use std::any::Any;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{plan_err, Result};
use futures::{FutureExt, Stream, StreamExt};

/// A stream that stays active while a background task runs.
/// - First poll: emits an empty batch to signal the operation started
/// - Subsequent polls: stays pending until background task completes
/// - On drop: SpawnedTask automatically aborts the background task
struct BackgroundTaskStream {
    schema: SchemaRef,
    task: Option<SpawnedTask<()>>,
    sent_initial_batch: bool,
}

impl Stream for BackgroundTaskStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // First poll: return an empty batch to signal operation started
        if !self.sent_initial_batch {
            self.sent_initial_batch = true;
            let batch = RecordBatch::new_empty(Arc::clone(&self.schema));
            return Poll::Ready(Some(Ok(batch)));
        }

        // Subsequent polls: wait for background task to complete
        if let Some(ref mut task) = self.task {
            match task.poll_unpin(cx) {
                Poll::Ready(_) => {
                    self.task = None;
                    Poll::Ready(None) // Task done, stream ends
                }
                Poll::Pending => Poll::Pending, // Task running, query stays active
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for BackgroundTaskStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// No Drop impl needed - SpawnedTask automatically aborts on drop

#[derive(Debug)]
pub struct ConsoleSinkExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl ConsoleSinkExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            EmissionType::Final,
            // The node returns no data, so it is bounded.
            Boundedness::Bounded,
        );
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for ConsoleSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for ConsoleSinkExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(child), true) => Ok(Arc::new(ConsoleSinkExec::new(child))),
            _ => plan_err!("{} should have exactly one child", self.name()),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = self.schema();

        // Process the stream in the background and return an empty stream immediately.
        // This prevents deadlock: the caller can poll the returned stream while we
        // consume and print batches asynchronously.
        // Using SpawnedTask instead of tokio::spawn for DataFusion integration and telemetry.
        let task = SpawnedTask::spawn(async move {
            let mut stream = stream;
            let mut i = 0;
            while let Some(batch) = stream.next().await {
                let text = match batch {
                    Ok(batch) => match pretty_format_batches(&[batch]) {
                        Ok(formatted) => format!("{formatted}"),
                        Err(e) => format!("error formatting batch: {e}"),
                    },
                    Err(e) => format!("error: {e}"),
                };
                // Use spawn_blocking to avoid blocking the tokio runtime on stdout I/O
                let batch_num = i;
                let _ = SpawnedTask::spawn_blocking(move || {
                    let mut stdout = std::io::stdout().lock();
                    let _ = writeln!(stdout, "partition {partition} batch {batch_num}");
                    let _ = writeln!(stdout, "{text}");
                })
                .join()
                .await;
                i += 1;
            }
        });

        // Return a stream that stays active while the background task runs.
        // This keeps the streaming query "active" until processing completes or is stopped.
        Ok(Box::pin(BackgroundTaskStream {
            schema,
            task: Some(task),
            sent_initial_batch: false,
        }))
    }
}
