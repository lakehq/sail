use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce::LimitedBatchCoalescer;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use log::debug;
use tokio::runtime::Handle;

use crate::stream::reader::TaskStreamReader;

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
}

impl ShuffleReadExec {
    pub fn new(reader: Arc<dyn TaskStreamReader>, properties: Arc<PlanProperties>) -> Self {
        Self { properties, reader }
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReadExec: partitioning={}",
            self.properties.output_partitioning(),
        )
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("ShuffleReadExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut guard = ReleaseUnopened {
            reader: Some(self.reader.clone()),
            partition,
        };
        let output = futures::stream::once(async move {
            let Some(reader) = guard.reader.take() else {
                return internal_err!("shuffle read partition {partition} opened twice");
            };
            let source = reader.open(partition).await?;
            Ok::<_, datafusion::error::DataFusionError>(source.map(|item| {
                item.map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))
            }))
        })
        .try_flatten();
        let output: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(self.schema(), output));
        // Unbounded inputs must emit partial batches promptly.
        if self.properties.boundedness.is_unbounded() {
            return Ok(output);
        }
        // The reader has merged the producers for this partition. Share one
        // coalescer across them instead of buffering each producer separately.
        let coalescer =
            LimitedBatchCoalescer::new(self.schema(), context.session_config().batch_size(), None);
        let output = futures::stream::try_unfold(
            (output, coalescer, false),
            |(mut input, mut coalescer, mut done)| async move {
                loop {
                    if let Some(batch) = coalescer.next_completed_batch() {
                        return Ok::<_, datafusion::error::DataFusionError>(Some((
                            batch,
                            (input, coalescer, done),
                        )));
                    }
                    if done {
                        return Ok(None);
                    }
                    match input.try_next().await? {
                        Some(batch) => {
                            coalescer.push_batch(batch)?;
                        }
                        None => {
                            coalescer.finish()?;
                            done = true;
                        }
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

/// Releases a partition whose stream is dropped before it is first polled,
/// for example the probe side of a hash join whose build side is empty.
struct ReleaseUnopened {
    reader: Option<Arc<dyn TaskStreamReader>>,
    partition: usize,
}

impl Drop for ReleaseUnopened {
    fn drop(&mut self) {
        let (Some(reader), Ok(handle)) = (self.reader.take(), Handle::try_current()) else {
            return;
        };
        let partition = self.partition;
        handle.spawn(async move {
            if let Err(error) = reader.release(partition).await {
                debug!("failed to release shuffle read partition {partition}: {error}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use tokio::sync::mpsc;

    use super::*;
    use crate::stream::reader::TaskStreamSource;

    #[derive(Debug)]
    enum Event {
        Open(usize),
        Release(usize),
    }

    #[derive(Debug)]
    struct EventReader(mpsc::UnboundedSender<Event>);

    #[tonic::async_trait]
    impl TaskStreamReader for EventReader {
        async fn open(&self, partition: usize) -> Result<TaskStreamSource> {
            let _ = self.0.send(Event::Open(partition));
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn release(&self, partition: usize) -> Result<()> {
            let _ = self.0.send(Event::Release(partition));
            Ok(())
        }
    }

    fn exec() -> (ShuffleReadExec, mpsc::UnboundedReceiver<Event>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(2),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        (
            ShuffleReadExec::new(Arc::new(EventReader(tx)), properties),
            rx,
        )
    }

    async fn next_event(rx: &mut mpsc::UnboundedReceiver<Event>) -> Option<Event> {
        tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn releases_partition_dropped_before_reading() -> Result<()> {
        let (exec, mut rx) = exec();
        drop(exec.execute(1, Arc::new(TaskContext::default()))?);
        assert!(matches!(next_event(&mut rx).await, Some(Event::Release(1))));
        assert!(next_event(&mut rx).await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn does_not_release_partition_that_was_read() -> Result<()> {
        let (exec, mut rx) = exec();
        collect(exec.execute(1, Arc::new(TaskContext::default()))?).await?;
        assert!(matches!(next_event(&mut rx).await, Some(Event::Open(1))));
        assert!(next_event(&mut rx).await.is_none());
        Ok(())
    }
}
