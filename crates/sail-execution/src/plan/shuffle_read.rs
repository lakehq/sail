use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce::LimitedBatchCoalescer;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use tokio::runtime::Handle;

use crate::stream::reader::{TaskStreamReader, TaskStreamSource};

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

/// Opens the input of one partition when it is first polled.
///
/// An operator can finish without reading one of its inputs (for example, a hash join
/// with an empty build side never polls its probe side). If the input is dropped before
/// it has been opened, it is still opened and closed in the background when the reader
/// requires so. An open call that is in progress is completed rather than repeated,
/// so that each stream of the partition is requested exactly once.
struct ShuffleReadOpen {
    close_unopened: bool,
    open: Option<BoxFuture<'static, Result<TaskStreamSource>>>,
}

impl Future for ShuffleReadOpen {
    type Output = Result<TaskStreamSource>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(open) = self.open.as_mut() else {
            return Poll::Ready(internal_err!("shuffle input is opened more than once"));
        };
        let output = ready!(open.as_mut().poll(cx));
        self.open = None;
        Poll::Ready(output)
    }
}

impl Drop for ShuffleReadOpen {
    fn drop(&mut self) {
        if !self.close_unopened {
            return;
        }
        let Some(open) = self.open.take() else {
            return;
        };
        // This is best-effort. If the input cannot be closed here, its data is
        // released when the streams of the stage are cleaned up.
        // The spawned task is not cancelled along with the task that owned the input,
        // but it takes no longer than the open call of a normal read.
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                let _ = open.await;
            });
        }
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
        let reader = self.reader.clone();
        let output = futures::stream::once(ShuffleReadOpen {
            close_unopened: reader.close_unopened(),
            open: Some(Box::pin(async move { reader.open(partition).await })),
        })
        .map_ok(|source| {
            source.map(|item| {
                item.map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))
            })
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use futures::Stream;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::stream::error::TaskStreamResult;

    #[derive(Debug)]
    struct TestReader {
        close_unopened: bool,
        /// The permits that `open` waits for, if any.
        permits: Option<Semaphore>,
        started: AtomicUsize,
        opened: AtomicUsize,
        closed: Arc<AtomicUsize>,
    }

    impl TestReader {
        fn new(close_unopened: bool, permits: Option<Semaphore>) -> Arc<Self> {
            Arc::new(Self {
                close_unopened,
                permits,
                started: AtomicUsize::new(0),
                opened: AtomicUsize::new(0),
                closed: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn counts(&self) -> (usize, usize, usize) {
            (
                self.started.load(Ordering::SeqCst),
                self.opened.load(Ordering::SeqCst),
                self.closed.load(Ordering::SeqCst),
            )
        }
    }

    #[tonic::async_trait]
    impl TaskStreamReader for TestReader {
        async fn open(&self, _partition: usize) -> Result<TaskStreamSource> {
            self.started.fetch_add(1, Ordering::SeqCst);
            if let Some(permits) = &self.permits {
                permits
                    .acquire()
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                    .forget();
            }
            self.opened.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(TestSource {
                closed: self.closed.clone(),
            }))
        }

        fn close_unopened(&self) -> bool {
            self.close_unopened
        }
    }

    /// An empty source that counts how many times it is dropped.
    struct TestSource {
        closed: Arc<AtomicUsize>,
    }

    impl Stream for TestSource {
        type Item = TaskStreamResult<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }

    impl Drop for TestSource {
        fn drop(&mut self) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn execute(reader: &Arc<TestReader>) -> Result<SendableRecordBatchStream> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        ShuffleReadExec::new(reader.clone(), Arc::new(properties))
            .execute(0, Arc::new(TaskContext::default()))
    }

    async fn wait_until_closed(reader: &TestReader) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            while reader.closed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    }

    #[tokio::test]
    async fn closes_only_unopened_input_on_drop() -> Result<()> {
        // An input that has been read is not opened again.
        let read = TestReader::new(true, None);
        let mut stream = execute(&read)?;
        assert!(stream.try_next().await?.is_none());
        drop(stream);

        // An input that does not need to be closed is never opened.
        let unaffected = TestReader::new(false, None);
        drop(execute(&unaffected)?);

        // An input that is dropped outside the runtime is left to stage cleanup.
        let outside = TestReader::new(true, None);
        let stream = execute(&outside)?;
        let _ = std::thread::spawn(move || drop(stream)).join();

        let unopened = TestReader::new(true, None);
        drop(execute(&unopened)?);
        wait_until_closed(&unopened).await?;

        assert_eq!(unopened.counts(), (1, 1, 1));
        assert_eq!(read.counts(), (1, 1, 1));
        assert_eq!(unaffected.counts(), (0, 0, 0));
        assert_eq!(outside.counts(), (0, 0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn completes_the_open_in_progress_on_drop() -> Result<()> {
        let reader = TestReader::new(true, Some(Semaphore::new(0)));
        let mut stream = execute(&reader)?;
        assert!(futures::poll!(stream.next()).is_pending());
        assert_eq!(reader.counts(), (1, 0, 0));
        drop(stream);

        if let Some(permits) = &reader.permits {
            permits.add_permits(1);
        }
        wait_until_closed(&reader).await?;
        // The open call that had started is the one that completes.
        assert_eq!(reader.counts(), (1, 1, 1));
        Ok(())
    }
}
