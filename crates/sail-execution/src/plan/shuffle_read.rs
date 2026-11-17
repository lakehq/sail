use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
use log::{debug, warn};
use tokio::runtime::Handle;

use crate::stream::reader::{TaskStreamReader, TaskStreamSource};

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
    replicated: bool,
    executed: Arc<[AtomicBool]>,
}

impl ShuffleReadExec {
    pub fn new(
        reader: Arc<dyn TaskStreamReader>,
        properties: Arc<PlanProperties>,
        replicated: bool,
    ) -> Self {
        let partitions = properties.output_partitioning().partition_count();
        Self {
            properties,
            reader,
            replicated,
            executed: (0..partitions).map(|_| AtomicBool::new(false)).collect(),
        }
    }

    /// Releases the partitions of a replicated input that this task never executed,
    /// for example the inputs of the other branches under a union.
    pub fn release_unexecuted(&self) {
        // Producers keep a replica for every consumer task, so one that is never
        // taken holds their output until stage cleanup. Other inputs have one
        // reader per partition, which may belong to another task.
        if !self.replicated {
            return;
        }
        for (partition, executed) in self.executed.iter().enumerate() {
            if !executed.swap(true, Ordering::Relaxed) {
                spawn_release(self.reader.clone(), partition);
            }
        }
    }
}

/// Opens the input of one partition when it is first polled.
///
/// An operator can finish without reading one of its inputs (for example, a hash join
/// with an empty build side never polls its probe side). If the input is dropped before
/// it has been opened, it is released in the background when the reader requires so.
/// An open call that is in progress is completed and its output is closed instead,
/// so that each stream of the partition is requested exactly once.
struct ShuffleReadOpen {
    reader: Arc<dyn TaskStreamReader>,
    partition: usize,
    /// The open call, which starts when the input is first polled.
    open: Option<BoxFuture<'static, Result<TaskStreamSource>>>,
    opened: bool,
}

impl Future for ShuffleReadOpen {
    type Output = Result<TaskStreamSource>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.opened {
            return Poll::Ready(internal_err!("shuffle input is opened more than once"));
        }
        let this = &mut *self;
        let open = this.open.get_or_insert_with(|| {
            let (reader, partition) = (this.reader.clone(), this.partition);
            Box::pin(async move { reader.open(partition).await })
        });
        let output = ready!(open.as_mut().poll(cx));
        self.open = None;
        self.opened = true;
        Poll::Ready(output)
    }
}

impl Drop for ShuffleReadOpen {
    fn drop(&mut self) {
        if self.opened || !self.reader.close_unopened() {
            return;
        }
        // This is best-effort. If the input cannot be released here, its data is
        // released when the streams of the stage are cleaned up.
        // The spawned task is not cancelled along with the task that owned the input,
        // but it takes no longer than the open call of a normal read.
        let Some(open) = self.open.take() else {
            spawn_release(self.reader.clone(), self.partition);
            return;
        };
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
        if let Some(executed) = self.executed.get(partition) {
            executed.store(true, Ordering::Relaxed);
        }
        let output = futures::stream::once(ShuffleReadOpen {
            reader: self.reader.clone(),
            partition,
            open: None,
            opened: false,
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

fn spawn_release(reader: Arc<dyn TaskStreamReader>, partition: usize) {
    let Ok(handle) = Handle::try_current() else {
        warn!("cannot release shuffle read partition {partition} outside a Tokio runtime");
        return;
    };
    handle.spawn(async move {
        if let Err(error) = reader.release(partition).await {
            debug!("failed to release shuffle read partition {partition}: {error}");
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
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
        /// The partitions that have been released.
        released: Mutex<Vec<usize>>,
    }

    impl TestReader {
        fn new(close_unopened: bool, permits: Option<Semaphore>) -> Arc<Self> {
            Arc::new(Self {
                close_unopened,
                permits,
                started: AtomicUsize::new(0),
                opened: AtomicUsize::new(0),
                closed: Arc::new(AtomicUsize::new(0)),
                released: Mutex::new(vec![]),
            })
        }

        fn counts(&self) -> (usize, usize, usize) {
            (
                self.started.load(Ordering::SeqCst),
                self.opened.load(Ordering::SeqCst),
                self.closed.load(Ordering::SeqCst),
            )
        }

        fn released(&self) -> Vec<usize> {
            self.released
                .lock()
                .map(|released| released.clone())
                .unwrap_or_default()
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

        async fn release(&self, partition: usize) -> Result<()> {
            if let Ok(mut released) = self.released.lock() {
                released.push(partition);
            }
            Ok(())
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

    fn exec(reader: &Arc<TestReader>, partitions: usize, replicated: bool) -> ShuffleReadExec {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        ShuffleReadExec::new(reader.clone(), Arc::new(properties), replicated)
    }

    fn execute(reader: &Arc<TestReader>) -> Result<SendableRecordBatchStream> {
        exec(reader, 1, false).execute(0, Arc::new(TaskContext::default()))
    }

    async fn wait_until(condition: impl Fn() -> bool) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    }

    #[tokio::test]
    async fn releases_only_unopened_input_on_drop() -> Result<()> {
        // An input that has been read is not released.
        let read = TestReader::new(true, None);
        let mut stream = execute(&read)?;
        assert!(stream.try_next().await?.is_none());
        drop(stream);

        // An input that does not need to be closed is not released.
        let unaffected = TestReader::new(false, None);
        drop(execute(&unaffected)?);

        // An input that is dropped outside the runtime is left to stage cleanup.
        let outside = TestReader::new(true, None);
        let stream = execute(&outside)?;
        let _ = std::thread::spawn(move || drop(stream)).join();

        // An input that has not been opened is released without being opened.
        let unopened = TestReader::new(true, None);
        drop(execute(&unopened)?);
        wait_until(|| !unopened.released().is_empty()).await?;

        assert_eq!(unopened.counts(), (0, 0, 0));
        assert_eq!(read.counts(), (1, 1, 1));
        assert_eq!(unaffected.counts(), (0, 0, 0));
        assert_eq!(outside.counts(), (0, 0, 0));
        for (reader, released) in [(&unopened, 1), (&read, 0), (&unaffected, 0), (&outside, 0)] {
            assert_eq!(reader.released().len(), released);
        }
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
        wait_until(|| reader.closed.load(Ordering::SeqCst) > 0).await?;
        // The open call that had started is the one that completes.
        assert_eq!(reader.counts(), (1, 1, 1));
        assert!(reader.released().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn releases_unexecuted_partitions_of_replicated_input_only() -> Result<()> {
        // The task reads one of the two partitions of each input.
        let replicated = TestReader::new(true, None);
        let other = TestReader::new(true, None);
        let inputs = [exec(&replicated, 2, true), exec(&other, 2, false)];
        for input in &inputs {
            let mut stream = input.execute(1, Arc::new(TaskContext::default()))?;
            assert!(stream.try_next().await?.is_none());
        }

        // The partition that the task has not executed is released once, if each
        // task has its own replica of the input. It belongs to another task otherwise.
        for _ in 0..2 {
            for input in &inputs {
                input.release_unexecuted();
            }
            wait_until(|| !replicated.released().is_empty()).await?;
            tokio::task::yield_now().await;
        }
        assert_eq!(replicated.released(), vec![0]);
        assert!(other.released().is_empty());
        assert_eq!(replicated.counts(), (1, 1, 1));
        assert_eq!(other.counts(), (1, 1, 1));
        Ok(())
    }
}
