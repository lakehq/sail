// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::common::{
    DataFusionError, Result, exec_datafusion_err, internal_datafusion_err, internal_err,
};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common::actor::ActorSystem;
use sail_common_datafusion::extension::task_context_with_extension;
use sail_common_datafusion::session::job::JobRunner;
use sail_data_source::listing::commit::JobWrites;
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverComponents, DriverHandle, DriverMessage, DriverOptions};
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;

fn explain_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    shuffle_backend: ShuffleBackendKind,
) -> Result<String> {
    JobGraph::try_new(plan, JobGraphOptions { shuffle_backend })
        .map(|graph| graph.to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub struct LocalJobRunner {
    session_id: String,
    next_job_id: AtomicU64,
    stopped: AtomicBool,
}

impl LocalJobRunner {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            next_job_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
        }
    }
}

impl Default for LocalJobRunner {
    fn default() -> Self {
        Self::new(String::new())
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(
            plan,
            ShuffleBackendKind::Flight {
                compression: crate::shuffle::ShuffleCompression::None,
                connection_count: std::num::NonZeroUsize::MIN,
                initial_stream_window_size: None,
                initial_connection_window_size: None,
            },
        )
    }

    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if self.stopped.load(Ordering::Relaxed) {
            return internal_err!("job runner is stopped");
        }
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let options = TracingExecOptions {
            metrics: global_metrics(),
            session_id: Some(self.session_id.clone()),
            job_id: Some(job_id),
            stage: None,
            attempt: None,
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        Ok(execute_stream(plan, ctx.task_ctx())?)
    }

    async fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

/// The writes of a job, which are removed when this is dropped before the job
/// runner has published or removed them.
struct JobWritesGuard(Option<Arc<JobWrites>>);

impl JobWritesGuard {
    fn take(&mut self) -> Option<Arc<JobWrites>> {
        self.0.take()
    }
}

impl Drop for JobWritesGuard {
    fn drop(&mut self) {
        if let Some(writes) = self.take() {
            // The files are removed whether or not the work is awaited.
            drop(writes.abort());
        }
    }
}

/// Returns the output of a job, which ends when the writes of the job are
/// published, or with the error of a write that cannot be published.
///
/// The tasks of a job do not publish the files of a write, see [`JobWrites`].
/// The files are published here when the output has ended without an error
/// and `confirm` has returned `true` after that. They are removed when
/// the output returns an error, when `confirm` returns `false`, or when
/// the returned stream is dropped before the output has ended.
///
/// The end of the output without an error means that the job has succeeded,
/// unless the driver has stopped:
///
///   1. The output ends when the driver has dropped the `JobOutputManager` of
///      the job and all its handles (`driver/output.rs`, `JobOutputStream`).
///   2. The job scheduler drops the manager of a running job in three places
///      (`driver/job_scheduler/core.rs`). `refresh_job` drops it when every task
///      region has succeeded, which `update_task_regions` decides: the latest
///      attempt of every task of the job has succeeded. Each attempt of a task
///      reads the latest attempts of its inputs, and the task of a commit has
///      recorded its files before it succeeds. `refresh_job` also drops it when
///      a task region has failed, after it has put the error in the output with
///      a handle. `stop` drops it without an error when the driver stops.
///   3. The output also ends without an error when the driver stops in any
///      other way, since the task that forwards the output belongs to
///      the driver (`driver/output.rs`, `build_job_output`).
///
/// `confirm` therefore asks the driver for a reply to a message that is sent
/// after the output has ended. A driver that has started to stop does not
/// reply, and a driver that replies has not dropped the manager by stopping.
fn publish_job_writes<F>(
    output: SendableRecordBatchStream,
    writes: Arc<JobWrites>,
    confirm: impl FnOnce() -> F + Send + 'static,
) -> SendableRecordBatchStream
where
    F: Future<Output = bool> + Send,
{
    let schema = output.schema();
    let state = (output, JobWritesGuard(Some(writes)), confirm);
    let output = futures::stream::unfold(Some(state), |state| async move {
        let (mut output, mut guard, confirm) = state?;
        match output.next().await {
            Some(Ok(batch)) => Some((Ok(batch), Some((output, guard, confirm)))),
            Some(Err(e)) => {
                if let Some(writes) = guard.take() {
                    writes.abort().await;
                }
                Some((Err(e), None))
            }
            None => {
                let writes = guard.take()?;
                // A job without writes ends here like its output.
                if writes.is_empty() {
                    return None;
                }
                let result = if confirm().await {
                    writes.commit().await
                } else {
                    writes.abort().await;
                    Err(exec_datafusion_err!(
                        "the write is not committed: the driver has stopped before the job has ended"
                    ))
                };
                result.err().map(|e| (Err(e), None))
            }
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, output))
}

pub struct ClusterJobRunner {
    driver: DriverHandle,
    shuffle_backend: ShuffleBackendKind,
}

impl ClusterJobRunner {
    pub fn new(
        system: &mut ActorSystem,
        options: DriverOptions,
        components: DriverComponents,
    ) -> Self {
        let shuffle_backend = options.shuffle_backend.clone();
        let driver = DriverHandle::new(system.spawn::<DriverActor>((options, components)));
        Self {
            driver,
            shuffle_backend,
        }
    }

    pub fn driver(&self) -> DriverHandle {
        self.driver.clone()
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(plan, self.shuffle_backend.clone())
    }

    /// Executes a plan on the cluster. This is where the cool stuff happens.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        // The tasks of the job can be attempted again, so the job publishes
        // the files of its writes when it has succeeded.
        let writes = Arc::new(JobWrites::default());
        let context = task_context_with_extension(&ctx.task_ctx(), Arc::clone(&writes));
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverMessage::ExecuteJob {
                plan,
                context,
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        let output = rx
            .await
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        let driver = self.driver.clone();
        // The driver replies to the activation, which has no effect on a driver
        // that runs jobs, unless it has started to stop.
        let confirm = move || async move { driver.activate().await.is_ok() };
        Ok(publish_job_writes(output, writes, confirm))
    }

    async fn stop(&self) {
        let _ = self.driver.shutdown_and_wait().await;
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::exec_err;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::{TryStreamExt, stream};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use sail_common_datafusion::extension::TaskAttempt;
    use sail_data_source::listing::commit::{FileWriteCommitExec, manifest_batch, manifest_schema};
    use uuid::Uuid;

    use super::*;

    /// A job whose only task is the commit of a write without files.
    struct TestJob {
        writes: Arc<JobWrites>,
        store: Arc<InMemory>,
        /// The output of the task of the commit.
        output: SendableRecordBatchStream,
    }

    impl TestJob {
        fn try_new() -> Result<Self> {
            let ctx = SessionContext::new();
            let store = Arc::new(InMemory::new());
            let url = ObjectStoreUrl::parse("memory://")?;
            ctx.register_object_store(url.as_ref(), Arc::clone(&store) as Arc<dyn ObjectStore>);
            let writes = Arc::new(JobWrites::default());
            let context = task_context_with_extension(&ctx.task_ctx(), Arc::clone(&writes));
            let context = task_context_with_extension(&context, Arc::new(TaskAttempt(0)));
            let manifest = manifest_batch(0, 1, &[])?;
            let input =
                MemorySourceConfig::try_new_exec(&[vec![manifest]], manifest_schema(), None)?;
            let commit =
                FileWriteCommitExec::new(input, url, Path::from("table"), Uuid::from_u128(1));
            let output = commit.execute(0, context)?;
            Ok(Self {
                writes,
                store,
                output,
            })
        }

        async fn objects(&self) -> Result<Vec<String>> {
            let objects = self.store.list(None);
            Ok(objects
                .map_ok(|object| object.location.to_string())
                .try_collect()
                .await?)
        }
    }

    async fn rows(output: SendableRecordBatchStream) -> Result<usize> {
        let batches: Vec<_> = output.try_collect().await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }

    #[tokio::test]
    async fn writes_of_a_job_are_published_when_its_output_has_ended() -> Result<()> {
        let job = TestJob::try_new()?;
        let (schema, output) = (job.output.schema(), job.output);
        // The task of the commit has ended, and the output of the job has not.
        let (end, ended) = oneshot::channel::<()>();
        let waiting = stream::once(ended).filter_map(|_| async { None });
        let output = Box::pin(RecordBatchStreamAdapter::new(schema, output.chain(waiting)));
        let mut output = publish_job_writes(output, Arc::clone(&job.writes), || async { true });
        assert_eq!(output.try_next().await?.map(|x| x.num_rows()), Some(1));
        let next = tokio::time::timeout(std::time::Duration::from_millis(50), output.try_next());
        assert!(next.await.is_err());
        assert!(!job.writes.is_empty());
        let store = Arc::clone(&job.store);
        assert_eq!(store.list(None).try_collect::<Vec<_>>().await?.len(), 0);

        drop(end);
        assert!(output.try_next().await?.is_none());
        let objects = store.list(None).try_collect::<Vec<_>>().await?;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].location.as_ref(), "table/_SUCCESS");
        assert!(job.writes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn writes_of_a_job_that_fails_are_not_published() -> Result<()> {
        let job = TestJob::try_new()?;
        let (schema, output) = (job.output.schema(), job.output);
        // The task of the commit has succeeded, and another task has failed.
        let failure = stream::once(async { exec_err!("task failure") });
        let output = Box::pin(RecordBatchStreamAdapter::new(schema, output.chain(failure)));
        let output = publish_job_writes(output, Arc::clone(&job.writes), || async { true });
        let result = rows(output).await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("task failure")),
            "{result:?}"
        );
        assert!(job.writes.is_empty());
        let store = Arc::clone(&job.store);
        assert_eq!(store.list(None).try_collect::<Vec<_>>().await?.len(), 0);
        Ok(())
    }

    /// The output of a job ends without an error when the driver stops.
    #[tokio::test]
    async fn writes_of_a_job_are_not_published_when_the_driver_has_stopped() -> Result<()> {
        let job = TestJob::try_new()?;
        let writes = Arc::clone(&job.writes);
        let output = publish_job_writes(job.output, writes, || async { false });
        let result = rows(output).await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("the driver has stopped")),
            "{result:?}"
        );
        assert!(job.writes.is_empty());
        let store = Arc::clone(&job.store);
        assert_eq!(store.list(None).try_collect::<Vec<_>>().await?.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn writes_of_a_job_whose_output_is_dropped_are_not_published() -> Result<()> {
        let job = TestJob::try_new()?;
        let writes = Arc::clone(&job.writes);
        let mut output = publish_job_writes(job.output, writes, || async { true });
        assert_eq!(output.try_next().await?.map(|x| x.num_rows()), Some(1));
        assert!(!job.writes.is_empty());
        drop(output);
        assert!(job.writes.is_empty());
        let store = Arc::clone(&job.store);
        assert_eq!(store.list(None).try_collect::<Vec<_>>().await?.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn job_without_writes_ends_like_its_output() -> Result<()> {
        let job = TestJob::try_new()?;
        let asked = Arc::new(AtomicBool::new(false));
        let confirm = {
            let asked = Arc::clone(&asked);
            move || async move {
                asked.store(true, Ordering::Relaxed);
                true
            }
        };
        // The task of the commit was not executed.
        let schema = job.output.schema();
        let output = Box::pin(RecordBatchStreamAdapter::new(schema, stream::empty()));
        let output = publish_job_writes(output, Arc::clone(&job.writes), confirm);
        assert_eq!(rows(output).await?, 0);
        assert!(!asked.load(Ordering::Relaxed));
        assert_eq!(job.objects().await?, Vec::<String>::new());
        Ok(())
    }
}
