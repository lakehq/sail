// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use sail_common::actor::ActorSystem;
use sail_common_datafusion::extension::task_context_with_extension;
use sail_common_datafusion::session::job::JobRunner;
use sail_data_source::listing::commit::WriteCommitLedger;
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

/// Returns the task context of a job whose tasks can be attempted again.
/// The job remembers the commits of its writes in the task context,
/// which lives as long as the job.
fn job_context(ctx: &SessionContext) -> Arc<TaskContext> {
    task_context_with_extension(&ctx.task_ctx(), Arc::new(WriteCommitLedger::default()))
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
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverMessage::ExecuteJob {
                plan,
                context: job_context(ctx),
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }

    async fn stop(&self) {
        let _ = self.driver.shutdown_and_wait().await;
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionConfig;

    use super::*;

    #[test]
    fn each_job_has_its_own_write_commit_ledger() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(7));
        let ledger = |context: &TaskContext| {
            context
                .session_config()
                .get_extension::<WriteCommitLedger>()
        };
        assert!(ledger(&ctx.task_ctx()).is_none());
        let (first, second) = (job_context(&ctx), job_context(&ctx));
        // The task context of the job is the one of the session otherwise.
        assert_eq!(first.session_config().batch_size(), 7);
        assert_eq!(first.session_id(), ctx.session_id());
        assert!(matches!(
            (ledger(&first), ledger(&second)),
            (Some(first), Some(second)) if !Arc::ptr_eq(&first, &second)
        ));
    }
}
