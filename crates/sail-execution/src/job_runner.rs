// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common::actor::ActorSystem;
use sail_common_datafusion::session::job::JobRunner;
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::sync::oneshot;

use crate::adaptive::{AdaptiveJoinOptions, Handoffs, SideRunner, select_joins};
use crate::driver::{
    DriverActor, DriverComponents, DriverHandle, DriverMessage, DriverOptions, SubmittedJob,
};
use crate::id::JobId;
use crate::job_graph::{ExternalStageInput, HandoffOutput, JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;

fn explain_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    shuffle_backend: ShuffleBackendKind,
) -> Result<String> {
    JobGraph::try_new(
        plan,
        JobGraphOptions {
            shuffle_backend,
            handoff: None,
        },
    )
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

pub struct ClusterJobRunner {
    driver: DriverHandle,
    shuffle_backend: ShuffleBackendKind,
    enable_adaptive_join_selection: bool,
    adaptive_join_broadcast_threshold: usize,
    /// Identifies the jobs of one query run with adaptive join selection.
    next_group: AtomicU64,
}

impl ClusterJobRunner {
    pub fn new(
        system: &mut ActorSystem,
        options: DriverOptions,
        components: DriverComponents,
    ) -> Self {
        let shuffle_backend = options.shuffle_backend.clone();
        let enable_adaptive_join_selection = options.enable_adaptive_join_selection;
        let adaptive_join_broadcast_threshold = options.adaptive_join_broadcast_threshold;
        let driver = DriverHandle::new(system.spawn::<DriverActor>((options, components)));
        Self {
            driver,
            shuffle_backend,
            enable_adaptive_join_selection,
            adaptive_join_broadcast_threshold,
            next_group: AtomicU64::new(0),
        }
    }

    pub fn driver(&self) -> DriverHandle {
        self.driver.clone()
    }

    async fn submit(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        handoff: Option<HandoffOutput>,
        group: Option<u64>,
    ) -> Result<SubmittedJob> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverMessage::ExecuteJob {
                plan,
                context: ctx.task_ctx(),
                handoff,
                group,
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }

    async fn run_job(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(self.submit(ctx, plan, None, None).await?.stream)
    }

    /// Runs the plan chosen by adaptive join selection, which may read handed-off jobs.
    /// The jobs are released when the output is dropped.
    async fn run_with_handoffs(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        handoffs: &Handoffs,
        group: u64,
    ) -> Result<SendableRecordBatchStream> {
        let release = ReleaseHandoffs {
            driver: self.driver.clone(),
            jobs: handoffs.take(),
        };
        let plan = handoffs.resolve(plan)?;
        let stream = self.submit(ctx, plan, None, Some(group)).await?.stream;
        if release.jobs.is_empty() {
            return Ok(stream);
        }
        let schema = stream.schema();
        let output = stream.map(move |batch| {
            // The jobs are released when the output is dropped.
            let _ = &release;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

/// Releases handed-off jobs when dropped.
struct ReleaseHandoffs {
    driver: DriverHandle,
    jobs: Vec<JobId>,
}

impl Drop for ReleaseHandoffs {
    fn drop(&mut self) {
        let jobs = std::mem::take(&mut self.jobs);
        if jobs.is_empty() {
            return;
        }
        let driver = self.driver.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                for job_id in jobs {
                    let _ = driver.send(DriverMessage::ReleaseJob { job_id }).await;
                }
            });
        }
    }
}

struct ClusterSideRunner<'a> {
    runner: &'a ClusterJobRunner,
    ctx: &'a SessionContext,
    group: u64,
}

#[tonic::async_trait]
impl SideRunner for ClusterSideRunner<'_> {
    async fn run(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        handoff: Option<HandoffOutput>,
    ) -> Result<(SendableRecordBatchStream, Option<ExternalStageInput>)> {
        let job = self
            .runner
            .submit(self.ctx, plan, handoff, Some(self.group))
            .await?;
        Ok((job.stream, job.handoff))
    }

    async fn hand_off(&self, job_id: JobId) -> bool {
        let (result, rx) = oneshot::channel();
        if self
            .runner
            .driver
            .send(DriverMessage::HandOffJob { job_id, result })
            .await
            .is_err()
        {
            return false;
        }
        rx.await.unwrap_or(false)
    }

    fn release(&self, job_id: JobId) {
        drop(ReleaseHandoffs {
            driver: self.runner.driver.clone(),
            jobs: vec![job_id],
        });
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
        if !self.enable_adaptive_join_selection {
            return self.run_job(ctx, plan).await;
        }
        let options = AdaptiveJoinOptions::new(
            ctx.copied_config().options().clone(),
            self.adaptive_join_broadcast_threshold,
        );
        let handoffs = Handoffs::default();
        let group = self.next_group.fetch_add(1, Ordering::Relaxed);
        let runner = ClusterSideRunner {
            runner: self,
            ctx,
            group,
        };
        let selected = select_joins(plan, &options, &runner, &handoffs).await;
        match selected {
            Ok(plan) => self.run_with_handoffs(ctx, plan, &handoffs, group).await,
            Err(error) => {
                drop(ReleaseHandoffs {
                    driver: self.driver.clone(),
                    jobs: handoffs.take(),
                });
                Err(error)
            }
        }
    }

    async fn stop(&self) {
        let _ = self.driver.shutdown_and_wait().await;
    }
}
