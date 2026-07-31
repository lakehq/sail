use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use sail_common_datafusion::session::job::{JobRunner, JobRunnerHistory, JobRunnerHistoryReporter};
use sail_common_datafusion::system::observable::{JobRunnerObserver, Observer, StateObservable};
use sail_server::actor::ActorSystem;
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverComponents, DriverEvent, DriverHandle, DriverOptions};
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;

fn explain_job_graph(plan: Arc<dyn ExecutionPlan>, use_blocking_shuffle: bool) -> Result<String> {
    JobGraph::try_new(
        plan,
        JobGraphOptions {
            use_blocking_shuffle,
        },
    )
    .map(|graph| graph.to_string())
    .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub struct LocalJobRunner {
    next_job_id: AtomicU64,
    stopped: AtomicBool,
    history_reporter: std::sync::Mutex<Option<Box<dyn JobRunnerHistoryReporter>>>,
}

impl LocalJobRunner {
    pub fn new(history_reporter: Box<dyn JobRunnerHistoryReporter>) -> Self {
        Self {
            next_job_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
            history_reporter: std::sync::Mutex::new(Some(history_reporter)),
        }
    }
}

#[tonic::async_trait]
impl StateObservable<JobRunnerObserver> for LocalJobRunner {
    async fn observe(&self, observer: JobRunnerObserver) {
        observer.nothing()
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(plan, false)
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
        let history_reporter = self
            .history_reporter
            .lock()
            .ok()
            .and_then(|mut history_reporter| history_reporter.take());
        if let Some(history_reporter) = history_reporter {
            history_reporter
                .report(JobRunnerHistory {
                    jobs: vec![],
                    stages: vec![],
                    tasks: vec![],
                    workers: vec![],
                })
                .await;
        }
    }
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
impl StateObservable<JobRunnerObserver> for ClusterJobRunner {
    async fn observe(&self, observer: JobRunnerObserver) {
        let result = self
            .driver
            .send(DriverEvent::ObserveState { observer })
            .await;
        if let Err(SendError(DriverEvent::ObserveState { observer })) = result {
            observer.fail(internal_datafusion_err!(
                "failed to observe state for cluster job runner"
            ));
        }
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(
            plan,
            matches!(&self.shuffle_backend, ShuffleBackendKind::Storage { .. }),
        )
    }

    /// Executes a plan on the cluster. This is where the cool stuff happens.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteJob {
                plan,
                context: ctx.task_ctx(),
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
