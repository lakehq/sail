use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common_datafusion::session::job::{
    JobRunner, JobRunnerHistory, JobRunnerHistoryReporter, JobSnapshot, StageSnapshot, TaskSnapshot,
};
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
    history_reporter: Mutex<Option<Box<dyn JobRunnerHistoryReporter>>>,
    state: Arc<Mutex<LocalJobRunnerState>>,
}

#[derive(Default)]
struct LocalJobRunnerState {
    jobs: Vec<JobSnapshot>,
    stages: Vec<StageSnapshot>,
    tasks: Vec<TaskSnapshot>,
}

impl LocalJobRunner {
    pub fn new(history_reporter: Box<dyn JobRunnerHistoryReporter>) -> Self {
        Self {
            next_job_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
            history_reporter: Mutex::new(Some(history_reporter)),
            state: Arc::new(Mutex::new(LocalJobRunnerState::default())),
        }
    }

    fn start_job(
        state: &Arc<Mutex<LocalJobRunnerState>>,
        job_id: u64,
        plan: Arc<dyn ExecutionPlan>,
    ) {
        let now = Utc::now();
        let physical_plan = DisplayableExecutionPlan::new(plan.as_ref())
            .indent(true)
            .to_string();
        let partitions = plan.output_partitioning().partition_count() as u64;
        if let Ok(mut state) = state.lock() {
            state.jobs.push(JobSnapshot {
                job_id,
                status: "RUNNING".to_string(),
                created_at: now,
                stopped_at: None,
            });
            state.stages.push(StageSnapshot {
                job_id,
                stage: 0,
                partitions,
                inputs: vec![],
                group: "local".to_string(),
                mode: "local".to_string(),
                distribution: plan.output_partitioning().to_string(),
                placement: "driver".to_string(),
                physical_plan,
                metrics_json: String::new(),
                status: "RUNNING".to_string(),
                created_at: now,
                stopped_at: None,
            });
            state.tasks.push(TaskSnapshot {
                job_id,
                stage: 0,
                partition: 0,
                attempt: 0,
                status: "RUNNING".to_string(),
                metrics_json: String::new(),
                created_at: now,
                stopped_at: None,
            });
        }
    }

    fn finish_job(
        state: &Arc<Mutex<LocalJobRunnerState>>,
        job_id: u64,
        status: &'static str,
        metrics_json: Option<String>,
    ) {
        let now = Utc::now();
        let metrics_json = metrics_json.unwrap_or_default();
        if let Ok(mut state) = state.lock() {
            if let Some(job) = state.jobs.iter_mut().find(|x| x.job_id == job_id) {
                job.status = status.to_string();
                job.stopped_at = Some(now);
            }
            if let Some(stage) = state.stages.iter_mut().find(|x| x.job_id == job_id) {
                stage.status = status.to_string();
                stage.metrics_json = metrics_json.clone();
                stage.stopped_at = Some(now);
            }
            if let Some(task) = state.tasks.iter_mut().find(|x| x.job_id == job_id) {
                task.status = status.to_string();
                task.metrics_json = metrics_json;
                task.stopped_at = Some(now);
            }
        }
    }

    fn history(&self) -> JobRunnerHistory {
        if let Ok(state) = self.state.lock() {
            JobRunnerHistory {
                jobs: state.jobs.clone(),
                stages: state.stages.clone(),
                tasks: state.tasks.clone(),
                workers: vec![],
            }
        } else {
            JobRunnerHistory {
                jobs: vec![],
                stages: vec![],
                tasks: vec![],
                workers: vec![],
            }
        }
    }
}

#[tonic::async_trait]
impl StateObservable<JobRunnerObserver> for LocalJobRunner {
    async fn observe(&self, observer: JobRunnerObserver) {
        self.history().observe(observer).await
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
        Self::start_job(&self.state, job_id, plan.clone());
        let stream = execute_stream(plan.clone(), ctx.task_ctx())?;
        let schema = stream.schema();
        let state = self.state.clone();
        let output = futures::stream::unfold(
            (stream, state, plan, false),
            move |(mut stream, state, plan, finished)| async move {
                if finished {
                    return None;
                }
                match stream.next().await {
                    Some(Ok(batch)) => Some((Ok(batch), (stream, state, plan, false))),
                    Some(Err(error)) => {
                        Self::finish_job(
                            &state,
                            job_id,
                            "FAILED",
                            crate::metrics::plan_metrics_json(plan.clone()),
                        );
                        Some((Err(error), (stream, state, plan, true)))
                    }
                    None => {
                        Self::finish_job(
                            &state,
                            job_id,
                            "SUCCEEDED",
                            crate::metrics::plan_metrics_json(plan),
                        );
                        None
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }

    async fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        let history_reporter = self
            .history_reporter
            .lock()
            .ok()
            .and_then(|mut history_reporter| history_reporter.take());
        if let Some(history_reporter) = history_reporter {
            history_reporter.report(self.history()).await;
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
