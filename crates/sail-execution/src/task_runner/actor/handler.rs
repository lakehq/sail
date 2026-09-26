use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{error, warn};
use prost::Message;
use sail_common::actor::{ActorAction, ActorContext};
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::driver::{DriverMessage, TaskStatus};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskAttempt, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamSink};
use crate::task::definition::TaskDefinition;
use crate::task_runner::monitor::TaskMonitor;
use crate::task_runner::preparation::TaskPreparation;
use crate::task_runner::{TaskRunnerActor, TaskRunnerPlacement};
use crate::worker::{WorkerLocation, WorkerMessage};

impl TaskRunnerActor {
    pub(super) fn handle_run_task_batch(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: usize,
        tasks: Vec<TaskAttempt>,
        definition: Arc<TaskDefinition>,
        context: Arc<TaskContext>,
        peers: Vec<WorkerLocation>,
    ) -> ExecutionResult<()> {
        // Peer tracking is independent of task admission, including canceled or replayed batches.
        if !peers.is_empty()
            && let TaskRunnerPlacement::Worker {
                worker_id,
                driver,
                peers: tracker,
                ..
            } = &mut self.placement
        {
            let peer_worker_ids = peers.iter().map(|peer| peer.worker_id).collect::<Vec<_>>();
            tracker.track(peers);
            let client = driver.core.clone();
            let worker_id = *worker_id;
            ctx.spawn(async move {
                if let Err(error) = client
                    .report_worker_known_peers(worker_id, peer_worker_ids)
                    .await
                {
                    warn!("failed to report worker known peers: {error}");
                }
            });
        }
        if !self.tasks.check_batch(job_id, stage, &tasks)? {
            return Ok(());
        }
        // Validate shared descriptions before admitting any task. Only descriptions are shared;
        // every task gets a fresh converter, executable plan, and shuffle reader/writer.
        let proto = Arc::new(PhysicalPlanNode::decode(definition.plan.as_ref()).map_err(
            |error| ExecutionError::InvalidArgument(format!("invalid physical plan: {error}")),
        )?);
        self.tasks.record_batch(job_id, stage, &tasks);
        for task in tasks {
            let key = task.task_key(job_id, stage);
            let stream = TaskPreparation {
                session_id: self.session_id.clone(),
                handle: ctx.handle().clone(),
                celeborn: self.extensions.celeborn_streams.is_some(),
            }
            .stream(
                key.clone(),
                definition.clone(),
                proto.clone(),
                context.clone(),
            );
            let (tx, rx) = oneshot::channel();
            self.signals.insert(key.clone(), tx);
            ctx.spawn(TaskMonitor::new(ctx.handle().clone(), key, stream, rx).supervise());
        }
        Ok(())
    }

    pub(super) fn handle_stop_task(&mut self, key: TaskKey) -> ActorAction {
        self.tasks.cancel(&key);
        if let Some(signal) = self.signals.remove(&key) {
            let _ = signal.send(());
        }
        ActorAction::Continue
    }

    pub(super) fn handle_close_job(&mut self, job_id: JobId) -> ActorAction {
        self.tasks.close_job(job_id);
        self.extensions.local_streams.remove_streams(job_id, None);
        self.signals.retain(|key, _| key.job_id != job_id);
        ActorAction::Continue
    }

    pub(super) fn handle_report_task_status(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> ActorAction {
        if !matches!(status, TaskStatus::Running) {
            self.signals.remove(&key);
        }
        match &mut self.placement {
            TaskRunnerPlacement::Driver { driver } => {
                let driver = driver.clone();
                ctx.spawn(async move {
                    let _ = driver
                        .send(DriverMessage::UpdateTask {
                            key,
                            status,
                            message,
                            cause,
                            sequence: None,
                        })
                        .await;
                });
            }
            TaskRunnerPlacement::Worker {
                sequence,
                driver,
                worker,
                retry_strategy,
                ..
            } => {
                let seq = *sequence;
                *sequence = match seq.checked_add(1) {
                    Some(s) => s,
                    None => {
                        error!("sequence number overflow");
                        return ActorAction::Stop;
                    }
                };
                let driver = driver.core.clone();
                let worker = worker.clone();
                let retry_strategy = retry_strategy.clone();
                ctx.spawn(async move {
                    let output = retry_strategy
                        .run(|| {
                            let driver = driver.clone();
                            let key = key.clone();
                            let message = message.clone();
                            let cause = cause.clone();
                            async move {
                                driver
                                    .report_task_status(key, status, message, cause, seq)
                                    .await
                            }
                        })
                        .await;
                    if let Err(error) = output {
                        error!("failed to report task status with retries: {error}");
                        let _ = worker.send(WorkerMessage::Shutdown).await;
                    }
                });
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_local_stream(&mut self, key: TaskStreamKey) -> ActorAction {
        self.extensions.local_streams.fail_stream_if_pending(&key);
        ActorAction::Continue
    }

    pub(super) fn handle_create_local_stream(
        &mut self,
        key: TaskStreamKey,
        replicas: usize,
        buffered: bool,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(key.job_id)
            || self.tasks.is_canceled(&TaskKey::from(key.clone()))
        {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        let _ = result.send(
            self.extensions
                .local_streams
                .create_stream(key, replicas, buffered, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_create_storage_stream(
        &mut self,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(key.job_id)
            || self.tasks.is_canceled(&TaskKey::from(key.clone()))
        {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        let output = self
            .extensions
            .storage_streams()
            .and_then(|streams| streams.create_stream(key, schema, &context));
        let _ = result.send(output);
        ActorAction::Continue
    }

    pub(super) fn handle_create_celeborn_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        mappers: usize,
        channels: usize,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(key.job_id) || self.tasks.is_canceled(&key) {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        if let Some(streams) = self.extensions.celeborn_streams.clone() {
            ctx.spawn(async move {
                let output = streams
                    .create_stream(key, mappers, channels, schema)
                    .await
                    .map_err(ExecutionError::from);
                let _ = result.send(output);
            });
            return ActorAction::Continue;
        }
        let _ = result.send(Err(ExecutionError::InternalError(
            "Celeborn stream requested without a Celeborn shuffle backend".to_string(),
        )));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_driver_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(key.job_id) {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        match &self.placement {
            TaskRunnerPlacement::Driver { .. } => {
                let _ = result.send(self.extensions.local_streams.fetch_stream(ctx, &key));
            }
            TaskRunnerPlacement::Worker { driver, .. } => {
                let client = driver.flight.clone();
                ctx.spawn(async move {
                    let _ = result.send(client.fetch_task_stream(key, schema).await);
                });
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(key.job_id) {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        match &mut self.placement {
            TaskRunnerPlacement::Driver { driver } => {
                let driver = driver.clone();
                ctx.spawn(async move {
                    let _ = driver
                        .send(DriverMessage::FetchWorkerStream {
                            worker_id,
                            key,
                            schema,
                            result,
                        })
                        .await;
                });
            }
            TaskRunnerPlacement::Worker {
                worker_id: this_worker_id,
                ..
            } if worker_id == *this_worker_id => {
                let _ = result.send(self.extensions.local_streams.fetch_stream(ctx, &key));
            }
            TaskRunnerPlacement::Worker { peers, .. } => match peers.get_client_set(worker_id) {
                Ok(client) => {
                    ctx.spawn(async move {
                        let _ = result.send(client.flight.fetch_task_stream(key, schema).await);
                    });
                }
                Err(error) => {
                    let _ = result.send(Err(error));
                    return ActorAction::Continue;
                }
            },
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_local_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(key.job_id) {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        let _ = result.send(self.extensions.local_streams.fetch_stream(ctx, &key));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_storage_stream(
        &mut self,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(key.job_id) {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        let output = self
            .extensions
            .storage_streams()
            .and_then(|streams| streams.fetch_stream(key, schema, &context));
        let _ = result.send(output);
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_celeborn_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: usize,
        channels: Vec<usize>,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        if self.tasks.is_job_closed(job_id) {
            let _ = result.send(Err(ExecutionError::InvalidArgument(
                "task or job has been canceled".into(),
            )));
            return ActorAction::Continue;
        }
        let streams = self.extensions.celeborn_streams.clone();
        ctx.spawn(async move {
            let output = match streams {
                Some(streams) => streams.fetch_stream(job_id, stage, channels, schema).await,
                None => Err(DataFusionError::Internal(
                    "Celeborn stream requested without a Celeborn shuffle backend".to_string(),
                )),
            };
            let _ = result.send(output.map_err(ExecutionError::from));
        });
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_local_streams(
        &mut self,
        job_id: JobId,
        stage: Option<usize>,
    ) -> ActorAction {
        self.extensions.local_streams.remove_streams(job_id, stage);
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_storage_streams(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: Option<usize>,
        context: Arc<TaskContext>,
    ) -> ActorAction {
        if let Some(streams) = self.extensions.storage_streams.clone() {
            ctx.spawn(async move {
                if let Err(error) = streams.remove_streams(job_id, stage, &context).await {
                    warn!("failed to remove storage shuffle data for job {job_id}: {error}");
                }
            });
        }
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_celeborn_streams(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: Option<usize>,
    ) -> ActorAction {
        if let Some(streams) = self.extensions.celeborn_streams.clone() {
            let unregister = matches!(self.placement, TaskRunnerPlacement::Driver { .. });
            ctx.spawn(async move {
                if let Err(error) = streams.remove_streams(job_id, stage, unregister).await {
                    warn!("failed to remove Celeborn shuffle data for job {job_id}: {error}");
                }
            });
        }
        ActorAction::Continue
    }

    pub(super) fn handle_shutdown(&mut self) -> ActorAction {
        ActorAction::Stop
    }
}
