use std::collections::HashSet;
use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use log::{debug, error, info, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::driver::actor::DriverActor;
use crate::driver::job_scheduler::{JobAction, TaskState};
use crate::driver::output::JobOutputItem;
use crate::driver::{DriverEvent, TaskStatus};
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey, TaskStreamKeyDisplay, WorkerId};
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter, TaskStreamAssignment};

impl DriverActor {
    pub(super) fn handle_server_ready(
        &mut self,
        ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ActorAction {
        let server = mem::take(&mut self.server);
        self.server = match server.ready(signal) {
            Ok(x) => x,
            Err(e) => {
                error!("{e}");
                return ActorAction::Stop;
            }
        };
        info!("driver server is ready on port {port}");
        self.worker_pool.set_driver_server_port(port);
        for _ in 0..self.options.worker_initial_count {
            self.worker_pool.start_worker(ctx);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_register_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        host: String,
        port: u16,
        result: oneshot::Sender<ExecutionResult<()>>,
    ) -> ActorAction {
        info!("worker {worker_id} is available at {host}:{port}");
        let out = self.worker_pool.register_worker(ctx, worker_id, host, port);
        if out.is_ok() {
            self.task_assigner.activate_worker(worker_id);
            self.run_tasks(ctx);
        }
        if result.send(out).is_err() {
            warn!("failed to send worker registration result");
        }
        ActorAction::Continue
    }

    pub(super) fn handle_worker_heartbeat(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
    ) -> ActorAction {
        self.worker_pool.update_worker_heartbeat(ctx, worker_id);
        ActorAction::Continue
    }

    pub(super) fn handle_worker_known_peers(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        peer_worker_ids: Vec<WorkerId>,
    ) -> ActorAction {
        self.worker_pool
            .update_worker_known_peers(worker_id, peer_worker_ids);
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
    ) -> ActorAction {
        if self.worker_pool.fail_worker_if_pending(worker_id) {
            self.task_assigner.track_worker_failed_to_start();
            self.scale_up_workers(ctx);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_idle_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        instant: Instant,
    ) -> ActorAction {
        if self.task_assigner.is_worker_idle(worker_id)
            && self
                .worker_pool
                .get_worker_last_update(worker_id)
                .is_some_and(|x| x <= instant)
        {
            self.worker_pool.stop_worker(
                ctx,
                worker_id,
                Some("worker has been idle for too long".to_string()),
            );
            self.task_assigner.deactivate_worker(worker_id);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_lost_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        instant: Instant,
    ) -> ActorAction {
        if self
            .worker_pool
            .get_worker_last_heartbeat(worker_id)
            .is_some_and(|x| x <= instant)
        {
            self.worker_pool.stop_worker(
                ctx,
                worker_id,
                Some("worker heartbeat timeout".to_string()),
            );

            let message = "task failed for lost worker".to_string();
            let keys = self.task_assigner.find_worker_tasks(worker_id);
            self.task_assigner.deactivate_worker(worker_id);
            for key in keys.iter() {
                self.job_scheduler.update_task(
                    key,
                    TaskState::Failed,
                    Some(message.clone()),
                    Some(CommonErrorCause::Execution(message.clone())),
                );
            }

            let job_ids = keys.iter().map(|k| k.job_id).collect::<HashSet<_>>();
            for job_id in job_ids {
                self.refresh_job(ctx, job_id);
                self.run_tasks(ctx);
                self.scale_up_workers(ctx);
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_execute_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let out = self.job_scheduler.accept_job(ctx, plan, context);
        if let Ok((job_id, _)) = &out {
            self.refresh_job(ctx, *job_id);
            self.run_tasks(ctx);
            self.scale_up_workers(ctx);
        }
        let _ = result.send(out.map(|(_, stream)| stream));
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
    ) -> ActorAction {
        self.clean_up_job(ctx, job_id);
        ActorAction::Continue
    }

    pub(super) fn handle_update_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        sequence: Option<u64>,
    ) -> ActorAction {
        if let Some(sequence) = sequence {
            if self
                .task_sequences
                .get(&key)
                .is_some_and(|s| sequence <= *s)
            {
                // The task status update is outdated, so we skip the remaining logic.
                warn!("{} sequence {sequence} is stale", TaskKeyDisplay(&key));
                return ActorAction::Continue;
            }
            self.task_sequences.insert(key.clone(), sequence);
        }
        match status {
            TaskStatus::Running => {
                self.job_scheduler
                    .update_task(&key, TaskState::Running, message, cause);
                self.refresh_job(ctx, key.job_id);
            }
            TaskStatus::Succeeded => {
                self.job_scheduler
                    .update_task(&key, TaskState::Succeeded, message, cause);
                self.task_assigner.unassign_task(&key);
                self.refresh_job(ctx, key.job_id);
                self.run_tasks(ctx);
                self.scale_up_workers(ctx);
            }
            TaskStatus::Failed => {
                // Some canceled tasks may report failed status due to closed streams,
                // but it is fine to handle them as failed tasks again.
                self.job_scheduler
                    .update_task(&key, TaskState::Failed, message, cause);
                self.task_assigner.unassign_task(&key);
                self.refresh_job(ctx, key.job_id);
                self.run_tasks(ctx);
                self.scale_up_workers(ctx);
            }
            TaskStatus::Canceled => {
                // The task attempt state should already be "canceled" but we update it
                // for the message and cause.
                self.job_scheduler
                    .update_task(&key, TaskState::Canceled, message, cause);
                // Task cancellation must have been initiated by the driver itself,
                // so it is a no-op to handle canceled tasks here.
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
    ) -> ActorAction {
        if self
            .job_scheduler
            .get_task_state(&key)
            .is_some_and(|x| matches!(x, TaskState::Created))
        {
            let message = "task scheduling timeout".to_string();
            let cause = CommonErrorCause::Execution(message.clone());
            ctx.send(DriverEvent::UpdateTask {
                key,
                status: TaskStatus::Failed,
                message: Some(message),
                cause: Some(cause),
                sequence: None,
            })
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
    ) -> ActorAction {
        self.stream_manager.fail_local_stream_if_pending(&key);
        ActorAction::Continue
    }

    pub(super) fn handle_create_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        let _ = result.send(
            self.stream_manager
                .create_local_stream(key, storage, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_create_remote_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.create_remote_stream(uri, key, schema));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_driver_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.fetch_local_stream(ctx, &key));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(
            self.worker_pool
                .fetch_task_stream(ctx, worker_id, &key, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_remote_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(
            self.stream_manager
                .fetch_remote_stream(ctx, uri, &key, schema),
        );
        ActorAction::Continue
    }

    fn refresh_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId) {
        for action in self.job_scheduler.refresh_job(job_id) {
            self.run_job_action(ctx, action);
        }
    }

    fn clean_up_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId) {
        for action in self.job_scheduler.clean_up_job(job_id) {
            self.run_job_action(ctx, action);
        }
    }

    fn run_job_action(&mut self, ctx: &mut ActorContext<Self>, action: JobAction) {
        debug!("job action: {action:?}");
        match action {
            JobAction::ScheduleTaskRegion { region } => {
                for (_, set) in &region.tasks {
                    for entry in &set.entries {
                        ctx.send_with_delay(
                            DriverEvent::ProbePendingTask {
                                key: entry.key.clone(),
                            },
                            self.options.task_launch_timeout,
                        );
                    }
                }
                self.task_assigner.enqueue_tasks(region);
            }
            JobAction::CancelTask { key } => {
                self.task_assigner.exclude_task(&key);
                if let Some(assignment) = self.task_assigner.unassign_task(&key) {
                    match assignment {
                        TaskAssignment::Driver => self.task_runner.stop_task(&key),
                        TaskAssignment::Worker { worker_id, slot: _ } => {
                            self.worker_pool.stop_task(ctx, worker_id, &key)
                        }
                    }
                }
            }
            JobAction::ExtendJobOutput {
                handle,
                key,
                schema,
            } => {
                let assignment =
                    TaskAssignmentGetter::get(&self.task_assigner, &TaskKey::from(key.clone()));
                let stream = match assignment {
                    None => {
                        warn!(
                            "cannot fetch unassigned stream {}",
                            TaskStreamKeyDisplay(&key)
                        );
                        return;
                    }
                    Some(TaskAssignment::Driver) => {
                        self.stream_manager.fetch_local_stream(ctx, &key)
                    }
                    Some(TaskAssignment::Worker { worker_id, slot: _ }) => self
                        .worker_pool
                        .fetch_task_stream(ctx, *worker_id, &key, schema.clone()),
                };
                let stream = futures::stream::once(async move {
                    stream.map_err(|e| TaskStreamError::External(Arc::new(e)))
                })
                .try_flatten();
                ctx.spawn(async move {
                    handle
                        .send(JobOutputItem::Stream {
                            key,
                            stream: Box::pin(stream),
                        })
                        .await;
                });
            }
            JobAction::FailJobOutput { handle, cause } => {
                ctx.spawn(async move {
                    handle.send(JobOutputItem::Error { cause }).await;
                });
            }
            JobAction::CleanUpJob { job_id, stage } => {
                if self.task_assigner.untrack_remote_streams(job_id, stage) {
                    self.stream_manager
                        .remove_remote_streams(ctx, job_id, stage);
                }
                for x in self.task_assigner.untrack_local_streams(job_id, stage) {
                    match x {
                        TaskStreamAssignment::Driver => {
                            self.stream_manager.remove_local_streams(job_id, stage);
                        }
                        TaskStreamAssignment::Worker { worker_id } => {
                            self.worker_pool.clean_up_job(ctx, worker_id, job_id, stage)
                        }
                    }
                }
            }
        }
    }

    fn run_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        let assignments = self.task_assigner.assign_tasks();
        self.task_assigner.track_streams(&assignments);
        for assignment in assignments {
            for entry in assignment.set.entries {
                let (definition, context) = match self
                    .job_scheduler
                    .get_task_definition(&entry.key, &self.task_assigner)
                {
                    Ok(x) => x,
                    Err(e) => {
                        // The task failure will be handled as a separate event
                        // after processing the current assignments.
                        ctx.send(DriverEvent::UpdateTask {
                            key: entry.key,
                            status: TaskStatus::Failed,
                            message: Some(e.to_string()),
                            cause: Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
                            sequence: None,
                        });
                        continue;
                    }
                };
                self.job_scheduler
                    .update_task(&entry.key, TaskState::Scheduled, None, None);
                match assignment.assignment {
                    TaskAssignment::Driver => self
                        .task_runner
                        .run_task(ctx, entry.key, definition, context),
                    TaskAssignment::Worker { worker_id, slot: _ } => self
                        .worker_pool
                        .run_task(ctx, worker_id, entry.key, definition),
                }
            }
        }
    }

    fn scale_up_workers(&mut self, ctx: &mut ActorContext<Self>) {
        for _ in 0..self.task_assigner.request_workers() {
            self.worker_pool.start_worker(ctx);
        }
    }
}
