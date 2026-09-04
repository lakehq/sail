use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use log::{debug, error, info, warn};
use sail_celeborn::lifecycle::{LifecycleManagerActor, LocalLifecycleManager};
use sail_common::actor::{ActorAction, ActorContext, ActorHandle};
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::driver::actor::DriverActor;
use crate::driver::job_scheduler::{JobAction, TaskState};
use crate::driver::output::JobOutputItem;
use crate::driver::worker_pool::{WorkerLaunch, WorkerLaunchReason};
use crate::driver::{DriverMessage, TaskStatus};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey, TaskStreamKeyDisplay, WorkerId};
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter, TaskStreamAssignment};
use crate::task_runner::TaskRunnerMessage;

impl DriverActor {
    pub(super) fn handle_celeborn_get_lifecycle_manager(
        &mut self,
        result: oneshot::Sender<Option<ActorHandle<LifecycleManagerActor>>>,
    ) -> ActorAction {
        let _ = result.send(
            self.extensions
                .lifecycle_manager
                .as_ref()
                .map(LocalLifecycleManager::handle),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_activate(
        &mut self,
        ctx: &mut ActorContext<Self>,
        result: oneshot::Sender<ExecutionResult<()>>,
    ) -> ActorAction {
        let output = if self.activated {
            Ok(())
        } else {
            info!("activating driver {}", self.options.driver_id);
            let count = self
                .task_assigner
                .request_initial_workers(self.options.worker_initial_count);
            self.start_workers(ctx, count, WorkerLaunchReason::Initial)
                .inspect(|_| {
                    self.activated = true;
                })
        };
        if result.send(output).is_err() {
            warn!("failed to send driver activation result");
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
            self.worker_launch_retries_exhausted = false;
            self.task_assigner.activate_worker(worker_id);
            self.run_tasks(ctx);
            self.scale_up_workers(ctx);
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
        self.handle_worker_launch_failure(
            ctx,
            worker_id,
            "worker registration timeout".to_string(),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_worker_failed_to_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        message: String,
    ) -> ActorAction {
        self.handle_worker_launch_failure(ctx, worker_id, message);
        ActorAction::Continue
    }

    pub(super) fn handle_retry_worker_launch(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        launch: WorkerLaunch,
    ) -> ActorAction {
        if !self.task_assigner.track_worker_failed_to_start(worker_id) {
            return ActorAction::Continue;
        }
        let should_retry = match launch.reason {
            WorkerLaunchReason::Initial => {
                self.task_assigner
                    .request_initial_workers(self.options.worker_initial_count)
                    > 0
            }
            WorkerLaunchReason::Demand => self.task_assigner.request_workers() > 0,
        };
        if should_retry {
            match self.worker_pool.start_worker(ctx, launch) {
                Ok(worker_id) => self.task_assigner.track_pending_worker(worker_id),
                Err(e) => {
                    error!("failed to retry worker launch: {e}");
                    ctx.send(DriverMessage::Shutdown { result: None });
                    return ActorAction::Continue;
                }
            }
        }
        self.scale_up_workers(ctx);
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
            // The task has not been assigned to a worker within the launch
            // timeout. If workers are still launching, the task can be assigned
            // once one registers (`handle_register_worker` runs pending tasks),
            // so reschedule the probe instead of failing. This keeps long,
            // many-stage jobs alive while the worker pool scales between stages.
            // It cannot loop forever: each worker launch has a finite retry
            // schedule. A failed worker remains pending here while waiting for
            // its retry so the replacement capacity is not requested twice.
            //
            // Re-probe at `worker_launch_timeout` (capped by `task_launch_timeout`)
            // rather than a full `task_launch_timeout`: that is the window a
            // pending worker takes to register or be failed, so once the last
            // pending worker resolves the task fails promptly instead of waiting
            // another full launch window.
            if self.task_assigner.has_pending_workers() {
                let delay = self
                    .options
                    .worker_launch_timeout
                    .min(self.options.task_launch_timeout);
                ctx.send_with_delay(DriverMessage::ProbePendingTask { key }, delay);
            } else {
                let message = "task scheduling timeout".to_string();
                let cause = CommonErrorCause::Execution(message.clone());
                ctx.send(DriverMessage::UpdateTask {
                    key,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                    sequence: None,
                })
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_driver_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let Some(task_runner) = self.task_runner.clone() else {
            let _ = result.send(Err(ExecutionError::InternalError(
                "task runner is not started".to_string(),
            )));
            return ActorAction::Continue;
        };
        ctx.spawn(async move {
            let _ = task_runner
                .send(TaskRunnerMessage::FetchDriverStream {
                    key,
                    schema: Arc::new(Schema::empty()),
                    result,
                })
                .await;
        });
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

    pub(super) fn handle_shutdown(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        result: Option<oneshot::Sender<()>>,
    ) -> ActorAction {
        if self.shutdown_notifier.is_some() {
            warn!("overriding existing shutdown notifier");
        }
        self.shutdown_notifier = result;
        ActorAction::Stop
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
                            DriverMessage::ProbePendingTask {
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
                        TaskAssignment::Driver => {
                            if let Some(task_runner) = self.task_runner.clone() {
                                ctx.spawn(async move {
                                    let _ =
                                        task_runner.send(TaskRunnerMessage::StopTask { key }).await;
                                });
                            }
                        }
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
                let (result, receiver) = oneshot::channel();
                match assignment {
                    None => {
                        warn!(
                            "cannot fetch unassigned stream {}",
                            TaskStreamKeyDisplay(&key)
                        );
                        return;
                    }
                    Some(TaskAssignment::Driver) => {
                        if let Some(task_runner) = self.task_runner.clone() {
                            let task_key = key.clone();
                            let task_schema = schema.clone();
                            ctx.spawn(async move {
                                let _ = task_runner
                                    .send(TaskRunnerMessage::FetchDriverStream {
                                        key: task_key,
                                        schema: task_schema,
                                        result,
                                    })
                                    .await;
                            });
                        } else {
                            let _ = result.send(Err(ExecutionError::InternalError(
                                "task runner is not started".to_string(),
                            )));
                        }
                    }
                    Some(TaskAssignment::Worker { worker_id, slot: _ }) => {
                        let _ = result.send(
                            self.worker_pool
                                .fetch_task_stream(ctx, *worker_id, &key, schema),
                        );
                    }
                }
                let stream = futures::stream::once(async move {
                    receiver
                        .await
                        .map_err(|error| TaskStreamError::External(Arc::new(error)))?
                        .map_err(|error| TaskStreamError::External(Arc::new(error)))
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
            JobAction::CleanUpJob {
                job_id,
                stage,
                context,
            } => {
                if self.task_assigner.untrack_storage_streams(job_id, stage)
                    && let Some(task_runner) = self.task_runner.clone()
                {
                    ctx.spawn(async move {
                        let _ = task_runner
                            .send(TaskRunnerMessage::CleanUpStorageStreams {
                                job_id,
                                stage,
                                context,
                            })
                            .await;
                    });
                }
                if self.task_assigner.untrack_external_streams(job_id, stage) {
                    if let Some(task_runner) = self.task_runner.clone() {
                        ctx.spawn(async move {
                            let _ = task_runner
                                .send(TaskRunnerMessage::CleanUpCelebornStreams { job_id, stage })
                                .await;
                        });
                    }
                    for worker_id in self.task_assigner.active_worker_ids() {
                        self.worker_pool.clean_up_job(ctx, worker_id, job_id, stage);
                    }
                }
                for x in self.task_assigner.untrack_local_streams(job_id, stage) {
                    match x {
                        TaskStreamAssignment::Driver => {
                            if let Some(task_runner) = self.task_runner.clone() {
                                ctx.spawn(async move {
                                    let _ = task_runner
                                        .send(TaskRunnerMessage::CleanUpLocalStreams {
                                            job_id,
                                            stage,
                                        })
                                        .await;
                                });
                            }
                        }
                        TaskStreamAssignment::Worker { worker_id } => {
                            self.worker_pool.clean_up_job(ctx, worker_id, job_id, stage)
                        }
                    }
                }
            }
        }
    }

    /// Assigns pending tasks to available workers and dispatches them for execution.
    ///
    /// Gets task assignments from the task assigner, builds task definitions from the job
    /// scheduler, and dispatches each task to either the driver or a remote worker via gRPC.
    /// Tasks that fail to build a definition are reported as failed.
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
                        // The task failure will be handled as a separate message
                        // after processing the current assignments.
                        ctx.send(DriverMessage::UpdateTask {
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
                    TaskAssignment::Driver => {
                        let Some(task_runner) = self.task_runner.clone() else {
                            ctx.send(DriverMessage::UpdateTask {
                                key: entry.key,
                                status: TaskStatus::Failed,
                                message: Some("task runner is not started".to_string()),
                                cause: None,
                                sequence: None,
                            });
                            continue;
                        };
                        ctx.spawn(async move {
                            let _ = task_runner
                                .send(TaskRunnerMessage::RunTask {
                                    key: entry.key,
                                    definition,
                                    context,
                                    peers: vec![],
                                })
                                .await;
                        });
                    }
                    TaskAssignment::Worker { worker_id, slot: _ } => self
                        .worker_pool
                        .run_task(ctx, worker_id, entry.key, definition),
                }
            }
        }
    }

    fn scale_up_workers(&mut self, ctx: &mut ActorContext<Self>) {
        if self.worker_launch_retries_exhausted {
            if self.task_assigner.has_worker_demand() {
                return;
            }
            self.worker_launch_retries_exhausted = false;
        }
        let count = self.task_assigner.request_workers();
        if let Err(e) = self.start_workers(ctx, count, WorkerLaunchReason::Demand) {
            error!("failed to request workers: {e}");
            ctx.send(DriverMessage::Shutdown { result: None });
        }
    }

    fn start_workers(
        &mut self,
        ctx: &mut ActorContext<Self>,
        count: usize,
        reason: WorkerLaunchReason,
    ) -> ExecutionResult<()> {
        for _ in 0..count {
            let launch = WorkerLaunch {
                reason,
                attempt: 0,
                retries: self.options.worker_launch_retry_strategy.retries(),
            };
            let worker_id = self.worker_pool.start_worker(ctx, launch)?;
            self.task_assigner.track_pending_worker(worker_id);
        }
        Ok(())
    }

    fn handle_worker_launch_failure(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        message: String,
    ) {
        let Some(mut launch) = self.worker_pool.fail_worker_if_pending(worker_id, message) else {
            return;
        };
        if let Some(step) = launch.retries.next() {
            launch.attempt = step.retry;
            warn!(
                "scheduling worker {worker_id} launch retry {} in {:?}",
                step.retry, step.delay,
            );
            ctx.send_with_delay(
                DriverMessage::RetryWorkerLaunch { worker_id, launch },
                step.delay,
            );
        } else {
            self.task_assigner.track_worker_failed_to_start(worker_id);
            self.worker_launch_retries_exhausted = self.task_assigner.has_worker_demand();
            warn!(
                "worker {worker_id} launch retries exhausted after attempt {}",
                launch.attempt
            );
        }
    }
}
