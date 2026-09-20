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
use crate::driver::output::{JobOutputItem, JobOutputOutcome};
use crate::driver::worker_scaler::{WorkerLaunchRequest, WorkerRetryRequest};
use crate::driver::{DriverMessage, TaskStatus};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{
    JobId, TaskAttempt, TaskKey, TaskKeyDisplay, TaskStreamKey, TaskStreamKeyDisplay, WorkerId,
};
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
            self.worker_scaler
                .request_initial_workers(count)
                .and_then(|requests| self.start_worker_launch(ctx, requests))
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
            self.worker_scaler.worker_registered(worker_id);
            self.task_assigner.activate_worker(worker_id);
            self.run_tasks(ctx);
            self.reconcile_worker_demands(ctx);
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
        self.fail_worker_launch_if_pending(
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
        self.fail_worker_launch_if_pending(ctx, worker_id, message);
        ActorAction::Continue
    }

    pub(super) fn handle_retry_worker_demand(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: WorkerRetryRequest,
    ) -> ActorAction {
        if let Some(request) = self.worker_scaler.retry(request)
            && let Err(e) = self.start_worker_launch(ctx, vec![request])
        {
            error!("failed to retry worker launch: {e}");
            ctx.send(DriverMessage::Shutdown { result: None });
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
                self.reconcile_worker_demands(ctx);
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
            self.reconcile_worker_demands(ctx);
        }
        let _ = result.send(out.map(|(_, stream)| stream));
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        outcome: JobOutputOutcome,
    ) -> ActorAction {
        self.clean_up_job(ctx, job_id, outcome);
        self.run_tasks(ctx);
        self.reconcile_worker_demands(ctx);
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
                self.unassign_task(ctx, &key);
                self.refresh_job(ctx, key.job_id);
                self.run_tasks(ctx);
                self.reconcile_worker_demands(ctx);
            }
            TaskStatus::Failed => {
                // Some canceled tasks may report failed status due to closed streams,
                // but it is fine to handle them as failed tasks again.
                self.job_scheduler
                    .update_task(&key, TaskState::Failed, message, cause);
                self.unassign_task(ctx, &key);
                self.refresh_job(ctx, key.job_id);
                self.run_tasks(ctx);
                self.reconcile_worker_demands(ctx);
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
            // schedule. A worker demand remains pending while waiting for its
            // retry so the replacement capacity is not requested twice.
            //
            // Re-probe at `worker_launch_timeout` (capped by `task_launch_timeout`)
            // rather than a full `task_launch_timeout`: that is the window a
            // pending worker takes to register or be failed, so once the last
            // pending worker resolves the task fails promptly instead of waiting
            // another full launch window.
            if self.worker_scaler.has_pending_worker_demands() {
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

    fn clean_up_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        outcome: JobOutputOutcome,
    ) {
        for action in self.job_scheduler.clean_up_job(job_id, outcome) {
            self.run_job_action(ctx, action);
        }
    }

    fn run_job_action(&mut self, ctx: &mut ActorContext<Self>, action: JobAction) {
        debug!("job action: {action:?}");
        match action {
            JobAction::ScheduleTaskRegion { region } => {
                if let Err(e) = self.task_assigner.enqueue_tasks(&region) {
                    // Failing one task fails the entire region. Report this as a
                    // separate message so that the current job actions finish first.
                    if let Some(key) = region.tasks.iter().flat_map(|(_, set)| set.tasks()).next() {
                        ctx.send(DriverMessage::UpdateTask {
                            key: key.clone(),
                            status: TaskStatus::Failed,
                            message: Some(e.to_string()),
                            cause: Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
                            sequence: None,
                        });
                    }
                    return;
                }
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
            }
            JobAction::CancelTask { key } => {
                self.task_assigner.exclude_task(&key);
                if let Some(assignment) = self.unassign_task(ctx, &key) {
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
                // Job closure is a lifecycle operation, independent of shuffle ownership.
                // Keep a closed-job record even on workers whose streams were cleaned earlier.
                if stage.is_none() {
                    if let Some(task_runner) = self.task_runner.clone() {
                        ctx.spawn(async move {
                            let _ = task_runner
                                .send(TaskRunnerMessage::CloseJob { job_id })
                                .await;
                        });
                    }
                    for worker_id in self.task_assigner.active_worker_ids() {
                        self.worker_pool.clean_up_job(ctx, worker_id, job_id, None);
                    }
                }
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
                    if stage.is_some() {
                        for worker_id in self.task_assigner.active_worker_ids() {
                            self.worker_pool.clean_up_job(ctx, worker_id, job_id, stage);
                        }
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
                            if stage.is_some() {
                                self.worker_pool.clean_up_job(ctx, worker_id, job_id, stage);
                            }
                        }
                    }
                }
            }
        }
    }

    fn unassign_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: &TaskKey,
    ) -> Option<TaskAssignment> {
        let assignment = self.task_assigner.unassign_task(key)?;
        if let TaskAssignment::Worker { worker_id, .. } = &assignment
            && self.task_assigner.is_worker_idle(*worker_id)
        {
            // A long-running task may outlive every previously scheduled idle probe.
            // Start a fresh idle window when its worker has no remaining work or streams.
            self.worker_pool.mark_worker_idle(ctx, *worker_id);
        }
        Some(assignment)
    }

    /// Reserve complete regions before resolving any routing. Definitions are scoped to this
    /// scheduling snapshot; batches additionally preserve region and worker boundaries.
    fn run_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        let assignments = self.task_assigner.assign_tasks();
        self.task_assigner.track_streams(&assignments);
        let mut batches = indexmap::IndexMap::<_, Vec<TaskKey>>::new();
        for assignment in assignments {
            let worker = match assignment.assignment {
                TaskAssignment::Driver => None,
                TaskAssignment::Worker { worker_id, .. } => Some(worker_id),
            };
            for entry in assignment.set.entries {
                let Some(region) = self.job_scheduler.task_region(&entry.key) else {
                    ctx.send(DriverMessage::UpdateTask {
                        key: entry.key,
                        status: TaskStatus::Failed,
                        message: Some("task region not found".into()),
                        cause: None,
                        sequence: None,
                    });
                    continue;
                };
                batches
                    .entry((entry.key.job_id, region, entry.key.stage, worker))
                    .or_default()
                    .push(entry.key);
            }
        }
        let mut definitions = std::collections::HashMap::new();
        for ((job_id, _, stage, worker), keys) in batches {
            let Some(first) = keys.first() else { continue };
            let definition = definitions.entry((job_id, stage)).or_insert_with(|| {
                let started = Instant::now();
                let result = self
                    .job_scheduler
                    .get_task_definition(first, &self.task_assigner)
                    .map(|(definition, context)| (Arc::new(definition), context))
                    .map_err(|error| {
                        (
                            error.to_string(),
                            CommonErrorCause::new::<PyErrExtractor>(&error),
                        )
                    });
                debug!(
                    "job {job_id} stage {stage} definition construction {:?}",
                    started.elapsed()
                );
                result
            });
            let (definition, context) = match definition {
                Ok((definition, context)) => (definition.clone(), context.clone()),
                Err((message, cause)) => {
                    for key in keys {
                        ctx.send(DriverMessage::UpdateTask {
                            key,
                            status: TaskStatus::Failed,
                            message: Some(message.clone()),
                            cause: Some(cause.clone()),
                            sequence: None,
                        });
                    }
                    continue;
                }
            };
            for key in &keys {
                self.job_scheduler
                    .update_task(key, TaskState::Scheduled, None, None);
            }
            let tasks = keys
                .into_iter()
                .map(|key| TaskAttempt {
                    partition: key.partition,
                    attempt: key.attempt,
                })
                .collect::<Vec<_>>();
            if let Some(worker_id) = worker {
                self.worker_pool
                    .run_task_batch(ctx, worker_id, job_id, stage, tasks, definition);
            } else {
                let task_runner = self.task_runner.clone();
                let driver = ctx.handle().clone();
                ctx.spawn(async move {
                    let output = async {
                        let task_runner = task_runner.ok_or_else(|| {
                            ExecutionError::InternalError("task runner is not started".into())
                        })?;
                        let (result, rx) = oneshot::channel();
                        task_runner
                            .send(TaskRunnerMessage::RunTaskBatch {
                                job_id,
                                stage,
                                tasks: tasks.clone(),
                                definition,
                                context,
                                peers: vec![],
                                result,
                            })
                            .await
                            .map_err(ExecutionError::from)?;
                        rx.await.map_err(|_| {
                            ExecutionError::InternalError(
                                "task runner stopped before admission".into(),
                            )
                        })?
                    }
                    .await;
                    if let Err(error) = output {
                        for task in tasks {
                            let _ = driver
                                .send(DriverMessage::UpdateTask {
                                    key: task.task_key(job_id, stage),
                                    status: TaskStatus::Failed,
                                    message: Some(error.to_string()),
                                    cause: Some(CommonErrorCause::new::<PyErrExtractor>(&error)),
                                    sequence: None,
                                })
                                .await;
                        }
                    }
                });
            }
        }
    }

    fn reconcile_worker_demands(&mut self, ctx: &mut ActorContext<Self>) {
        let output = self
            .worker_scaler
            .reconcile(self.task_assigner.count_worker_demands())
            .and_then(|requests| self.start_worker_launch(ctx, requests));
        if let Err(e) = output {
            error!("failed to request workers: {e}");
            ctx.send(DriverMessage::Shutdown { result: None });
        }
    }

    fn start_worker_launch(
        &mut self,
        ctx: &mut ActorContext<Self>,
        requests: Vec<WorkerLaunchRequest>,
    ) -> ExecutionResult<()> {
        for request in requests {
            let demand_id = request.demand_id;
            debug!(
                "launching worker demand {} attempt {}",
                demand_id, request.attempt
            );
            let worker_id = self.worker_pool.start_worker(ctx)?;
            if !self.worker_scaler.bind_worker(request, worker_id) {
                return Err(ExecutionError::InternalError(format!(
                    "failed to bind worker {worker_id} to demand {}",
                    demand_id
                )));
            }
        }
        Ok(())
    }

    fn fail_worker_launch_if_pending(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        message: String,
    ) {
        if !self.worker_pool.fail_worker_if_pending(worker_id, message) {
            return;
        }
        if let Some(request) = self.worker_scaler.worker_failed(worker_id) {
            warn!(
                "scheduling worker demand {} launch retry {} in {:?}",
                request.demand_id, request.attempt, request.delay,
            );
            ctx.send_with_delay(DriverMessage::RetryWorkerDemand { request }, request.delay);
        }
        self.reconcile_worker_demands(ctx);
    }
}
