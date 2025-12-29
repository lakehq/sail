use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use log::{error, info, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::driver::actor::DriverActor;
use crate::driver::worker_pool::{WorkerIdle, WorkerLost, WorkerTimeout};
use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;

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
            self.schedule_tasks(ctx);
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
        self.worker_pool.record_worker_heartbeat(ctx, worker_id);
        ActorAction::Continue
    }

    pub(super) fn handle_worker_known_peers(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        peer_worker_ids: Vec<WorkerId>,
    ) -> ActorAction {
        self.worker_pool
            .record_worker_known_peers(worker_id, peer_worker_ids);
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
    ) -> ActorAction {
        match self.worker_pool.probe_pending_worker(worker_id) {
            WorkerTimeout::Yes => {
                // start a new worker to compensate the failed one
                self.worker_pool.start_worker(ctx);
            }
            WorkerTimeout::No => {}
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_idle_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        instant: Instant,
    ) -> ActorAction {
        match self.worker_pool.probe_idle_worker(worker_id, instant) {
            WorkerIdle::Yes => {
                let reason = "worker has been idle for too long".to_string();
                self.worker_pool.stop_worker(ctx, worker_id, Some(reason));
            }
            WorkerIdle::No => {}
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_lost_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        instant: Instant,
    ) -> ActorAction {
        match self.worker_pool.probe_lost_worker(worker_id, instant) {
            WorkerLost::Yes => {
                let reason = "worker heartbeat timeout".to_string();
                self.worker_pool
                    .stop_worker(ctx, worker_id, Some(reason.clone()));
                self.fail_tasks_for_worker(ctx, worker_id, reason);
            }
            WorkerLost::No => {}
        }
        ActorAction::Continue
    }

    pub(super) fn handle_execute_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        plan: Arc<dyn ExecutionPlan>,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let out = self.job_scheduler.accept_job(plan);
        if out.is_ok() {
            self.schedule_tasks(ctx);
        }
        let _ = result.send(out);
        ActorAction::Continue
    }

    // FIXME: clean up job
    pub(super) fn handle_clean_up_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
    ) -> ActorAction {
        self.worker_pool.detach_job(ctx, job_id);
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
                if let Some(cause) = cause {
                    warn!(
                        "cause ignored for running task {}: {cause:?}",
                        TaskKeyDisplay(&key)
                    );
                }
                self.update_running_task(ctx, key, message);
            }
            TaskStatus::Succeeded => {
                if let Some(cause) = cause {
                    warn!(
                        "cause ignored for succeeded task {}: {cause:?}",
                        TaskKeyDisplay(&key)
                    );
                }
                self.update_succeeded_task(ctx, key, message);
            }
            TaskStatus::Failed => {
                self.update_failed_task(ctx, key, message, cause);
            }
            TaskStatus::Canceled => {
                self.update_canceled_task(ctx, key, message, cause);
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
    ) -> ActorAction {
        if self.job_scheduler.is_pending_task(&key) {
            let message = "task scheduling timeout".to_string();
            self.update_failed_task(ctx, key, Some(message), None);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_driver_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.fetch_local_stream(&key));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_worker_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _worker_id: WorkerId,
        _key: TaskStreamKey,
        _schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(Err(ExecutionError::InternalError(
            "not implemented: fetch worker stream".to_string(),
        )));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_remote_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.fetch_remote_stream(uri, &key, schema));
        ActorAction::Continue
    }

    pub fn update_running_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        message: Option<String>,
    ) {
        self.job_scheduler.record_running_task(&key, message);
        self.update_job_output(ctx, &key);
        self.schedule_tasks(ctx);
    }

    pub fn update_succeeded_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        message: Option<String>,
    ) {
        self.job_scheduler.record_succeeded_task(&key, message);
        self.worker_pool.detach_task(ctx, &key);
        self.schedule_tasks(ctx);
        self.update_job_output(ctx, &key);
    }

    pub fn update_failed_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) {
        let cause = cause.unwrap_or_else(|| {
            CommonErrorCause::Internal(format!(
                "{} failed: {}",
                TaskKeyDisplay(&key),
                message.as_deref().unwrap_or("unknown reason")
            ))
        });
        self.job_scheduler.record_failed_task(&key, message);
        self.worker_pool.detach_task(ctx, &key);
        self.schedule_tasks(ctx);
    }

    pub fn update_canceled_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) {
        let cause = cause.unwrap_or_else(|| {
            CommonErrorCause::Internal(format!(
                "{} canceled: {}",
                TaskKeyDisplay(&key),
                message.as_deref().unwrap_or("unknown reason")
            ))
        });
        self.job_scheduler.record_canceled_task(&key, message);
        self.schedule_tasks(ctx);
    }

    fn schedule_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        let _actions = self.job_scheduler.next();
        // update worker pool for canceled tasks
        // count required task slots and scale up workers if needed
        todo!()
    }

    fn run_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        let slots = self.worker_pool.find_idle_task_slots();
        let schedules = self.job_scheduler.run_tasks(ctx, slots);
        for (worker_id, key, definition) in schedules.into_iter() {
            self.worker_pool.run_task(ctx, worker_id, key, definition);
        }
    }

    fn update_job_output(&mut self, _ctx: &mut ActorContext<Self>, _key: &TaskKey) {
        // determine the task streams via the job scheduler
        // fetch task streams via the worker pool
        // add task streams via the job scheduler
        todo!()
    }

    fn cancel_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId, cause: CommonErrorCause) {
        let tasks = self.job_scheduler.cancel_job(job_id);
        for task in tasks {
            self.worker_pool.cancel_task(ctx, &task);
        }
    }

    fn fail_tasks_for_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        reason: String,
    ) {
        let keys = self.worker_pool.find_tasks_for_worker(worker_id);
        for key in keys {
            let reason = format!(
                "{} failed for worker {worker_id}: {reason}",
                TaskKeyDisplay(&key)
            );
            self.update_failed_task(ctx, key, Some(reason), None);
        }
    }
}
