use std::collections::HashSet;
use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use log::{error, info, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::driver::actor::DriverActor;
use crate::driver::job_scheduler::TaskState;
use crate::driver::task::TaskAssignment;
use crate::driver::{DriverEvent, TaskStatus};
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
            // start a new worker to compensate the failed one
            self.worker_pool.start_worker(ctx);
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

            let keys = self.task_assigner.find_worker_tasks(worker_id);
            self.task_assigner.deactivate_worker(worker_id);
            for key in keys.iter() {
                self.job_scheduler.update_task(
                    key,
                    TaskState::Failed,
                    Some("task failed for lost worker".to_string()),
                );
            }

            let job_ids = keys.iter().map(|k| k.job_id).collect::<HashSet<_>>();
            for job_id in job_ids {
                self.schedule_job(ctx, job_id);
            }
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
        if let Ok((job_id, _)) = &out {
            self.schedule_job(ctx, *job_id);
        }
        let _ = result.send(out.map(|(_, stream)| stream));
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
    ) -> ActorAction {
        todo!();
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
                    .update_task(&key, TaskState::Running, message);
                self.update_job_output(ctx, key.job_id);
            }
            TaskStatus::Succeeded => {
                self.job_scheduler
                    .update_task(&key, TaskState::Succeeded, message);
                self.task_assigner.unassign_task(&key);
                self.update_job_output(ctx, key.job_id);
                self.schedule_job(ctx, key.job_id);
                self.run_tasks(ctx);
            }
            TaskStatus::Failed => {
                // Some canceled tasks may report failed status due to closed streams,
                // but it is fine to handle them as failed tasks again.
                self.job_scheduler
                    .update_task(&key, TaskState::Failed, message);
                self.task_assigner.unassign_task(&key);
                self.schedule_job(ctx, key.job_id);
                self.run_tasks(ctx);
            }
            TaskStatus::Canceled => {
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
            .is_some_and(|x| matches!(x, TaskState::Pending))
        {
            let message = "task scheduling timeout".to_string();
            ctx.send(DriverEvent::UpdateTask {
                key,
                status: TaskStatus::Failed,
                message: Some(message.clone()),
                cause: None,
                sequence: None,
            })
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

    fn schedule_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId) {
        let actions = self.job_scheduler.schedule_job(job_id);
        // update worker pool for canceled tasks
        // count required task slots and scale up workers if needed
        todo!()
    }

    fn run_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        let assignments = self.task_assigner.assign_tasks();
        for assignment in assignments {
            match assignment.assignment {
                TaskAssignment::Driver => {
                    todo!()
                }
                TaskAssignment::Worker { worker_id, slot: _ } => {
                    for entry in assignment.set.entries {
                        let definition = match self
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
                        self.worker_pool
                            .run_task(ctx, worker_id, entry.key, definition);
                    }
                }
            }
        }
    }

    fn update_job_output(&mut self, _ctx: &mut ActorContext<Self>, _job_id: JobId) {
        // determine the task streams via the job scheduler
        // fetch task streams via the worker pool
        // add task streams via the job scheduler
        todo!()
    }
}
