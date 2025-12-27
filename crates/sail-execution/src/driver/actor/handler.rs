use std::mem;
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use log::{error, info, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::driver::actor::DriverActor;
use crate::driver::job_scheduler::TaskTimeout;
use crate::driver::output::JobOutput;
use crate::driver::worker_pool::{WorkerIdle, WorkerLost, WorkerTimeout};
use crate::driver::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskInstance, WorkerId};

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
        match self.job_scheduler.accept_job(plan) {
            Ok(job_id) => {
                self.job_outputs
                    .insert(job_id, JobOutput::Pending { result });
                self.scale_up_workers(ctx);
                self.schedule_tasks(ctx);
            }
            Err(e) => {
                let _ = result.send(Err(e));
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
    ) -> ActorAction {
        self.job_outputs.remove(&job_id);
        self.worker_pool.detach_job(ctx, job_id);
        ActorAction::Continue
    }

    pub(super) fn handle_update_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        instance: TaskInstance,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        sequence: Option<u64>,
    ) -> ActorAction {
        if let Some(sequence) = sequence {
            if self
                .task_sequences
                .get(&instance)
                .is_some_and(|s| sequence <= *s)
            {
                // The task status update is outdated, so we skip the remaining logic.
                warn!(
                    "job {} task {} attempt {} sequence {sequence} is stale",
                    instance.job_id, instance.task_id, instance.attempt
                );
                return ActorAction::Continue;
            }
            self.task_sequences.insert(instance.clone(), sequence);
        }
        self.update_task(ctx, instance, status, message, cause);
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        instance: TaskInstance,
    ) -> ActorAction {
        match self.job_scheduler.probe_pending_task(&instance) {
            TaskTimeout::Yes => {
                let message = "task scheduling timeout".to_string();
                self.update_task(ctx, instance, TaskStatus::Failed, Some(message), None);
            }
            TaskTimeout::No => {}
        }
        ActorAction::Continue
    }

    fn scale_up_workers(&mut self, ctx: &mut ActorContext<Self>) {
        let max_workers = self.options.worker_max_count;
        let slots_per_worker = self.options.worker_task_slots;
        let active_workers = self.worker_pool.count_active_workers();
        let used_slots = self.job_scheduler.count_active_tasks();
        let pending_slots = self.job_scheduler.count_pending_tasks();
        let available_slots = active_workers * slots_per_worker;
        if available_slots >= used_slots + pending_slots {
            return;
        }
        let missing_slots = used_slots + pending_slots - available_slots;
        // round up the number of workers to the nearest integer
        let missing_workers = missing_slots.div_ceil(slots_per_worker);
        let missing_workers = if max_workers > 0 {
            missing_workers.min(max_workers.saturating_sub(active_workers))
        } else {
            missing_workers
        };
        for _ in 0..missing_workers {
            self.worker_pool.start_worker(ctx);
        }
    }

    pub fn update_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        instance: TaskInstance,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> ActorAction {
        match status {
            TaskStatus::Running => {
                self.job_scheduler.record_running_task(&instance, message);
                self.schedule_tasks(ctx);
                self.try_update_job_output(ctx, instance.job_id);
            }
            TaskStatus::Succeeded => {
                self.job_scheduler.record_succeeded_task(&instance, message);
                self.worker_pool.detach_task(ctx, &instance);
                self.schedule_tasks(ctx);
                self.try_update_job_output(ctx, instance.job_id);
            }
            TaskStatus::Failed => {
                // TODO: support task retry
                let cause = cause.unwrap_or_else(|| {
                    CommonErrorCause::Internal(format!(
                        "job {} task {} failed at attempt {}: {}",
                        instance.job_id,
                        instance.task_id,
                        instance.attempt,
                        message.as_deref().unwrap_or("unknown reason")
                    ))
                });
                self.job_scheduler.record_failed_task(&instance, message);
                self.worker_pool.detach_task(ctx, &instance);
                self.cancel_job(ctx, instance.job_id, cause);
                self.schedule_tasks(ctx);
            }
            TaskStatus::Canceled => {
                let cause = cause.unwrap_or_else(|| {
                    CommonErrorCause::Internal(format!(
                        "job {} task {} attempt {} canceled: {}",
                        instance.job_id,
                        instance.task_id,
                        instance.attempt,
                        message.as_deref().unwrap_or("unknown reason")
                    ))
                });
                self.job_scheduler.record_canceled_task(&instance, message);
                self.worker_pool.detach_task(ctx, &instance);
                self.cancel_job(ctx, instance.job_id, cause);
                self.schedule_tasks(ctx);
            }
        }
        ActorAction::Continue
    }

    fn schedule_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        let slots = self.worker_pool.find_idle_task_slots();
        for schedule in self.job_scheduler.schedule_tasks(ctx, slots) {
            self.worker_pool.run_task(ctx, schedule);
        }
    }

    fn try_update_job_output(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId) {
        let Some(output) = self.job_outputs.remove(&job_id) else {
            return;
        };
        match output {
            JobOutput::Pending { result } => {
                if let Some(stream) = self.try_build_job_output(job_id) {
                    let stream = match stream {
                        Ok(x) => x,
                        Err(e) => {
                            let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                            let _ = result.send(Err(e));
                            self.cancel_job(ctx, job_id, cause);
                            return;
                        }
                    };
                    let (sender, receiver) = mpsc::channel(self.options.job_output_buffer);
                    let receiver_stream = Box::pin(RecordBatchStreamAdapter::new(
                        stream.schema(),
                        ReceiverStream::new(receiver),
                    ));
                    if result.send(Ok(receiver_stream)).is_err() {
                        self.cancel_job(
                            ctx,
                            job_id,
                            CommonErrorCause::Internal("job output receiver dropped".to_string()),
                        );
                        return;
                    }
                    self.job_outputs
                        .insert(job_id, JobOutput::run(ctx, job_id, stream, sender));
                } else {
                    self.job_outputs
                        .insert(job_id, JobOutput::Pending { result });
                }
            }
            x @ JobOutput::Running { .. } => {
                self.job_outputs.insert(job_id, x);
            }
        }
    }

    fn try_build_job_output(
        &mut self,
        job_id: JobId,
    ) -> Option<ExecutionResult<SendableRecordBatchStream>> {
        let metadata = self.job_scheduler.get_job_output(job_id)?;
        Some(metadata.and_then(|m| self.worker_pool.build_job_output_stream(m)))
    }

    fn cancel_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId, cause: CommonErrorCause) {
        if let Some(output) = self.job_outputs.remove(&job_id) {
            output.fail(ctx, job_id, cause);
        }
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
        let instances = self.worker_pool.find_tasks_for_worker(worker_id);
        for instance in instances {
            let reason = format!(
                "job {} task {} attempt {} failed for worker {worker_id}: {reason}",
                instance.job_id, instance.task_id, instance.attempt
            );
            self.update_task(ctx, instance, TaskStatus::Failed, Some(reason), None);
        }
    }
}
