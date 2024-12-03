use std::mem;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::future::try_join_all;
use futures::TryStreamExt;
use log::{debug, error, info, warn};
use prost::bytes::BytesMut;
use prost::Message;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::driver::actor::output::JobOutput;
use crate::driver::actor::DriverActor;
use crate::driver::planner::JobGraph;
use crate::driver::state::{
    JobDescriptor, JobStage, TaskDescriptor, TaskMode, TaskState, TaskStatus, WorkerDescriptor,
    WorkerState,
};
use crate::driver::DriverEvent;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskId, WorkerId};
use crate::plan::{ShuffleConsumption, ShuffleReadExec, ShuffleWriteExec};
use crate::stream::{
    LocalStreamStorage, MergedRecordBatchStream, TaskReadLocation, TaskWriteLocation,
};
use crate::worker_manager::WorkerLaunchOptions;

impl DriverActor {
    pub(super) fn handle_server_ready(
        &mut self,
        ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ActorAction {
        let server = mem::take(&mut self.server);
        self.server = match server.ready(signal, port) {
            Ok(x) => x,
            Err(e) => return ActorAction::fail(e),
        };
        info!("driver server is ready on port {port}");
        self.start_workers(ctx, self.options().worker_initial_count);
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
        let out = if let Some(worker) = self.state.get_worker(worker_id) {
            match worker.state {
                WorkerState::Pending => {
                    self.state.update_worker(
                        worker_id,
                        WorkerState::Running {
                            host,
                            port,
                            tasks: Default::default(),
                            jobs: Default::default(),
                            updated_at: Instant::now(),
                            heartbeat_at: Instant::now(),
                        },
                        None,
                    );
                    self.schedule_lost_worker_probe(ctx, worker_id);
                    self.schedule_idle_worker_probe(ctx, worker_id);
                    self.schedule_tasks(ctx);
                    Ok(())
                }
                WorkerState::Running { .. } => Err(ExecutionError::InternalError(format!(
                    "worker {worker_id} is already running"
                ))),
                WorkerState::Stopped => Err(ExecutionError::InternalError(format!(
                    "worker {worker_id} is stopped"
                ))),
                WorkerState::Failed => Err(ExecutionError::InternalError(format!(
                    "worker {worker_id} is failed"
                ))),
            }
        } else {
            Err(ExecutionError::InvalidArgument(format!(
                "worker {worker_id} not found"
            )))
        };
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
        self.state.record_worker_heartbeat(worker_id);
        self.schedule_lost_worker_probe(ctx, worker_id);
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
    ) -> ActorAction {
        let Some(worker) = self.state.get_worker(worker_id) else {
            warn!("worker {worker_id} not found");
            return ActorAction::Continue;
        };
        if matches!(&worker.state, WorkerState::Pending) {
            warn!("worker {worker_id} registration timeout");
            let message = "worker registration timeout".to_string();
            self.state
                .update_worker(worker_id, WorkerState::Failed, Some(message));
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
        let Some(worker) = self.state.get_worker(worker_id) else {
            warn!("worker {worker_id} not found");
            return ActorAction::Continue;
        };
        if let WorkerState::Running {
            tasks,
            jobs,
            updated_at,
            ..
        } = &worker.state
        {
            if tasks.is_empty() && jobs.is_empty() && *updated_at <= instant {
                let reason = "worker has been idle for too long".to_string();
                self.stop_worker(ctx, worker_id, Some(reason));
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_lost_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        instant: Instant,
    ) -> ActorAction {
        let Some(worker) = self.state.get_worker(worker_id) else {
            warn!("worker {worker_id} not found");
            return ActorAction::Continue;
        };
        if let WorkerState::Running { heartbeat_at, .. } = &worker.state {
            if *heartbeat_at <= instant {
                warn!("worker {worker_id} heartbeat timeout");
                let message = "worker heartbeat timeout".to_string();
                self.fail_tasks_for_worker(ctx, worker_id, message.clone());
                self.stop_worker(ctx, worker_id, Some(message));
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
        match self.accept_job(ctx, plan) {
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

    pub(super) fn handle_remove_job_output(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
    ) -> ActorAction {
        self.job_outputs.remove(&job_id);
        for worker_id in self.state.detach_job_from_workers(job_id) {
            self.remove_worker_streams(ctx, worker_id, job_id);
            self.schedule_idle_worker_probe(ctx, worker_id);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_update_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
        message: Option<String>,
        sequence: Option<u64>,
    ) -> ActorAction {
        if let Some(sequence) = sequence {
            if self
                .task_sequences
                .get(&task_id)
                .is_some_and(|s| sequence <= *s)
            {
                // The task status update is outdated, so we skip the remaining logic.
                warn!("task {task_id} sequence {sequence} is stale");
                return ActorAction::Continue;
            }
            self.task_sequences.insert(task_id, sequence);
        }
        self.update_task(ctx, task_id, attempt, status, message);
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
    ) -> ActorAction {
        let Some(task) = self.state.get_task(task_id) else {
            warn!("task {task_id} not found");
            return ActorAction::Continue;
        };
        if task.attempt == attempt && matches!(&task.state, TaskState::Pending) {
            let message = "task scheduling timeout".to_string();
            self.update_task(
                ctx,
                task_id,
                task.attempt,
                TaskStatus::Failed,
                Some(message),
            );
        }
        ActorAction::Continue
    }

    fn accept_job(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<JobId> {
        let job_id = self.state.next_job_id()?;
        debug!(
            "job {} execution plan\n{}",
            job_id,
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let graph = JobGraph::try_new(plan)?;
        debug!("job {} job graph \n{}", job_id, graph);
        let mut stages = vec![];
        for (s, stage) in graph.stages().iter().enumerate() {
            let last = s == graph.stages().len() - 1;
            let mut tasks = vec![];
            for p in 0..stage.output_partitioning().partition_count() {
                let task_id = self.state.next_task_id()?;
                let attempt = 0;
                let channel = if last {
                    Some(format!("job-{job_id}/task-{task_id}/attempt-{attempt}").into())
                } else {
                    None
                };
                self.state.add_task(
                    task_id,
                    TaskDescriptor {
                        job_id,
                        stage: s,
                        partition: p,
                        attempt,
                        mode: TaskMode::Pipelined,
                        state: TaskState::Created,
                        messages: vec![],
                        channel,
                    },
                );
                self.task_queue.push_back(task_id);
                tasks.push(task_id);
            }
            stages.push(JobStage {
                plan: Arc::clone(stage),
                tasks,
            })
        }
        let descriptor = JobDescriptor { stages };
        self.state.add_job(job_id, descriptor);
        Ok(job_id)
    }

    fn scale_up_workers(&mut self, ctx: &mut ActorContext<Self>) {
        let max_workers = self.options().worker_max_count;
        let slots_per_worker = self.options().worker_task_slots;
        let active_workers = self.state.count_active_workers();
        let used_slots = self.state.count_active_tasks();
        let pending_slots = self.state.count_pending_tasks();
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
        self.start_workers(ctx, missing_workers);
    }

    fn start_workers(&mut self, ctx: &mut ActorContext<Self>, count: usize) {
        let Ok(worker_ids) = (0..count)
            .map(|_| self.state.next_worker_id())
            .collect::<ExecutionResult<Vec<_>>>()
        else {
            error!("failed to generate worker IDs");
            ctx.send(DriverEvent::Shutdown);
            return;
        };
        for &worker_id in worker_ids.iter() {
            let descriptor = WorkerDescriptor {
                state: WorkerState::Pending,
                messages: vec![],
            };
            self.state.add_worker(worker_id, descriptor);
            self.start_worker(ctx, worker_id);
            ctx.send_with_delay(
                DriverEvent::ProbePendingWorker { worker_id },
                self.options().worker_launch_timeout,
            );
        }
    }

    fn start_worker(&mut self, ctx: &mut ActorContext<Self>, worker_id: WorkerId) {
        let Some(port) = self.server.port() else {
            error!("the driver server is not ready");
            return;
        };
        let options = WorkerLaunchOptions {
            enable_tls: self.options().enable_tls,
            driver_external_host: self.options().driver_external_host.to_string(),
            driver_external_port: if self.options().driver_external_port > 0 {
                self.options().driver_external_port
            } else {
                port
            },
            worker_heartbeat_interval: self.options().worker_heartbeat_interval,
            worker_stream_buffer: self.options().worker_stream_buffer,
            rpc_retry_strategy: self.options().rpc_retry_strategy.clone(),
        };
        let worker_manager = Arc::clone(&self.worker_manager);
        ctx.spawn(async move {
            if let Err(e) = worker_manager.launch_worker(worker_id, options).await {
                error!("failed to start worker {worker_id}: {e}");
            }
        });
    }

    pub fn update_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
        message: Option<String>,
    ) -> ActorAction {
        let Some(task) = self.state.get_task(task_id) else {
            return ActorAction::warn(format!("task {task_id} not found"));
        };
        let job_id = task.job_id;
        match status {
            TaskStatus::Running => {
                if let Some(state) = task.state.run() {
                    self.state.update_task(task_id, attempt, state, message);
                } else {
                    return ActorAction::warn(format!(
                        "task {task_id} cannot be updated to the running state from its current state"
                    ));
                }
                self.schedule_tasks(ctx);
                self.try_update_job_output(ctx, job_id);
            }
            TaskStatus::Succeeded => {
                let worker_id = task.state.worker_id();
                if let Some(state) = task.state.succeed() {
                    self.state.update_task(task_id, attempt, state, message);
                } else {
                    return ActorAction::warn(format!(
                        "task {task_id} cannot be updated to the succeeded state from its current state"
                    ));
                }
                if let Some(worker_id) = worker_id {
                    self.state.detach_task_from_worker(task_id, worker_id);
                    self.schedule_idle_worker_probe(ctx, worker_id);
                }
                self.schedule_tasks(ctx);
                self.try_update_job_output(ctx, job_id);
            }
            TaskStatus::Failed => {
                // TODO: support task retry
                let reason = format!(
                    "task {} failed at attempt {}: {}",
                    task_id,
                    attempt,
                    message.as_deref().unwrap_or("unknown reason")
                );
                let worker_id = task.state.worker_id();
                self.state
                    .update_task(task_id, attempt, TaskState::Failed, message);
                if let Some(worker_id) = worker_id {
                    self.state.detach_task_from_worker(task_id, worker_id);
                    self.schedule_idle_worker_probe(ctx, worker_id);
                }
                self.cancel_job(ctx, job_id, reason);
                self.schedule_tasks(ctx);
            }
            TaskStatus::Canceled => {
                let reason = format!(
                    "task {} canceled at attempt {}: {}",
                    task_id,
                    attempt,
                    message.as_deref().unwrap_or("unknown reason")
                );
                let worker_id = task.state.worker_id();
                self.state
                    .update_task(task_id, attempt, TaskState::Canceled, message);
                if let Some(worker_id) = worker_id {
                    self.state.detach_task_from_worker(task_id, worker_id);
                    self.schedule_idle_worker_probe(ctx, worker_id);
                }
                self.cancel_job(ctx, job_id, reason);
                self.schedule_tasks(ctx);
            }
        }
        ActorAction::Continue
    }

    fn schedule_idle_worker_probe(&mut self, ctx: &mut ActorContext<Self>, worker_id: WorkerId) {
        let Some(worker) = self.state.get_worker(worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        let WorkerState::Running { updated_at, .. } = &worker.state else {
            warn!("worker {worker_id} is not running");
            return;
        };
        ctx.send_with_delay(
            DriverEvent::ProbeIdleWorker {
                worker_id,
                instant: *updated_at,
            },
            self.options().worker_max_idle_time,
        );
    }

    fn schedule_lost_worker_probe(&mut self, ctx: &mut ActorContext<Self>, worker_id: WorkerId) {
        let Some(worker) = self.state.get_worker(worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        let WorkerState::Running { heartbeat_at, .. } = &worker.state else {
            warn!("worker {worker_id} is not running");
            return;
        };
        ctx.send_with_delay(
            DriverEvent::ProbeLostWorker {
                worker_id,
                instant: *heartbeat_at,
            },
            self.options().worker_heartbeat_timeout,
        );
    }

    fn find_idle_task_slots(&self) -> Vec<(WorkerId, usize)> {
        self.state
            .list_workers()
            .into_iter()
            .filter_map(|(id, worker)| {
                let count = match &worker.state {
                    WorkerState::Running { tasks, .. } => {
                        self.options().worker_task_slots.saturating_sub(tasks.len())
                    }
                    _ => 0,
                };
                if count > 0 {
                    Some((id, count))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    fn schedule_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        let slots = self.find_idle_task_slots();
        let mut assigner = TaskSlotAssigner::new(slots);
        let mut skipped_tasks = vec![];
        while let Some(task_id) = self.task_queue.pop_front() {
            if !self.state.can_schedule_task(task_id) {
                skipped_tasks.push(task_id);
                continue;
            }
            match self.prepare_pending_task(ctx, task_id) {
                Ok(()) => {}
                Err(e) => {
                    warn!("failed to prepare pending task {task_id}: {e}");
                    continue;
                }
            };
            let Some(worker_id) = assigner.next() else {
                skipped_tasks.push(task_id);
                // We do not break the loop even if there are no more available task slots.
                // We want to examine all the tasks in the queue and mark eligible tasks as pending.
                continue;
            };
            match self.schedule_task(ctx, task_id, worker_id) {
                Ok(()) => {}
                Err(e) => {
                    warn!("failed to schedule task {task_id} to worker {worker_id}: {e}");
                }
            };
        }
        self.task_queue.extend(skipped_tasks);
    }

    fn prepare_pending_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
    ) -> ExecutionResult<()> {
        let Some(task) = self.state.get_task(task_id) else {
            return Err(ExecutionError::InternalError(format!(
                "task {task_id} not found"
            )));
        };
        if matches!(task.state, TaskState::Created) {
            let attempt = task.attempt;
            self.state
                .update_task(task_id, attempt, TaskState::Pending, None);
            ctx.send_with_delay(
                DriverEvent::ProbePendingTask { task_id, attempt },
                self.options().task_launch_timeout,
            );
        };
        Ok(())
    }

    fn schedule_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        worker_id: WorkerId,
    ) -> ExecutionResult<()> {
        let Some(task) = self.state.get_task(task_id) else {
            return Err(ExecutionError::InternalError(format!(
                "task {task_id} not found"
            )));
        };
        let Some(job) = self.state.get_job(task.job_id) else {
            return Err(ExecutionError::InternalError(format!(
                "job {} not found for task {}",
                task.job_id, task_id
            )));
        };
        let Some(stage) = job.stages.get(task.stage) else {
            return Err(ExecutionError::InternalError(format!(
                "stage {} not found for job {} and task {}",
                task.stage, task.job_id, task_id
            )));
        };
        match task.state {
            TaskState::Pending => {}
            _ => {
                return Err(ExecutionError::InternalError(format!(
                    "task {task_id} cannot be scheduled in its current state"
                )));
            }
        };
        // Clear the sequence number before scheduling the task to the worker.
        self.task_sequences.remove(&task_id);
        let attempt = task.attempt;
        let partition = task.partition;
        let channel = task.channel.clone();
        let plan = match self.rewrite_shuffle(task.job_id, job, stage.plan.clone()) {
            Ok(x) => x,
            Err(e) => {
                let message = format!("failed to rewrite shuffle: {e}");
                self.update_task(ctx, task_id, attempt, TaskStatus::Failed, Some(message));
                return Err(ExecutionError::InternalError(e.to_string()));
            }
        };
        let plan = match self.encode_plan(plan) {
            Ok(x) => x,
            Err(e) => {
                let message = format!("failed to encode task plan: {e}");
                self.update_task(ctx, task_id, attempt, TaskStatus::Failed, Some(message));
                return Err(ExecutionError::InternalError(e.to_string()));
            }
        };
        let client = match self.worker_client(worker_id) {
            Ok(client) => client.clone(),
            Err(e) => {
                let message = format!("failed to get worker {worker_id} client: {e}");
                self.update_task(ctx, task_id, attempt, TaskStatus::Failed, Some(message));
                return Err(ExecutionError::InternalError(e.to_string()));
            }
        };
        self.state
            .update_task(task_id, attempt, TaskState::Scheduled { worker_id }, None);
        self.state.attach_task_to_worker(task_id);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Err(e) = client
                .run_task(task_id, attempt, plan, partition, channel)
                .await
            {
                let _ = handle
                    .send(DriverEvent::UpdateTask {
                        task_id,
                        attempt,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to run task via the worker client: {e}")),
                        sequence: None,
                    })
                    .await;
            }
        });
        Ok(())
    }

    fn encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Vec<u8>> {
        let plan =
            PhysicalPlanNode::try_from_physical_plan(plan, self.physical_plan_codec.as_ref())?;
        let mut buffer = BytesMut::new();
        plan.encode(&mut buffer)?;
        Ok(buffer.freeze().into())
    }

    fn rewrite_shuffle(
        &self,
        job_id: JobId,
        job: &JobDescriptor,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        // TODO: This can be expensive. We may want to cache the result
        //   when the task attempt is the same.
        let result = plan.transform(|node| {
            if let Some(shuffle) = node.as_any().downcast_ref::<ShuffleReadExec>() {
                let locations = (0..shuffle.locations().len())
                    .map(|partition| {
                        job.stages[shuffle.stage()]
                            .tasks
                            .iter()
                            .map(|task_id| {
                                let task = self
                                    .state
                                    .get_task(*task_id)
                                    .ok_or_else(|| exec_datafusion_err!("task {task_id} not found"))?;
                                let worker_id = task.state.worker_id().ok_or_else(
                                    || exec_datafusion_err!("task {task_id} is not bound to a worker"),
                                )?;
                                let worker = self
                                    .state
                                    .get_worker(worker_id)
                                    .ok_or_else(|| exec_datafusion_err!("worker {worker_id} not found"))?;
                                let (host, port) = match &worker.state {
                                    WorkerState::Running { host, port, .. } => (host.clone(), *port),
                                    _ => return exec_err!("worker {worker_id} is not running"),
                                };
                                let attempt = task.attempt;
                                Ok(TaskReadLocation::Worker {
                                    worker_id,
                                    host,
                                    port,
                                    channel: format!("job-{job_id}/task-{task_id}/attempt-{attempt}/partition-{partition}")
                                        .into(),
                                })
                            })
                            .collect::<Result<Vec<_>, DataFusionError>>()
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let shuffle = shuffle.clone().with_locations(locations);
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else if let Some(shuffle) = node.as_any().downcast_ref::<ShuffleWriteExec>() {
                let storage = match shuffle.consumption() {
                    ShuffleConsumption::Single => LocalStreamStorage::Ephemeral,
                    ShuffleConsumption::Multiple => LocalStreamStorage::Memory,
                };
                let locations = (0..shuffle.locations().len())
                    .map(|partition| {
                        let task_id = job.stages[shuffle.stage()].tasks[partition];
                        let task = self
                            .state
                            .get_task(task_id)
                            .ok_or_else(|| exec_datafusion_err!("task {task_id} not found"))?;
                        let attempt = task.attempt;
                        let locations = (0..shuffle.shuffle_partitioning().partition_count())
                            .map(|p| {
                                TaskWriteLocation::Local {
                                    channel: format!("job-{job_id}/task-{task_id}/attempt-{attempt}/partition-{p}")
                                        .into(),
                                    storage,
                                }
                            })
                            .collect();
                        Ok(locations)
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let shuffle = shuffle.clone().with_locations(locations);
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else {
                Ok(Transformed::no(node))
            }
        });
        Ok(result.data()?)
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
                            let reason = e.to_string();
                            let _ = result.send(Err(e));
                            self.cancel_job(ctx, job_id, reason);
                            return;
                        }
                    };
                    let (sender, receiver) = mpsc::channel(self.options().job_output_buffer);
                    let receiver_stream = Box::pin(RecordBatchStreamAdapter::new(
                        stream.schema(),
                        ReceiverStream::new(receiver),
                    ));
                    if result.send(Ok(receiver_stream)).is_err() {
                        self.cancel_job(ctx, job_id, "job output receiver dropped".to_string());
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
        let Some(job) = self.state.get_job(job_id) else {
            return Some(Err(ExecutionError::InternalError(format!(
                "job {job_id} not found"
            ))));
        };
        let Some(last_stage) = job.stages.last() else {
            return Some(Err(ExecutionError::InternalError(format!(
                "last stage not found for job {job_id}"
            ))));
        };
        let schema = last_stage.plan.schema();
        let channels = last_stage
            .tasks
            .iter()
            .map(|task_id| {
                self.state
                    .get_task(*task_id)
                    .and_then(|task| match task.state {
                        // We should not consider tasks in the "scheduled" state even if the
                        // worker ID is known at that time. This is because the task may not be
                        // running on the worker, and the shuffle stream may not be available.
                        TaskState::Running { worker_id } | TaskState::Succeeded { worker_id } => {
                            Some((*task_id, task.channel.clone(), worker_id))
                        }
                        _ => None,
                    })
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|(task_id, channel, worker_id)| {
                let channel = channel.ok_or_else(|| {
                    ExecutionError::InternalError(format!("task channel is not set: {task_id}"))
                })?;
                let client = self.worker_client(worker_id)?;
                Ok((channel, client))
            })
            .collect::<ExecutionResult<Vec<_>>>();
        let channels = match channels {
            Ok(x) => x,
            Err(e) => return Some(Err(e)),
        };
        let output_schema = schema.clone();
        let output = futures::stream::once(async move {
            let futures = channels.into_iter().map(|(channel, client)| {
                let channel_schema = schema.clone();
                async move { client.fetch_task_stream(channel, channel_schema).await }
            });
            let streams = try_join_all(futures)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Box::pin(MergedRecordBatchStream::new(schema, streams)))
                as Result<_, DataFusionError>
        })
        .try_flatten();
        let stream = Box::pin(RecordBatchStreamAdapter::new(output_schema, output));
        Some(Ok(stream))
    }

    fn cancel_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId, reason: String) {
        if let Some(output) = self.job_outputs.remove(&job_id) {
            output.fail(reason);
        }
        for worker_id in self.state.detach_job_from_workers(job_id) {
            self.remove_worker_streams(ctx, worker_id, job_id);
            self.schedule_idle_worker_probe(ctx, worker_id);
        }
        let tasks = self
            .state
            .find_active_tasks_for_job(job_id)
            .into_iter()
            .map(|(task_id, task)| (task_id, task.attempt, task.state.worker_id()))
            .collect::<Vec<_>>();
        // The tasks are canceled, but they may remain in the task queue.
        // This is OK, since they will be removed when the task scheduling logic runs next time.
        let tasks = tasks
            .into_iter()
            .flat_map(|(task_id, attempt, worker_id)| {
                let reason = format!("task {task_id} attempt {attempt} canceled for job {job_id}");
                self.state
                    .update_task(task_id, attempt, TaskState::Canceled, Some(reason));
                if let Some(worker_id) = worker_id {
                    self.state.detach_task_from_worker(task_id, worker_id);
                    self.schedule_idle_worker_probe(ctx, worker_id);
                }
                let client = match self.worker_client(worker_id?) {
                    Ok(client) => client.clone(),
                    Err(e) => {
                        warn!("failed to stop task {task_id}: {e}");
                        return None;
                    }
                };
                Some((task_id, attempt, client))
            })
            .collect::<Vec<_>>();
        ctx.spawn(async move {
            for (task_id, attempt, client) in tasks {
                if let Err(e) = client.stop_task(task_id, attempt).await {
                    warn!("failed to stop task {task_id}: {e}");
                }
            }
        });
    }

    fn fail_tasks_for_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        reason: String,
    ) {
        let tasks = self
            .state
            .find_tasks_for_worker(worker_id)
            .into_iter()
            .map(|(task_id, task)| (task_id, task.attempt))
            .collect::<Vec<_>>();
        for (task_id, attempt) in tasks {
            let reason =
                format!("task {task_id} attempt {attempt} failed for worker {worker_id}: {reason}");
            self.update_task(ctx, task_id, attempt, TaskStatus::Failed, Some(reason));
        }
    }

    fn remove_worker_streams(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        job_id: JobId,
    ) {
        let client = match self.worker_client(worker_id) {
            Ok(x) => x,
            Err(e) => {
                warn!("failed to get worker client {worker_id}: {e}");
                return;
            }
        };
        ctx.spawn(async move {
            let prefix = format!("job-{job_id}/");
            if let Err(e) = client.remove_stream(prefix).await {
                error!("failed to remove streams in worker {worker_id}: {e}");
            }
        });
    }

    fn stop_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        reason: Option<String>,
    ) {
        let Some(worker) = self.state.get_worker(worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        match worker.state {
            WorkerState::Pending => {
                warn!("trying to stop pending worker {worker_id}");
                self.state
                    .update_worker(worker_id, WorkerState::Stopped, reason);
            }
            WorkerState::Running { .. } => {
                info!("stopping worker {worker_id}");
                let client = match self.worker_client(worker_id) {
                    Ok(x) => x,
                    Err(e) => {
                        warn!("failed to stop worker {worker_id}: {e}");
                        return;
                    }
                };
                ctx.spawn(async move {
                    if let Err(e) = client.stop_worker().await {
                        error!("failed to stop worker {worker_id}: {e}");
                    }
                });
                self.state
                    .update_worker(worker_id, WorkerState::Stopped, reason);
            }
            WorkerState::Stopped | WorkerState::Failed => {}
        }
    }

    pub(super) fn stop_all_workers(&mut self, ctx: &mut ActorContext<Self>) {
        let worker_ids = self
            .state
            .list_workers()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        let reason = "stopping all workers".to_string();
        for worker_id in worker_ids {
            self.stop_worker(ctx, worker_id, Some(reason.clone()));
        }
    }
}

struct TaskSlotAssigner {
    slots: Vec<(WorkerId, usize)>,
}

impl TaskSlotAssigner {
    pub fn new(slots: Vec<(WorkerId, usize)>) -> Self {
        Self { slots }
    }

    pub fn next(&mut self) -> Option<WorkerId> {
        self.slots.iter_mut().find_map(|(worker_id, slots)| {
            if *slots > 0 {
                *slots -= 1;
                Some(*worker_id)
            } else {
                None
            }
        })
    }
}
