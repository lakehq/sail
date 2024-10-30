use std::mem;
use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::future::try_join_all;
use futures::TryStreamExt;
use log::{error, info, warn};
use prost::bytes::BytesMut;
use prost::Message;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::{mpsc, oneshot};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::driver::actor::output::JobOutput;
use crate::driver::actor::DriverActor;
use crate::driver::planner::JobGraph;
use crate::driver::state::{
    JobDescriptor, JobStage, TaskDescriptor, TaskMode, TaskStatus, WorkerDescriptor, WorkerStatus,
};
use crate::driver::DriverEvent;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskAttempt, TaskId, WorkerId};
use crate::stream::MergedRecordBatchStream;
use crate::worker_manager::WorkerLaunchOptions;

impl DriverActor {
    pub(super) fn handle_server_ready(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ActorAction {
        let server = mem::take(&mut self.server);
        self.server = match server.ready(signal, port) {
            Ok(x) => x,
            Err(e) => return ActorAction::fail(e),
        };
        info!("driver server is ready on port {port}");
        ActorAction::Continue
    }

    pub(super) fn handle_start_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
    ) -> ActorAction {
        let Some(port) = self.server.port() else {
            return ActorAction::fail("the driver server is not ready");
        };
        let options = WorkerLaunchOptions {
            driver_host: self.options().driver_external_host.to_string(),
            driver_port: self.options().driver_external_port.unwrap_or(port),
        };
        let worker_manager = Arc::clone(&self.worker_manager);
        ctx.spawn(async move {
            if let Err(e) = worker_manager.start_worker(worker_id, options).await {
                error!("failed to start worker {worker_id}: {e}");
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_register_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        host: String,
        port: u16,
    ) -> ActorAction {
        info!("worker {worker_id} is available at {host}:{port}");
        let status = WorkerStatus::Running { host, port };
        self.state.update_worker_status(worker_id, status);
        if let Some(worker) = self.state.get_worker(worker_id) {
            self.schedule_tasks_for_job(ctx, worker.job_id);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_stop_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
    ) -> ActorAction {
        let Some(worker) = self.state.get_worker(worker_id) else {
            return ActorAction::Continue;
        };
        if matches!(worker.status, WorkerStatus::Running { .. }) {
            self.state
                .update_worker_status(worker_id, WorkerStatus::Stopped);
            let worker_manager = Arc::clone(&self.worker_manager);
            ctx.spawn(async move {
                if let Err(e) = worker_manager.stop_worker(worker_id).await {
                    error!("failed to stop worker {worker_id}: {e}");
                }
            });
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
        // TODO: reuse workers
        for (worker_id, _) in self.state.find_workers_for_job(job_id) {
            ctx.send(DriverEvent::StopWorker { worker_id });
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
    ) -> ActorAction {
        self.state
            .update_task_status(task_id, attempt, status, message.clone());
        let Some(task) = self.state.get_task(task_id) else {
            return ActorAction::warn(format!("task {task_id} not found"));
        };
        let job_id = task.job_id;
        match status {
            TaskStatus::Running => {
                self.schedule_tasks_for_job(ctx, job_id);
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
                self.stop_job(ctx, job_id, reason);
            }
            TaskStatus::Pending | TaskStatus::Succeeded | TaskStatus::Canceled => {}
        }
        ActorAction::Continue
    }

    fn accept_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<JobId> {
        let job_id = self.state.next_job_id()?;
        let graph = JobGraph::try_new(job_id, plan)?;
        let worker_ids = self.start_workers_for_job(ctx, job_id)?;
        let mut worker_id_selector = worker_ids.iter().cycle();
        let mut stages = vec![];
        for stage in graph.stages() {
            let mut tasks = vec![];
            for p in 0..stage.output_partitioning().partition_count() {
                let task_id = self.state.next_task_id()?;
                self.state.add_task(
                    task_id,
                    TaskDescriptor {
                        job_id,
                        stage: 0,
                        partition: p,
                        attempt: 0,
                        worker_id: *worker_id_selector.next().unwrap(),
                        mode: TaskMode::Pipelined,
                        status: TaskStatus::Pending,
                        messages: vec![],
                        channel: None, // FIXME
                    },
                );
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

    fn start_workers_for_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
    ) -> ExecutionResult<Vec<WorkerId>> {
        let worker_ids = (0..self.options().worker_count_per_job)
            .map(|_| self.state.next_worker_id())
            .collect::<ExecutionResult<Vec<_>>>()?;
        for x in worker_ids.iter() {
            let descriptor = WorkerDescriptor {
                job_id,
                status: WorkerStatus::Pending,
            };
            self.state.add_worker(*x, descriptor);
            ctx.send(DriverEvent::StartWorker { worker_id: *x });
        }
        Ok(worker_ids)
    }

    fn schedule_tasks_for_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId) {
        let tasks = self
            .state
            .find_schedulable_tasks_for_job(job_id)
            .into_iter()
            .map(|(task_id, task)| TaskAttempt::new(task_id, task.attempt))
            .collect::<Vec<_>>();
        for TaskAttempt { task_id, attempt } in tasks {
            if let Err(e) = self.schedule_task(ctx, task_id) {
                ctx.send(DriverEvent::UpdateTask {
                    task_id,
                    attempt,
                    status: TaskStatus::Failed,
                    message: Some(e.to_string()),
                });
            }
        }
    }

    fn schedule_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
    ) -> ExecutionResult<()> {
        let task = self
            .state
            .get_task(task_id)
            .ok_or_else(|| ExecutionError::InternalError(format!("task {task_id} not found")))?;
        let job = self.state.get_job(task.job_id).ok_or_else(|| {
            ExecutionError::InternalError(format!(
                "job {} not found for task {}",
                task.job_id, task_id
            ))
        })?;
        let stage = job.stages.get(task.stage).ok_or_else(|| {
            ExecutionError::InternalError(format!(
                "stage {} not found for job {} and task {}",
                task.stage, task.job_id, task_id
            ))
        })?;
        let attempt = task.attempt;
        let partition = task.partition;
        let channel = task.channel.clone();
        let plan = self.rewrite_shuffle(stage.plan.clone(), partition)?;
        let plan = self.encode_plan(plan)?;
        let client = self.worker_client(task.worker_id)?.clone();
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
                        message: Some(e.to_string()),
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
        _plan: Arc<dyn ExecutionPlan>,
        _partition: usize,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        todo!()
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
                            self.stop_job(ctx, job_id, reason);
                            return;
                        }
                    };
                    let (sender, receiver) = mpsc::channel(self.options().job_output_buffer);
                    let receiver_stream = Box::pin(RecordBatchStreamAdapter::new(
                        stream.schema(),
                        ReceiverStream::new(receiver),
                    ));
                    if result.send(Ok(receiver_stream)).is_err() {
                        self.stop_job(ctx, job_id, "job output receiver dropped".to_string());
                        return;
                    }
                    self.job_outputs
                        .insert(job_id, JobOutput::run(ctx, job_id, stream, sender));
                } else {
                    self.job_outputs
                        .insert(job_id, JobOutput::Pending { result });
                }
            }
            JobOutput::Running { .. } => {}
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
                self.state.get_task(*task_id).and_then(|task| {
                    matches!(task.status, TaskStatus::Running | TaskStatus::Succeeded)
                        .then(|| (*task_id, task.channel.clone(), task.worker_id))
                })
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|(task_id, channel, worker_id)| {
                let channel = channel.ok_or_else(|| {
                    ExecutionError::InternalError(format!("task channel is not set: {task_id}"))
                })?;
                let client = self.worker_client(worker_id)?.clone();
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

    fn stop_job(&mut self, ctx: &mut ActorContext<Self>, job_id: JobId, reason: String) {
        if let Some(output) = self.job_outputs.remove(&job_id) {
            output.fail(reason);
        }
        let tasks = self
            .state
            .find_running_tasks_for_job(job_id)
            .into_iter()
            .map(|(task_id, task)| (task_id, task.attempt, task.worker_id))
            .collect::<Vec<_>>();
        let tasks = tasks
            .into_iter()
            .flat_map(|(task_id, attempt, worker_id)| {
                let client = match self.worker_client(worker_id) {
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
}
