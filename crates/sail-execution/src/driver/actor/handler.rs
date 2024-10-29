use std::mem;
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{error, info};
use prost::bytes::BytesMut;
use prost::Message;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::driver::actor::core::JobSubscriber;
use crate::driver::actor::DriverActor;
use crate::driver::planner::JobGraph;
use crate::driver::state::{
    JobDescriptor, JobStage, TaskDescriptor, TaskMode, TaskStatus, WorkerStatus,
};
use crate::driver::DriverEvent;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskId, WorkerId};
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
        self.schedule_tasks(ctx);
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
                self.job_subscribers
                    .insert(job_id, JobSubscriber { result });
            }
            Err(e) => {
                let _ = result.send(Err(e));
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_update_task(
        &mut self,
        _ctx: &mut ActorContext<Self>,
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
        match status {
            TaskStatus::Running => {
                // TODO: collect task output from the last stage
                if let Some(_subscriber) = self.job_subscribers.remove(&task.job_id) {
                    // let client = match self.worker_client(task.worker_id) {
                    //     Ok(x) => x.clone(),
                    //     Err(e) => return ActorAction::warn(e),
                    // };
                    // if let Some(channel) = task.channel.clone() {
                    //     ctx.spawn(async move {
                    //         let out = client.fetch_task_stream(channel, schema).await;
                    //         let _ = subscriber.result.send(out);
                    //     });
                    // }
                }
            }
            TaskStatus::Failed => {
                let job_id = task.job_id;
                if let Some(subscriber) = self.job_subscribers.remove(&job_id) {
                    let _ = subscriber
                        .result
                        .send(Err(ExecutionError::InternalError(format!(
                            "task failed: {}",
                            message.as_deref().unwrap_or("unknown reason")
                        ))));
                }
                // TODO: support task retry
                // TODO: fail all tasks in the job after the maximum attempt
            }
            _ => {}
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
        // We start a fixed number of workers for each job for now.
        // TODO: implement worker reuse logic
        let worker_ids = self.start_workers(ctx, 4)?;
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
                        channel: None,
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

    fn start_workers(
        &mut self,
        ctx: &mut ActorContext<Self>,
        count: usize,
    ) -> ExecutionResult<Vec<WorkerId>> {
        let worker_ids = (0..count)
            .map(|_| self.state.next_worker_id())
            .collect::<ExecutionResult<Vec<_>>>()?;
        for x in worker_ids.iter() {
            ctx.send(DriverEvent::StartWorker { worker_id: *x });
        }
        Ok(worker_ids)
    }

    fn schedule_tasks(&mut self, ctx: &mut ActorContext<Self>) {
        // TODO: find tasks that can be scheduled
        let task_ids = vec![];
        for (task_id, attempt) in task_ids {
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
        // TODO: rewrite the plan for shuffle read and write
        let plan = self.encode_plan(Arc::clone(&stage.plan))?;
        let attempt = task.attempt;
        let partition = task.partition;
        let channel = task.channel.clone();
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
}
