use std::mem;
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties};
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
    JobDescriptor, JobStage, TaskDescriptor, TaskMode, TaskStatus, WorkerDescriptor,
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
        self.state.add_worker(
            worker_id,
            WorkerDescriptor {
                host,
                port,
                active: true,
            },
        );
        while let Some(job_id) = self.incoming_jobs.pop_front() {
            let subscriber = match self.job_subscribers.remove(&job_id) {
                Some(x) => x,
                None => continue,
            };
            if let Err(e) = self.schedule_job(ctx, worker_id, job_id) {
                let message = e.to_string();
                let _ = subscriber.result.send(Err(e));
                return ActorAction::warn(message);
            }
            self.job_subscribers.insert(job_id, subscriber);
            break;
        }
        ActorAction::Continue
    }

    pub(super) fn handle_stop_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
    ) -> ActorAction {
        if let Some(worker) = self.state.get_worker_mut(&worker_id) {
            if worker.active {
                worker.active = false;
                let worker_manager = Arc::clone(&self.worker_manager);
                ctx.spawn(async move {
                    if let Err(e) = worker_manager.stop_worker(worker_id).await {
                        error!("failed to stop worker {worker_id}: {e}");
                    }
                });
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
        let plan = match plan.output_partitioning().partition_count() {
            0 => {
                let stream = Box::pin(EmptyRecordBatchStream::new(plan.schema()));
                let _ = result.send(Ok(stream));
                return ActorAction::Continue;
            }
            1 => plan,
            // TODO: move this to planner
            2.. => Arc::new(CoalescePartitionsExec::new(Arc::clone(&plan))),
        };
        let job_id = match self.state.next_job_id() {
            Ok(x) => x,
            Err(e) => return ActorAction::fail(e),
        };
        let graph = JobGraph::try_new(job_id, Arc::clone(&plan)).unwrap();
        println!("{graph}");
        let mut tasks = vec![];
        for p in 0..plan.output_partitioning().partition_count() {
            let task_id = match self.state.next_task_id() {
                Ok(x) => x,
                Err(e) => return ActorAction::fail(e),
            };
            self.state.add_task(
                task_id,
                TaskDescriptor {
                    job_id,
                    stage: 0,
                    partition: p,
                    attempt: 0,
                    mode: TaskMode::Pipelined,
                    status: TaskStatus::Pending,
                    messages: vec![],
                },
            );
            tasks.push(task_id);
        }
        let descriptor = JobDescriptor {
            stages: vec![JobStage { plan, tasks }],
        };
        self.state.add_job(job_id, descriptor);
        self.incoming_jobs.push_back(job_id);
        self.job_subscribers
            .insert(job_id, JobSubscriber { result });
        // TODO: update worker launch logic
        let worker_id = match self.state.next_worker_id() {
            Ok(x) => x,
            Err(e) => return ActorAction::fail(e),
        };
        ctx.send(DriverEvent::StartWorker { worker_id });
        ActorAction::Continue
    }

    pub(super) fn handle_update_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        task_id: TaskId,
        status: TaskStatus,
        message: Option<String>,
    ) -> ActorAction {
        let task = match self.state.get_task_mut(task_id) {
            Ok(x) => x,
            Err(e) => return ActorAction::warn(e),
        };
        if let Some(message) = &message {
            task.messages.push(message.clone());
        }
        task.status = status;
        match task.status {
            TaskStatus::Running => {
                if let Some(subscriber) = self.job_subscribers.remove(&task.job_id) {
                    let attempt = task.attempt;
                    let stage = match self.state.find_job_stage_by_task(task_id) {
                        Ok(x) => x,
                        Err(e) => return ActorAction::warn(e),
                    };
                    let schema = stage.plan.schema();
                    let client = match self.worker_client(worker_id) {
                        Ok(x) => x.clone(),
                        Err(e) => return ActorAction::warn(e),
                    };
                    ctx.spawn(async move {
                        let out = client.fetch_task_stream(task_id, attempt, schema).await;
                        let _ = subscriber.result.send(out);
                    });
                }
            }
            TaskStatus::Failed => {
                let job_id = task.job_id;
                if let Some(subscriber) = self.job_subscribers.remove(&job_id) {
                    let _ = subscriber
                        .result
                        .send(Err(ExecutionError::InternalError(format!(
                            "task failed or canceled: {}",
                            message.as_deref().unwrap_or("unknown reason")
                        ))));
                }
            }
            _ => {}
        }
        ActorAction::Continue
    }

    fn schedule_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        job_id: JobId,
    ) -> ExecutionResult<()> {
        // TODO: create job stages and implement scheduling logic
        let _ = self
            .state
            .get_worker(&worker_id)
            .ok_or_else(|| ExecutionError::InternalError("worker not found".to_string()))?;
        let stage = &self.state.get_job(job_id)?.stages[0];
        let plan = self.encode_plan(Arc::clone(&stage.plan))?;
        let task_id = stage.tasks[0];
        let task = self.state.get_task(task_id)?;
        let attempt = task.attempt;
        let client = self.worker_client(worker_id)?.clone();
        ctx.spawn(async move {
            if let Err(e) = client.run_task(task_id, attempt, plan, 0).await {
                error!("failed to run task {task_id} on worker {worker_id}: {e}");
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
