use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::info;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use tokio::sync::oneshot;

use crate::driver::actor::DriverActor;
use crate::driver::state::{
    JobDescriptor, JobStage, TaskDescriptor, TaskMode, TaskStatus, WorkerDescriptor,
};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskId, WorkerId};
use crate::job::JobDefinition;
use crate::rpc::ServerMonitor;
use crate::worker_manager::WorkerLaunchContext;

impl DriverActor {
    pub(super) fn handle_server_ready(
        &mut self,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ExecutionResult<()> {
        self.server = ServerMonitor::running(signal);
        self.server_listen_port = Some(port);
        info!("driver server is ready on port {port}");
        Ok(())
    }

    pub(super) fn handle_register_worker(
        &mut self,
        worker_id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<()> {
        info!("worker {worker_id} is available at {host}:{port}");
        self.state.add_worker(
            worker_id,
            WorkerDescriptor {
                host,
                port,
                active: true,
            },
        );
        if let Some((job_id, result)) = self.incoming_job_queue.pop_front() {
            self.schedule_job(worker_id, job_id)?;
            self.pending_jobs.insert(job_id, result);
        }
        Ok(())
    }

    pub(super) fn handle_execute_job(
        &mut self,
        job: JobDefinition,
        result: oneshot::Sender<SendableRecordBatchStream>,
    ) -> ExecutionResult<()> {
        let JobDefinition { plan } = job;
        let plan = match plan.output_partitioning().partition_count() {
            0 => {
                let stream = Box::pin(EmptyRecordBatchStream::new(plan.schema()));
                let _ = result.send(stream);
                return Ok(());
            }
            1 => plan,
            2.. => Arc::new(CoalescePartitionsExec::new(Arc::clone(&plan))),
        };
        let job_id = self.state.job_id_generator.next()?;
        let mut tasks = vec![];
        for p in 0..plan.output_partitioning().partition_count() {
            let task_id = self.state.task_id_generator.next()?;
            self.state.add_task(
                task_id,
                TaskDescriptor {
                    job_id,
                    stage: 0,
                    partition: p,
                    attempt: 0,
                    mode: TaskMode::Pipelined,
                    status: TaskStatus::Pending,
                },
            );
            tasks.push(task_id);
        }
        let descriptor = JobDescriptor {
            stages: vec![JobStage { plan, tasks }],
        };
        self.state.add_job(job_id, descriptor);
        self.incoming_job_queue.push_back((job_id, result));
        // TODO: update worker launch logic
        self.launch_worker()?;
        Ok(())
    }

    pub(super) fn handle_task_updated(
        &mut self,
        worker_id: WorkerId,
        task_id: TaskId,
        status: TaskStatus,
    ) -> ExecutionResult<()> {
        let task = self.state.get_task_mut(task_id)?;
        task.status = status;
        if matches!(task.status, TaskStatus::Running) {
            if let Some(result) = self.pending_jobs.remove(&task.job_id) {
                let attempt = task.attempt;
                let stage = self.state.find_job_stage_by_task(task_id)?;
                let schema = stage.plan.schema();
                let client = self.worker_client(worker_id)?.clone();
                tokio::spawn(async move {
                    let stream = client.fetch_task_stream(task_id, attempt, schema).await?;
                    let _ = result.send(stream);
                    Ok::<_, ExecutionError>(())
                });
            }
        }
        Ok(())
    }

    fn launch_worker(&mut self) -> ExecutionResult<()> {
        let port = self
            .server_listen_port
            .ok_or_else(|| ExecutionError::InternalError("server port is not set".to_string()))?;
        let id = self.state.worker_id_generator.next()?;
        let ctx = WorkerLaunchContext {
            driver_host: self.options().driver_external_host.to_string(),
            driver_port: self.options().driver_external_port.unwrap_or(port),
        };
        self.worker_manager.launch_worker(id, ctx)?;
        Ok(())
    }

    fn schedule_job(&mut self, worker_id: WorkerId, job_id: JobId) -> ExecutionResult<()> {
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
        tokio::spawn(async move { client.run_task(task_id, attempt, plan, 0).await });
        Ok(())
    }

    fn encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Vec<u8>> {
        let plan =
            PhysicalPlanNode::try_from_physical_plan(plan, self.physical_plan_codec.as_ref())?;
        let mut buffer = BytesMut::new();
        plan.encode(&mut buffer)?;
        let plan: Bytes = buffer.into();
        Ok(plan.into())
    }
}
