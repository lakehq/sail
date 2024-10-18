use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::info;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use tokio::sync::oneshot;

use crate::driver::actor::DriverActor;
use crate::driver::event::TaskChannel;
use crate::driver::state::{JobDescriptor, TaskStatus, WorkerDescriptor};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskId, WorkerId};
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
        info!("registering worker {worker_id} at {host}:{port}");
        self.state.add_worker(
            worker_id,
            WorkerDescriptor {
                host,
                port,
                active: true,
            },
        );
        self.schedule_task(worker_id)?;
        Ok(())
    }

    pub(super) fn handle_execute_job(
        &mut self,
        job: JobDefinition,
        channel: TaskChannel,
    ) -> ExecutionResult<()> {
        let JobDefinition { plan } = job;
        let job_id = self.job_id_generator.next()?;
        let task_id = self.task_id_generator.next()?;
        self.state.add_job(job_id, JobDescriptor { task_id, plan });
        self.state.add_task(task_id, 0, job_id);
        self.task_channels.insert(task_id, channel);
        self.launch_worker()?;
        Ok(())
    }

    pub(super) fn handle_task_updated(
        &mut self,
        task_id: TaskId,
        partition: usize,
        status: TaskStatus,
    ) -> ExecutionResult<()> {
        self.state.update_task(task_id, partition, status);
        Ok(())
    }

    fn launch_worker(&mut self) -> ExecutionResult<()> {
        let port = self
            .server_listen_port
            .ok_or_else(|| ExecutionError::InternalError("server port is not set".to_string()))?;
        let id = self.worker_id_generator.next()?;
        let ctx = WorkerLaunchContext {
            driver_host: self.options().driver_external_host.to_string(),
            driver_port: self.options().driver_external_port.unwrap_or(port),
        };
        self.worker_manager.launch_worker(id, ctx)?;
        Ok(())
    }

    fn schedule_task(&mut self, worker_id: WorkerId) -> ExecutionResult<()> {
        if let Some((task_id, partition)) = self.state.get_one_pending_task() {
            let _ = self
                .state
                .get_worker(&worker_id)
                .ok_or_else(|| ExecutionError::InternalError("worker not found".to_string()))?;
            let plan = self
                .state
                .get_task_plan(task_id)
                .ok_or_else(|| ExecutionError::InternalError("task plan not found".to_string()))?;
            let plan = self.encode_plan(plan)?;
            let client = self.worker_client(worker_id)?.clone();
            tokio::spawn(async move { client.run_task(task_id, partition, plan).await });
        }
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
