use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::info;
use prost::Message;
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::TaskId;
use crate::rpc::ServerMonitor;
use crate::worker::actor::core::WorkerActor;

impl WorkerActor {
    pub(super) fn handle_server_ready(
        &mut self,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ExecutionResult<()> {
        let worker_id = self.options().worker_id;
        info!("worker {worker_id} server is ready on port {port}");
        self.server = ServerMonitor::running(signal);
        let host = self.options().worker_external_host.clone();
        let port = self.options().worker_external_port.unwrap_or(port);
        let client = self.driver_client()?.clone();
        tokio::spawn(async move { client.register_worker(worker_id, host, port).await });
        Ok(())
    }

    pub(super) fn handle_run_task(
        &mut self,
        task_id: TaskId,
        partition: usize,
        plan: Vec<u8>,
    ) -> ExecutionResult<()> {
        let out: ExecutionResult<()> = {
            let ctx = self.session_context();
            let plan = PhysicalPlanNode::decode(plan.as_slice())?;
            let plan = plan.try_into_physical_plan(
                ctx.as_ref(),
                &ctx.runtime_env(),
                self.physical_plan_codec.as_ref(),
            )?;
            let stream = plan.execute(partition, self.task_context())?;
            self.task_streams.insert((task_id, partition), stream);
            Ok(())
        };
        let status = match out {
            Ok(_) => TaskStatus::Running,
            Err(_) => TaskStatus::Failed,
        };
        let client = self.driver_client()?.clone();
        tokio::spawn(async move { client.report_task_status(task_id, partition, status).await });
        Ok(())
    }

    pub(super) fn handle_stop_task(
        &mut self,
        task_id: TaskId,
        partition: usize,
    ) -> ExecutionResult<()> {
        self.task_streams.remove(&(task_id, partition));
        let status = TaskStatus::Canceled;
        let client = self.driver_client()?.clone();
        tokio::spawn(async move { client.report_task_status(task_id, partition, status).await });
        Ok(())
    }

    fn session_context(&self) -> Arc<SessionContext> {
        Arc::new(SessionContext::default())
    }

    fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }
}
