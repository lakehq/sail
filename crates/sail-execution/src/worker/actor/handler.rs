use std::mem;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::info;
use prost::Message;
use sail_server::actor::ActorContext;
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskId;
use crate::worker::actor::core::WorkerActor;
use crate::worker::actor::TaskAttempt;

impl WorkerActor {
    pub(super) fn handle_server_ready(
        &mut self,
        ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ExecutionResult<()> {
        let worker_id = self.options().worker_id;
        info!("worker {worker_id} server is ready on port {port}");
        let server = mem::take(&mut self.server);
        self.server = server.ready(signal, port)?;
        let host = self.options().worker_external_host.clone();
        let port = self.options().worker_external_port.unwrap_or(port);
        let client = self.driver_client.clone();
        ctx.spawn(async move { client.register_worker(worker_id, host, port).await });
        Ok(())
    }

    pub(super) fn handle_run_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        plan: Vec<u8>,
        partition: usize,
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
            self.task_streams
                .insert(TaskAttempt::new(task_id, attempt), stream);
            Ok(())
        };
        let status = match out {
            Ok(_) => TaskStatus::Running,
            Err(_) => TaskStatus::Failed,
        };
        let client = self.driver_client.clone();
        ctx.spawn(async move { client.report_task_status(task_id, status).await });
        Ok(())
    }

    pub(super) fn handle_stop_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
    ) -> ExecutionResult<()> {
        self.task_streams
            .remove(&TaskAttempt::new(task_id, attempt));
        let status = TaskStatus::Canceled;
        let client = self.driver_client.clone();
        ctx.spawn(async move { client.report_task_status(task_id, status).await });
        Ok(())
    }

    pub(super) fn handle_fetch_task_stream(
        &mut self,
        _ctx: &ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        result: oneshot::Sender<SendableRecordBatchStream>,
    ) -> ExecutionResult<()> {
        let key = TaskAttempt::new(task_id, attempt);
        let stream = self.task_streams.remove(&key).ok_or_else(|| {
            ExecutionError::InternalError(format!(
                "task stream not found: {task_id} attempt {attempt}"
            ))
        })?;
        let _ = result.send(stream);
        Ok(())
    }

    fn session_context(&self) -> Arc<SessionContext> {
        Arc::new(SessionContext::default())
    }

    fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }
}
