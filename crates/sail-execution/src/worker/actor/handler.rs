use std::mem;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{error, info};
use prost::Message;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskId;
use crate::worker::actor::core::WorkerActor;
use crate::worker::actor::TaskAttempt;
use crate::worker::WorkerEvent;

impl WorkerActor {
    pub(super) fn handle_server_ready(
        &mut self,
        ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ActorAction {
        let worker_id = self.options().worker_id;
        info!("worker {worker_id} server is ready on port {port}");
        let server = mem::take(&mut self.server);
        self.server = match server.ready(signal, port) {
            Ok(x) => x,
            Err(e) => return ActorAction::fail(e),
        };
        let host = self.options().worker_external_host.clone();
        let port = self.options().worker_external_port.unwrap_or(port);
        let client = self.driver_client.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Err(e) = client.register_worker(worker_id, host, port).await {
                error!("failed to register worker: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_run_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        plan: Vec<u8>,
        partition: usize,
    ) -> ActorAction {
        let mut execute = || -> ExecutionResult<()> {
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

        let (status, message) = match execute() {
            Ok(()) => (TaskStatus::Running, None),
            Err(e) => {
                error!("failed to run task: {e}");
                (TaskStatus::Failed, Some(e.to_string()))
            }
        };
        self.report_task_status(ctx, task_id, status, message);
        ActorAction::Continue
    }

    pub(super) fn handle_stop_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
    ) -> ActorAction {
        self.task_streams
            .remove(&TaskAttempt::new(task_id, attempt));
        let status = TaskStatus::Canceled;
        self.report_task_status(ctx, task_id, status, None);
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_task_stream(
        &mut self,
        _ctx: &ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let key = TaskAttempt::new(task_id, attempt);
        let out = self.task_streams.remove(&key).ok_or_else(|| {
            ExecutionError::InternalError(format!(
                "task stream not found: {task_id} attempt {attempt}"
            ))
        });
        let _ = result.send(out);
        ActorAction::Continue
    }

    fn report_task_status(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        status: TaskStatus,
        message: Option<String>,
    ) {
        let client = self.driver_client.clone();
        ctx.spawn(async move {
            if let Err(e) = client.report_task_status(task_id, status, message).await {
                error!("failed to report task status: {e}");
            }
        });
    }

    fn session_context(&self) -> Arc<SessionContext> {
        Arc::new(SessionContext::default())
    }

    fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }
}
