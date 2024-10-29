use std::mem;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{error, info};
use prost::Message;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::{mpsc, oneshot};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::tokio_stream::StreamExt;

use crate::driver::state::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskId, WorkerId};
use crate::stream::ChannelName;
use crate::worker::actor::core::WorkerActor;
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
        channel: Option<ChannelName>,
    ) -> ActorAction {
        let execute = || -> ExecutionResult<()> {
            let ctx = self.session_context();
            let plan = PhysicalPlanNode::decode(plan.as_slice())?;
            let plan = plan.try_into_physical_plan(
                ctx.as_ref(),
                &ctx.runtime_env(),
                self.physical_plan_codec.as_ref(),
            )?;
            // TODO: monitor the stream and update task status
            let stream = plan.execute(partition, self.task_context())?;
            if let Some(channel) = channel {
                self.memory_streams.insert(channel, stream);
            }
            Ok(())
        };

        let (status, message) = match execute() {
            Ok(()) => (TaskStatus::Running, None),
            Err(e) => {
                error!("failed to run task: {e}");
                (TaskStatus::Failed, Some(e.to_string()))
            }
        };
        self.report_task_status(ctx, task_id, attempt, status, message);
        ActorAction::Continue
    }

    pub(super) fn handle_stop_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
    ) -> ActorAction {
        let status = TaskStatus::Canceled;
        self.report_task_status(ctx, task_id, attempt, status, None);
        ActorAction::Continue
    }

    pub(super) fn handle_create_memory_task_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        channel: ChannelName,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<mpsc::Sender<RecordBatch>>>,
    ) -> ActorAction {
        let (tx, rx) = mpsc::channel(self.options().memory_stream_buffer);
        let stream = RecordBatchStreamAdapter::new(schema, ReceiverStream::new(rx).map(Ok));
        self.memory_streams.insert(channel, Box::pin(stream));
        let _ = result.send(Ok(tx));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_this_worker_task_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        channel: ChannelName,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let out = self.memory_streams.remove(&channel).ok_or_else(|| {
            ExecutionError::InternalError(format!("task stream not found: {channel}"))
        });
        let _ = result.send(out);
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_other_worker_task_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        channel: ChannelName,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let client = self.worker_clients.get(&worker_id).cloned();
        ctx.spawn(async move {
            let stream = match client {
                Some(client) => client.fetch_task_stream(channel, schema).await,
                None => Err(ExecutionError::InternalError(format!(
                    "worker not found: {worker_id}"
                ))),
            };
            let _ = result.send(stream);
        });
        ActorAction::Continue
    }

    fn report_task_status(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
        message: Option<String>,
    ) {
        let client = self.driver_client.clone();
        ctx.spawn(async move {
            if let Err(e) = client
                .report_task_status(task_id, attempt, status, message)
                .await
            {
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
