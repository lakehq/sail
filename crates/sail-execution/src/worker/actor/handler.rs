use std::mem;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{debug, error, info};
use prost::Message;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskAttempt, TaskId, WorkerId};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec};
use crate::stream::{ChannelName, RecordBatchStreamWriter, TaskStreamPersistence};
use crate::worker::actor::core::WorkerActor;
use crate::worker::actor::local_stream::{EphemeralStream, LocalStream, MemoryStream};
use crate::worker::actor::stream_accessor::WorkerStreamAccessor;
use crate::worker::actor::stream_monitor::TaskStreamMonitor;
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
        let port = if self.options().worker_external_port > 0 {
            self.options().worker_external_port
        } else {
            port
        };
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
        let stream = match self.execute_plan(ctx, task_id, attempt, plan, partition) {
            Ok(x) => x,
            Err(e) => {
                let event = WorkerEvent::ReportTaskStatus {
                    task_id,
                    attempt,
                    status: TaskStatus::Failed,
                    message: Some(format!("failed to execute plan: {e}")),
                };
                ctx.send(event);
                return ActorAction::Continue;
            }
        };
        let handle = ctx.handle().clone();
        let (tx, rx) = oneshot::channel();
        self.task_signals
            .insert(TaskAttempt::new(task_id, attempt), tx);
        let monitor = if let Some(channel) = channel {
            let mut output =
                EphemeralStream::new(self.options().worker_stream_buffer, stream.schema());
            let writer = match output.publish(ctx) {
                Ok(x) => x,
                Err(e) => {
                    let event = WorkerEvent::ReportTaskStatus {
                        task_id,
                        attempt,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to create output stream writer: {e}")),
                    };
                    ctx.send(event);
                    return ActorAction::Continue;
                }
            };
            self.local_streams.insert(channel, Box::new(output));
            TaskStreamMonitor::new(handle, task_id, attempt, stream, Some(writer), rx)
        } else {
            TaskStreamMonitor::new(handle, task_id, attempt, stream, None, rx)
        };
        ctx.spawn(monitor.run());
        ActorAction::Continue
    }

    pub(super) fn handle_stop_task(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
    ) -> ActorAction {
        let key = TaskAttempt::new(task_id, attempt);
        if let Some(signal) = self.task_signals.remove(&key) {
            let _ = signal.send(());
        }
        ActorAction::Continue
    }

    pub(super) fn handle_report_task_status(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
        message: Option<String>,
    ) -> ActorAction {
        let sequence = self.sequence;
        self.sequence = match self.sequence.checked_add(1) {
            Some(x) => x,
            None => return ActorAction::fail("sequence number overflow"),
        };
        let client = self.driver_client.clone();
        ctx.spawn(async move {
            if let Err(e) = client
                .report_task_status(task_id, attempt, status, message, sequence)
                .await
            {
                error!("failed to report task status: {e}");
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_create_local_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        channel: ChannelName,
        persistence: TaskStreamPersistence,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn RecordBatchStreamWriter>>>,
    ) -> ActorAction {
        let mut stream: Box<dyn LocalStream> = match persistence {
            TaskStreamPersistence::Ephemeral => Box::new(EphemeralStream::new(
                self.options().worker_stream_buffer,
                schema,
            )),
            TaskStreamPersistence::Memory => Box::new(MemoryStream::new(schema)),
            TaskStreamPersistence::Disk => {
                return ActorAction::fail("not implemented: create disk stream")
            }
        };
        let _ = result.send(stream.publish(ctx));
        self.local_streams.insert(channel, stream);
        ActorAction::Continue
    }

    pub(super) fn handle_create_remote_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _uri: String,
        _schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn RecordBatchStreamWriter>>>,
    ) -> ActorAction {
        let _ = result.send(Err(ExecutionError::InternalError(
            "not implemented: create remote stream".to_string(),
        )));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_this_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        channel: ChannelName,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let out = self
            .local_streams
            .get_mut(&channel)
            .map(|x| {
                x.subscribe(ctx).map_err(|e| {
                    ExecutionError::InternalError(format!(
                        "failed to read task stream {channel}: {e}"
                    ))
                })
            })
            .unwrap_or_else(|| {
                Err(ExecutionError::InternalError(format!(
                    "task stream not found: {channel}"
                )))
            });
        let _ = result.send(out);
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_other_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        host: String,
        port: u16,
        channel: ChannelName,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let client = match self.worker_client(worker_id, host, port) {
            Ok(x) => x.clone(),
            Err(e) => {
                let _ = result.send(Err(e));
                return ActorAction::Continue;
            }
        };
        ctx.spawn(async move {
            let out = client.fetch_task_stream(channel, schema).await;
            let _ = result.send(out);
        });
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_remote_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _uri: String,
        _schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    ) -> ActorAction {
        let _ = result.send(Err(ExecutionError::InternalError(
            "not implemented: fetch remote stream".to_string(),
        )));
        ActorAction::Continue
    }

    fn session_context(&self) -> Arc<SessionContext> {
        Arc::new(SessionContext::default())
    }

    fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    fn execute_plan(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        plan: Vec<u8>,
        partition: usize,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let session_ctx = self.session_context();
        let plan = PhysicalPlanNode::decode(plan.as_slice())?;
        let plan = plan.try_into_physical_plan(
            session_ctx.as_ref(),
            &session_ctx.runtime_env(),
            self.physical_plan_codec.as_ref(),
        )?;
        let plan = self.rewrite_shuffle(ctx, plan)?;
        debug!(
            "task {} attempt {} execution plan\n{}",
            task_id,
            attempt,
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let stream = plan.execute(partition, self.task_context())?;
        Ok(stream)
    }

    fn rewrite_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let worker_id = self.options().worker_id;
        let handle = ctx.handle();
        let result = plan.transform(move |node| {
            if let Some(shuffle) = node.as_any().downcast_ref::<ShuffleReadExec>() {
                let accessor = WorkerStreamAccessor::new(worker_id, handle.clone());
                let shuffle = shuffle.clone().with_reader(Some(Arc::new(accessor)));
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else if let Some(shuffle) = node.as_any().downcast_ref::<ShuffleWriteExec>() {
                let accessor = WorkerStreamAccessor::new(worker_id, handle.clone());
                let shuffle = shuffle.clone().with_writer(Some(Arc::new(accessor)));
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else {
                Ok(Transformed::no(node))
            }
        });
        Ok(result.data()?)
    }
}
