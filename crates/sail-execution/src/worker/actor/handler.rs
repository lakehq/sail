use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{debug, error, info, warn};
use prost::Message;
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::schema_adapter::DeltaSchemaAdapterFactory;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{ActorAction, ActorContext};
use sail_telemetry::telemetry::global_metric_registry;
use sail_telemetry::{trace_execution_plan, TracingExecOptions};
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskAttempt, TaskId, WorkerId};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec};
use crate::stream::channel::ChannelName;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
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
            Err(e) => {
                error!("{e}");
                return ActorAction::Stop;
            }
        };
        let host = self.options().worker_external_host.clone();
        let port = if self.options().worker_external_port > 0 {
            self.options().worker_external_port
        } else {
            port
        };
        let retry_strategy = self.options().rpc_retry_strategy.clone();
        let client = self.driver_client();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Err(e) = retry_strategy
                .run(|| {
                    let client = client.clone();
                    let host = host.clone();
                    async move { client.register_worker(worker_id, host, port).await }
                })
                .await
            {
                error!("failed to register worker with retries: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
            if let Err(e) = handle.send(WorkerEvent::StartHeartbeat).await {
                error!("failed to start worker heartbeat: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_start_heartbeat(&mut self, ctx: &mut ActorContext<Self>) -> ActorAction {
        let worker_id = self.options().worker_id;
        let client = self.driver_client();
        let interval = self.options().worker_heartbeat_interval;
        ctx.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = client.report_worker_heartbeat(worker_id).await {
                    warn!("failed to report worker heartbeat: {e}");
                    // We do not retry heartbeat since we will report heartbeat again after
                    // the configured interval.
                }
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
                    cause: Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
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
            let mut output = EphemeralStream::new(self.options().worker_stream_buffer);
            let writer = match output.publish(ctx) {
                Ok(x) => x,
                Err(e) => {
                    let event = WorkerEvent::ReportTaskStatus {
                        task_id,
                        attempt,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to create output stream writer: {e}")),
                        cause: None,
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
        cause: Option<CommonErrorCause>,
    ) -> ActorAction {
        let sequence = self.sequence;
        self.sequence = match self.sequence.checked_add(1) {
            Some(x) => x,
            None => {
                error!("sequence number overflow");
                return ActorAction::Stop;
            }
        };
        let client = self.driver_client();
        let handle = ctx.handle().clone();
        let retry_strategy = self.options().rpc_retry_strategy.clone();
        ctx.spawn(async move {
            if let Err(e) = retry_strategy
                .run(|| {
                    let client = client.clone();
                    let message = message.clone();
                    let cause = cause.clone();
                    async move {
                        client
                            .report_task_status(task_id, attempt, status, message, cause, sequence)
                            .await
                    }
                })
                .await
            {
                error!("failed to report task status with retries: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_create_local_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        channel: ChannelName,
        storage: LocalStreamStorage,
        _schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        let mut stream: Box<dyn LocalStream> = match storage {
            LocalStreamStorage::Ephemeral => {
                Box::new(EphemeralStream::new(self.options().worker_stream_buffer))
            }
            LocalStreamStorage::Memory => Box::new(MemoryStream::new()),
            LocalStreamStorage::Disk => {
                error!("not implemented: create disk stream");
                return ActorAction::Stop;
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
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
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
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
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
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
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
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(Err(ExecutionError::InternalError(
            "not implemented: fetch remote stream".to_string(),
        )));
        ActorAction::Continue
    }

    pub(super) fn handle_remove_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        channel_prefix: String,
    ) -> ActorAction {
        let mut keys = Vec::new();
        for key in self.local_streams.keys() {
            if key.has_prefix(&channel_prefix) {
                keys.push(key.clone());
            }
        }
        for key in keys {
            self.local_streams.remove(&key);
        }
        ActorAction::Continue
    }

    fn execute_plan(
        &mut self,
        ctx: &mut ActorContext<Self>,
        task_id: TaskId,
        attempt: usize,
        plan: Vec<u8>,
        partition: usize,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let session_ctx = self.session_context()?;
        let plan = PhysicalPlanNode::decode(plan.as_slice())?;
        let plan = plan.try_into_physical_plan(
            session_ctx.task_ctx().as_ref(),
            self.physical_plan_codec.as_ref(),
        )?;
        let plan = self.rewrite_parquet_adapters(plan)?;
        let plan = self.rewrite_shuffle(ctx, plan)?;
        debug!(
            "task {} attempt {} execution plan\n{}",
            task_id,
            attempt,
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let options = TracingExecOptions {
            metric_registry: global_metric_registry(),
            job_id: None, // TODO: propagate job ID
            task_id: Some(task_id.into()),
            task_attempt: Some(attempt),
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        let stream = plan.execute(partition, session_ctx.task_ctx())?;
        Ok(stream)
    }

    fn rewrite_parquet_adapters(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform(|node| {
            if let Some(ds) = node.as_any().downcast_ref::<DataSourceExec>() {
                if let Some((base_config, _parquet)) = ds.downcast_to_file_source::<ParquetSource>()
                {
                    let builder = FileScanConfigBuilder::from(base_config.clone());
                    let new_source = base_config
                        .file_source()
                        .with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory))?;
                    let new_exec =
                        DataSourceExec::from_data_source(builder.with_source(new_source).build());
                    return Ok(Transformed::yes(new_exec as Arc<dyn ExecutionPlan>));
                }
            }
            Ok(Transformed::no(node))
        });
        Ok(result.data()?)
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
