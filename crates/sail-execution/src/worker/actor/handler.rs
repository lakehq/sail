use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
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

use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey, WorkerId};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::worker::actor::stream_accessor::WorkerStreamAccessor;
use crate::worker::actor::task_monitor::TaskMonitor;
use crate::worker::actor::WorkerActor;
use crate::worker::event::WorkerLocation;
use crate::worker::task::{TaskDefinition, TaskInput, TaskOutput};
use crate::worker::WorkerEvent;

impl WorkerActor {
    pub(super) fn handle_server_ready(
        &mut self,
        ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ActorAction {
        let worker_id = self.options.worker_id;
        info!("worker {worker_id} server is ready on port {port}");
        let server = mem::take(&mut self.server);
        self.server = match server.ready(signal) {
            Ok(x) => x,
            Err(e) => {
                error!("{e}");
                return ActorAction::Stop;
            }
        };
        let host = self.options.worker_external_host.clone();
        let port = if self.options.worker_external_port > 0 {
            self.options.worker_external_port
        } else {
            port
        };
        let retry_strategy = self.options.rpc_retry_strategy.clone();
        let client = self.driver_client_set.core.clone();
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
        let worker_id = self.options.worker_id;
        let client = self.driver_client_set.core.clone();
        let interval = self.options.worker_heartbeat_interval;
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

    pub(super) fn handle_report_known_peers(
        &mut self,
        ctx: &mut ActorContext<Self>,
        peer_worker_ids: Vec<WorkerId>,
    ) -> ActorAction {
        let worker_id = self.options.worker_id;
        let client = self.driver_client_set.core.clone();
        ctx.spawn(async move {
            if let Err(e) = client
                .report_worker_known_peers(worker_id, peer_worker_ids)
                .await
            {
                warn!("failed to report worker known peers: {e}");
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_run_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        definition: TaskDefinition,
        peers: Vec<WorkerLocation>,
    ) -> ActorAction {
        self.peer_tracker.track(ctx, peers);
        let stream = match self.execute_plan(ctx, &key, definition) {
            Ok(x) => x,
            Err(e) => {
                let event = WorkerEvent::ReportTaskStatus {
                    key,
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
        self.task_signals.insert(key.clone(), tx);
        let monitor = TaskMonitor::new(handle, key, stream, rx);
        ctx.spawn(monitor.run());
        ActorAction::Continue
    }

    pub(super) fn handle_stop_task(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskKey,
    ) -> ActorAction {
        if let Some(signal) = self.task_signals.remove(&key) {
            let _ = signal.send(());
        }
        ActorAction::Continue
    }

    pub(super) fn handle_report_task_status(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
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
        let client = self.driver_client_set.core.clone();
        let handle = ctx.handle().clone();
        let retry_strategy = self.options.rpc_retry_strategy.clone();
        ctx.spawn(async move {
            if let Err(e) = retry_strategy
                .run(|| {
                    let client = client.clone();
                    let key = key.clone();
                    let message = message.clone();
                    let cause = cause.clone();
                    async move {
                        client
                            .report_task_status(key, status, message, cause, sequence)
                            .await
                    }
                })
                .await
            {
                // Failure to report task status is considered fatal so that
                // the driver can eventually detect this lost worker due to
                // missing worker heartbeats and mark all the task attempts
                // on this worker as failed.
                error!("failed to report task status with retries: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_create_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        let _ = result.send(
            self.stream_manager
                .create_local_stream(key, storage, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_create_remote_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.create_remote_stream(uri, key, schema));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_driver_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let client = self.driver_client_set.flight.clone();
        ctx.spawn(async move {
            let out = client.fetch_task_stream(key, schema).await;
            let _ = result.send(out);
        });
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_this_worker_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.fetch_local_stream(&key));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_other_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let client = match self.peer_tracker.get_client_set(worker_id) {
            Ok(x) => x.flight,
            Err(e) => {
                let _ = result.send(Err(e));
                return ActorAction::Continue;
            }
        };
        ctx.spawn(async move {
            let out = client.fetch_task_stream(key, schema).await;
            let _ = result.send(out);
        });
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_remote_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.fetch_remote_stream(uri, &key, schema));
        ActorAction::Continue
    }

    pub(super) fn handle_remove_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: Option<usize>,
    ) -> ActorAction {
        self.stream_manager.remove_local_stream(job_id, stage);
        ActorAction::Continue
    }

    fn execute_plan(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: &TaskKey,
        definition: TaskDefinition,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let task_ctx = self.options.session.task_ctx();
        let plan = PhysicalPlanNode::decode(definition.plan.as_ref())?;
        let plan = plan.try_into_physical_plan(&task_ctx, self.physical_plan_codec.as_ref())?;
        let plan = self.rewrite_parquet_adapters(plan)?;
        let plan = self.rewrite_shuffle(ctx, key, &definition.inputs, &definition.output, plan)?;
        debug!(
            "{} execution plan\n{}",
            TaskKeyDisplay(key),
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let options = TracingExecOptions {
            metric_registry: global_metric_registry(),
            job_id: Some(key.job_id.into()),
            stage: Some(key.stage.into()),
            attempt: Some(key.attempt),
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        let stream = plan.execute(key.partition, task_ctx)?;
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
        key: &TaskKey,
        inputs: &[TaskInput],
        output: &TaskOutput,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let worker_id = self.options.worker_id;
        let handle = ctx.handle();
        let result = plan.transform(move |node| {
            if let Some(placeholder) = node.as_any().downcast_ref::<ShuffleReadExec>() {
                let partitioning = placeholder.properties().output_partitioning().clone();
                let mut locations = vec![vec![]; partitioning.partition_count()];
                // FIXME
                let accessor = WorkerStreamAccessor::new(worker_id, handle.clone());
                let shuffle = ShuffleReadExec::new(
                    locations,
                    Arc::new(accessor),
                    placeholder.schema(),
                    partitioning,
                );
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else {
                Ok(Transformed::no(node))
            }
        });
        let plan = result.data()?;
        let schema = plan.schema();
        let accessor = WorkerStreamAccessor::new(worker_id, handle.clone());
        let mut locations = vec![vec![]; plan.output_partitioning().partition_count()];
        match locations.get_mut(key.partition) {
            Some(x) => x.extend(output.locations(key)),
            None => {
                return Err(ExecutionError::InternalError(format!(
                    "invalid partition: {}",
                    TaskKeyDisplay(key)
                )));
            }
        };
        let partitioning = output.partitioning(
            &self.options.session.task_ctx(),
            &schema,
            self.physical_plan_codec.as_ref(),
        )?;
        let shuffle = ShuffleWriteExec::new(plan, locations, Arc::new(accessor), partitioning);
        Ok(Arc::new(shuffle))
    }
}
