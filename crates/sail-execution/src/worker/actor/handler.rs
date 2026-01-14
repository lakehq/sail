use std::mem;

use datafusion::arrow::datatypes::SchemaRef;
use log::{error, info, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_delta_lake::physical_plan::DeltaPhysicalExprAdapterFactory;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::task::definition::TaskDefinition;
use crate::worker::actor::WorkerActor;
use crate::worker::event::{WorkerLocation, WorkerStreamOwner};
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
        self.task_runner
            .run_task(ctx, key, definition, self.options.session.task_ctx());
        ActorAction::Continue
    }

    pub(super) fn handle_stop_task(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskKey,
    ) -> ActorAction {
        self.task_runner.stop_task(&key);
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

    pub(super) fn handle_probe_pending_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
    ) -> ActorAction {
        self.stream_manager.fail_local_stream_if_pending(&key);
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

    pub(super) fn handle_fetch_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        owner: WorkerStreamOwner,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        match owner {
            WorkerStreamOwner::This => {
                let _ = result.send(self.stream_manager.fetch_local_stream(ctx, &key));
            }
            WorkerStreamOwner::Worker {
                worker_id,
                schema: _,
            } if worker_id == self.options.worker_id => {
                let _ = result.send(self.stream_manager.fetch_local_stream(ctx, &key));
            }
            WorkerStreamOwner::Worker { worker_id, schema } => {
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
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_remote_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(
            self.stream_manager
                .fetch_remote_stream(ctx, uri, &key, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_job(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: Option<usize>,
    ) -> ActorAction {
        self.stream_manager.remove_local_streams(job_id, stage);
        ActorAction::Continue
    }

    fn execute_plan(
        &mut self,
        ctx: &mut ActorContext<Self>,
        instance: &TaskInstance,
        plan: Vec<u8>,
        partition: usize,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let task_ctx = self.options.session.task_ctx();
        let plan = PhysicalPlanNode::decode(plan.as_slice())?;
        let plan = plan.try_into_physical_plan(&task_ctx, self.physical_plan_codec.as_ref())?;
        let plan = self.rewrite_shuffle(ctx, plan)?;
        let plan = self.rewrite_parquet_adapters(plan)?;
        debug!(
            "job {} task {} attempt {} execution plan\n{}",
            instance.job_id,
            instance.task_id,
            instance.attempt,
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let options = TracingExecOptions {
            metric_registry: global_metric_registry(),
            job_id: Some(instance.job_id.into()),
            task_id: Some(instance.task_id.into()),
            task_attempt: Some(instance.attempt),
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        let stream = plan.execute(partition, task_ctx)?;
        Ok(stream)
    }

    fn rewrite_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let worker_id = self.options.worker_id;
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

    fn rewrite_parquet_adapters(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform(|node| {
            if let Some(ds) = node.as_any().downcast_ref::<DataSourceExec>() {
                if let Some((base_config, _parquet)) = ds.downcast_to_file_source::<ParquetSource>()
                {
                    // Try to determine if this is Delta or Iceberg based on table metadata
                    // For now, we default to Delta adapter. In the future, we could add
                    // metadata to distinguish between different table formats.
                    let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});

                    let builder = FileScanConfigBuilder::from(base_config.clone())
                        .with_expr_adapter(Some(adapter_factory));
                    let new_exec = DataSourceExec::from_data_source(builder.build());
                    return Ok(Transformed::yes(new_exec));
                }
            }
            Ok(Transformed::no(node))
        });
        Ok(result.data()?)
    }
}
