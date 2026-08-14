use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, internal_err};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use log::{debug, error, warn};
use sail_common::actor::{ActorAction, ActorContext};
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use sail_python_udf::error::PyErrExtractor;
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::sync::oneshot;

use crate::driver::{DriverMessage, TaskStatus};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey, WorkerId};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec, StageInputExec};
use crate::proto::decode_remote_physical_plan;
use crate::stream::accessor::TaskStreamFactory;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamSink};
use crate::task::definition::{TaskDefinition, TaskInput, TaskOutput};
use crate::task_runner::monitor::TaskMonitor;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage, TaskRunnerPlacement};
use crate::worker::{WorkerLocation, WorkerMessage};

impl TaskRunnerActor {
    pub(super) fn handle_run_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        definition: TaskDefinition,
        context: Arc<TaskContext>,
        peers: Vec<WorkerLocation>,
    ) -> ActorAction {
        if !peers.is_empty()
            && let TaskRunnerPlacement::Worker {
                worker_id,
                driver,
                peers: tracker,
                ..
            } = &mut self.placement
        {
            let peer_worker_ids = peers.iter().map(|peer| peer.worker_id).collect::<Vec<_>>();
            tracker.track(peers);
            let client = driver.core.clone();
            let worker_id = *worker_id;
            ctx.spawn(async move {
                if let Err(error) = client
                    .report_worker_known_peers(worker_id, peer_worker_ids)
                    .await
                {
                    warn!("failed to report worker known peers: {error}");
                }
            });
        }
        let stream = match self.execute_plan(ctx, &key, definition, context) {
            Ok(stream) => stream,
            Err(error) => {
                ctx.send(TaskRunnerMessage::ReportTaskStatus {
                    key,
                    status: TaskStatus::Failed,
                    message: Some(format!("failed to execute plan: {error}")),
                    cause: Some(CommonErrorCause::new::<PyErrExtractor>(&error)),
                });
                return ActorAction::Continue;
            }
        };
        let (tx, rx) = oneshot::channel();
        self.signals.insert(key.clone(), tx);
        ctx.spawn(TaskMonitor::new(ctx.handle().clone(), key, stream, rx).run());
        ActorAction::Continue
    }

    pub(super) fn handle_stop_task(&mut self, key: TaskKey) -> ActorAction {
        if let Some(signal) = self.signals.remove(&key) {
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
        match &mut self.placement {
            TaskRunnerPlacement::Driver { driver } => {
                let driver = driver.clone();
                ctx.spawn(async move {
                    let _ = driver
                        .send(DriverMessage::UpdateTask {
                            key,
                            status,
                            message,
                            cause,
                            sequence: None,
                        })
                        .await;
                });
            }
            TaskRunnerPlacement::Worker {
                sequence,
                driver,
                worker,
                retry_strategy,
                ..
            } => {
                let seq = *sequence;
                *sequence = match seq.checked_add(1) {
                    Some(s) => s,
                    None => {
                        error!("sequence number overflow");
                        return ActorAction::Stop;
                    }
                };
                let driver = driver.core.clone();
                let worker = worker.clone();
                let retry_strategy = retry_strategy.clone();
                ctx.spawn(async move {
                    let output = retry_strategy
                        .run(|| {
                            let driver = driver.clone();
                            let key = key.clone();
                            let message = message.clone();
                            let cause = cause.clone();
                            async move {
                                driver
                                    .report_task_status(key, status, message, cause, seq)
                                    .await
                            }
                        })
                        .await;
                    if let Err(error) = output {
                        error!("failed to report task status with retries: {error}");
                        let _ = worker.send(WorkerMessage::Shutdown).await;
                    }
                });
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_local_stream(&mut self, key: TaskStreamKey) -> ActorAction {
        self.extensions.local_streams.fail_stream_if_pending(&key);
        ActorAction::Continue
    }

    pub(super) fn handle_create_local_stream(
        &mut self,
        key: TaskStreamKey,
        replicas: usize,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> ActorAction {
        let _ = result.send(
            self.extensions
                .local_streams
                .create_stream(key, replicas, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_create_storage_stream(
        &mut self,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> ActorAction {
        let output = self
            .extensions
            .storage_streams()
            .and_then(|streams| streams.create_stream(key, schema, &context));
        let _ = result.send(output);
        ActorAction::Continue
    }

    pub(super) fn handle_create_celeborn_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        mappers: usize,
        channels: usize,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        if let Some(streams) = self.extensions.celeborn_streams.clone() {
            ctx.spawn(async move {
                let output = streams
                    .create_stream(key, mappers, channels, schema)
                    .await
                    .map_err(ExecutionError::from);
                let _ = result.send(output);
            });
            return ActorAction::Continue;
        }
        let _ = result.send(Err(ExecutionError::InternalError(
            "Celeborn stream requested without a Celeborn shuffle backend".to_string(),
        )));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_driver_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        match &self.placement {
            TaskRunnerPlacement::Driver { .. } => {
                let _ = result.send(self.extensions.local_streams.fetch_stream(ctx, &key));
            }
            TaskRunnerPlacement::Worker { driver, .. } => {
                let client = driver.flight.clone();
                ctx.spawn(async move {
                    let _ = result.send(client.fetch_task_stream(key, schema).await);
                });
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        match &mut self.placement {
            TaskRunnerPlacement::Driver { driver } => {
                let driver = driver.clone();
                ctx.spawn(async move {
                    let _ = driver
                        .send(DriverMessage::FetchWorkerStream {
                            worker_id,
                            key,
                            schema,
                            result,
                        })
                        .await;
                });
            }
            TaskRunnerPlacement::Worker {
                worker_id: this_worker_id,
                ..
            } if worker_id == *this_worker_id => {
                let _ = result.send(self.extensions.local_streams.fetch_stream(ctx, &key));
            }
            TaskRunnerPlacement::Worker { peers, .. } => match peers.get_client_set(worker_id) {
                Ok(client) => {
                    ctx.spawn(async move {
                        let _ = result.send(client.flight.fetch_task_stream(key, schema).await);
                    });
                }
                Err(error) => {
                    let _ = result.send(Err(error));
                    return ActorAction::Continue;
                }
            },
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_local_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(self.extensions.local_streams.fetch_stream(ctx, &key));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_storage_stream(
        &mut self,
        key: TaskStreamKey,
        schema: Arc<Schema>,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let output = self
            .extensions
            .storage_streams()
            .and_then(|streams| streams.fetch_stream(key, schema, &context));
        let _ = result.send(output);
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_celeborn_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: usize,
        channels: Vec<usize>,
        schema: Arc<Schema>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let streams = self.extensions.celeborn_streams.clone();
        ctx.spawn(async move {
            let output = match streams {
                Some(streams) => streams.fetch_stream(job_id, stage, channels, schema).await,
                None => Err(DataFusionError::Internal(
                    "Celeborn stream requested without a Celeborn shuffle backend".to_string(),
                )),
            };
            let _ = result.send(output.map_err(ExecutionError::from));
        });
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_local_streams(
        &mut self,
        job_id: JobId,
        stage: Option<usize>,
    ) -> ActorAction {
        self.extensions.local_streams.remove_streams(job_id, stage);
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_storage_streams(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: Option<usize>,
        context: Arc<TaskContext>,
    ) -> ActorAction {
        if let Some(streams) = self.extensions.storage_streams.clone() {
            ctx.spawn(async move {
                if let Err(error) = streams.remove_streams(job_id, stage, &context).await {
                    warn!("failed to remove storage shuffle data for job {job_id}: {error}");
                }
            });
        }
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_celeborn_streams(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: Option<usize>,
    ) -> ActorAction {
        if let Some(streams) = self.extensions.celeborn_streams.clone() {
            let unregister = matches!(self.placement, TaskRunnerPlacement::Driver { .. });
            ctx.spawn(async move {
                if let Err(error) = streams.remove_streams(job_id, stage, unregister).await {
                    warn!("failed to remove Celeborn shuffle data for job {job_id}: {error}");
                }
            });
        }
        ActorAction::Continue
    }

    pub(super) fn handle_shutdown(&mut self) -> ActorAction {
        ActorAction::Stop
    }

    fn execute_plan(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: &TaskKey,
        definition: TaskDefinition,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let plan =
            decode_remote_physical_plan(&context, self.codec.as_ref(), definition.plan.as_ref())?;
        let plan = self.rewrite_file_scans(plan)?;
        let plan = self.rewrite_shuffle(
            ctx,
            key,
            &definition.inputs,
            &definition.output,
            plan,
            context.clone(),
        )?;
        debug!(
            "{} execution plan\n{}",
            TaskKeyDisplay(key),
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let plan = trace_execution_plan(
            plan,
            TracingExecOptions {
                metrics: global_metrics(),
                job_id: Some(key.job_id.into()),
                stage: Some(key.stage),
                attempt: Some(key.attempt),
                operator_id: None,
            },
        )?;
        Ok(plan.execute(key.partition, context)?)
    }

    fn rewrite_file_scans(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform(|node| {
            if let Some(ds) = node.downcast_ref::<DataSourceExec>()
                && let Some(base_config) = ds.data_source().downcast_ref::<FileScanConfig>()
            {
                // DataFusion file scans can use process-local sibling state to let
                // partitions steal work from a shared queue of all file groups. In Sail
                // cluster mode each partition runs as an isolated task with its own
                // deserialized plan, so that queue would be recreated in every task and
                // every task would scan every file. Preserve-order disables sibling
                // work sharing and keeps each task on its own file group.
                let mut builder =
                    FileScanConfigBuilder::from(base_config.clone()).with_preserve_order(true);
                if ds.downcast_to_file_source::<ParquetSource>().is_some()
                    && base_config.expr_adapter_factory.is_none()
                {
                    let adapter_factory: Arc<dyn PhysicalExprAdapterFactory> =
                        Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {});
                    builder = builder.with_expr_adapter(Some(adapter_factory));
                }
                return Ok(Transformed::yes(
                    DataSourceExec::from_data_source(builder.build()) as Arc<dyn ExecutionPlan>,
                ));
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
        context: Arc<TaskContext>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let mappers = plan.output_partitioning().partition_count();
        let streams = TaskStreamFactory::new(
            ctx.handle().clone(),
            context.clone(),
            &self.extensions,
            mappers,
        );
        let result = {
            let streams = streams.clone();
            plan.transform(move |node| {
                if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
                    let Some(input) = inputs.get(*placeholder.input()) else {
                        return internal_err!(
                            "stage input index {} out of bounds for {}",
                            placeholder.input(),
                            TaskKeyDisplay(key)
                        );
                    };
                    return Ok(Transformed::yes(Arc::new(ShuffleReadExec::new(
                        streams.reader(key.clone(), input.clone(), placeholder.schema()),
                        placeholder.properties().clone(),
                    ))));
                }
                Ok(Transformed::no(node))
            })
        };
        let plan = result.data()?;
        let schema = plan.schema();
        let partitioning = output.shuffle_partitioning(&context, &schema, self.codec.as_ref())?;
        let writer = streams.writer(key.clone(), output.clone(), schema.clone());
        Ok(Arc::new(ShuffleWriteExec::new(plan, writer, partitioning)))
    }
}
