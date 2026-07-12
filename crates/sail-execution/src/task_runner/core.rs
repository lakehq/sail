use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::debug;
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{Actor, ActorContext};
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec, StageInputExec};
use crate::proto::{RemoteExecutionCodec, decode_remote_physical_plan};
use crate::stream_accessor::{StreamAccessor, StreamAccessorMessage};
use crate::task::definition::{
    TaskDefinition, TaskInput, TaskLaunchContext, TaskOutput, TaskResources,
};
use crate::task_runner::monitor::TaskMonitor;
use crate::task_runner::{TaskRunner, TaskRunnerMessage};

impl TaskRunner {
    pub fn new() -> Self {
        Self {
            signals: HashMap::new(),
        }
    }

    pub fn run_task<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: TaskKey,
        definition: TaskDefinition,
        launch_context: TaskLaunchContext,
        context: Arc<TaskContext>,
    ) where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let (stream, resource_memory_reservations) =
            match self.execute_plan(ctx, &key, definition, launch_context, context) {
                Ok(x) => x,
                Err(e) => {
                    let event = T::Message::report_task_status(
                        key,
                        TaskStatus::Failed,
                        Some(format!("failed to execute plan: {e}")),
                        Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
                    );
                    ctx.send(event);
                    return;
                }
            };
        let handle = ctx.handle().clone();
        let (tx, rx) = oneshot::channel();
        self.signals.insert(key.clone(), tx);
        let monitor = TaskMonitor::new(handle, key, stream, rx);
        ctx.spawn(async move {
            monitor.run().await;
            drop(resource_memory_reservations);
        });
    }

    pub fn stop_task(&mut self, key: &TaskKey) {
        if let Some(signal) = self.signals.remove(key) {
            let _ = signal.send(());
        }
    }

    pub fn stop_job(&mut self, job_id: JobId, stage: Option<usize>) {
        let keys = self
            .signals
            .keys()
            .filter(|key| key.job_id == job_id && stage.is_none_or(|stage| key.stage == stage))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            self.stop_task(&key);
        }
    }

    pub fn stop_all(&mut self) {
        for (_, signal) in self.signals.drain() {
            let _ = signal.send(());
        }
    }

    /// Deserializes and prepares a physical plan for execution on this node.
    fn execute_plan<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskKey,
        definition: TaskDefinition,
        launch_context: TaskLaunchContext,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<(
        SendableRecordBatchStream,
        Vec<Arc<tokio::sync::OwnedSemaphorePermit>>,
    )>
    where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let TaskDefinition {
            plan,
            inputs,
            output,
        } = definition;
        let TaskLaunchContext {
            resources:
                TaskResources {
                    python_artifacts,
                    local_relation_resources,
                },
            resource_memory_reservations,
        } = launch_context;
        let codec = RemoteExecutionCodec::for_task(python_artifacts, local_relation_resources);
        let plan = decode_remote_physical_plan(&context, &codec, plan.as_ref())?;
        let plan = self.rewrite_file_scans(plan)?;
        let plan = self.rewrite_shuffle(ctx, key, &inputs, &output, plan, &context, &codec)?;
        debug!(
            "{} execution plan\n{}",
            TaskKeyDisplay(key),
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let options = TracingExecOptions {
            metrics: global_metrics(),
            job_id: Some(key.job_id.into()),
            stage: Some(key.stage),
            attempt: Some(key.attempt),
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        let stream = plan.execute(key.partition, context)?;
        Ok((stream, resource_memory_reservations))
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
                if ds.downcast_to_file_source::<ParquetSource>().is_some() {
                    let adapter_factory: Arc<dyn PhysicalExprAdapterFactory> =
                        Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {});
                    builder = builder.with_expr_adapter(Some(adapter_factory));
                }
                let new_exec = DataSourceExec::from_data_source(builder.build());
                return Ok(Transformed::yes(new_exec as Arc<dyn ExecutionPlan>));
            }
            Ok(Transformed::no(node))
        });
        Ok(result.data()?)
    }

    fn rewrite_shuffle<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskKey,
        inputs: &[TaskInput],
        output: &TaskOutput,
        plan: Arc<dyn ExecutionPlan>,
        context: &TaskContext,
        codec: &dyn PhysicalExtensionCodec,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>>
    where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let handle = ctx.handle();
        let result = plan.transform(move |node| {
            if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
                let Some(input) = inputs.get(*placeholder.input()) else {
                    return internal_err!(
                        "stage input index {} out of bounds for {}",
                        placeholder.input(),
                        TaskKeyDisplay(key)
                    );
                };
                let locations = input.locations(key.job_id);
                let accessor = StreamAccessor::new(handle.clone());
                let shuffle = ShuffleReadExec::new(
                    locations,
                    Arc::new(accessor),
                    placeholder.properties().clone(),
                );
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else {
                Ok(Transformed::no(node))
            }
        });
        let plan = result.data()?;
        let schema = plan.schema();
        let accessor = StreamAccessor::new(handle.clone());
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
        let partitioning = output.partitioning(context, &schema, codec)?;
        let row_based = output.row_based();
        let shuffle =
            ShuffleWriteExec::new(plan, locations, Arc::new(accessor), partitioning, row_based);
        Ok(Arc::new(shuffle))
    }
}
