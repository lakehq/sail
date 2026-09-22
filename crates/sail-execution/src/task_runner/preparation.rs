use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, internal_err};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::TryStreamExt;
use log::debug;
use sail_common::actor::ActorHandle;
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio_util::sync::CancellationToken;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec, StageInputExec};
use crate::proto::{RemoteExecutionCodec, proto_to_physical_plan};
use crate::stream::accessor::TaskStreamFactory;
use crate::task::definition::{TaskDefinition, TaskInput, TaskOutput};
use crate::task_runner::TaskRunnerActor;

pub(super) struct TaskPreparation {
    pub session_id: String,
    pub handle: ActorHandle<TaskRunnerActor>,
    pub celeborn: bool,
    pub enable_shuffle_read_coalescing: bool,
}

impl TaskPreparation {
    /// The returned schema belongs to ShuffleWriteExec's completion stream, not the stage's data.
    pub fn stream(
        self,
        key: TaskKey,
        definition: Arc<TaskDefinition>,
        proto: Arc<PhysicalPlanNode>,
        context: Arc<TaskContext>,
    ) -> SendableRecordBatchStream {
        preparation_stream(key.clone(), move |canceled| {
            self.execute_plan(&key, &definition, &proto, &canceled, context)
        })
    }

    fn execute_plan(
        &self,
        key: &TaskKey,
        definition: &TaskDefinition,
        proto: &PhysicalPlanNode,
        canceled: &CancellationToken,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let plan = proto_to_physical_plan(&context, &RemoteExecutionCodec, proto)?;
        let plan = self.rewrite_file_scans(plan)?;
        let plan = self.rewrite_shuffle(
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
                session_id: Some(self.session_id.clone()),
                job_id: Some(key.job_id.into()),
                stage: Some(key.stage),
                attempt: Some(key.attempt),
                operator_id: None,
            },
        )?;
        if canceled.is_cancelled() {
            return Err(ExecutionError::InternalError(
                "task canceled during preparation".into(),
            ));
        }
        Ok(plan.execute(key.partition, context)?)
    }

    fn rewrite_file_scans(
        &self,
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
        &self,
        key: &TaskKey,
        inputs: &[TaskInput],
        output: &TaskOutput,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let mappers = plan.output_partitioning().partition_count();
        let streams =
            TaskStreamFactory::new(self.handle.clone(), context.clone(), self.celeborn, mappers);
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
                        self.enable_shuffle_read_coalescing,
                    ))));
                }
                Ok(Transformed::no(node))
            })
        };
        let plan = result.data()?;
        let schema = plan.schema();
        let partitioning = output.shuffle_partitioning(&context, &schema, &RemoteExecutionCodec)?;
        let writer = streams.writer(key.clone(), output.clone(), schema.clone());
        Ok(Arc::new(ShuffleWriteExec::new(plan, writer, partitioning)))
    }
}

/// Dropping the pending stream cancels preparation cooperatively. The blocking task owns
/// its inputs and result, so Tokio drops an abandoned result even after the monitor exits.
fn preparation_stream(
    key: TaskKey,
    prepare: impl FnOnce(CancellationToken) -> ExecutionResult<SendableRecordBatchStream>
    + Send
    + 'static,
) -> SendableRecordBatchStream {
    let stream = futures::stream::once(async move {
        let canceled = CancellationToken::new();
        let _cancel_on_drop = canceled.clone().drop_guard();
        let queued = Instant::now();
        let span = fastrace::Span::enter_with_local_parent("TaskPreparation");
        tokio::task::spawn_blocking(move || {
            let _parent = span.set_local_parent();
            let started = Instant::now();
            if canceled.is_cancelled() {
                return Err(ExecutionError::InternalError(
                    "task canceled before preparation".into(),
                ));
            }
            let result = prepare(canceled);
            debug!(
                "{} preparation wait={:?} duration={:?}",
                TaskKeyDisplay(&key),
                started.duration_since(queued),
                started.elapsed()
            );
            result
        })
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .map_err(|error| DataFusionError::External(Box::new(error)))
    })
    .try_flatten();
    Box::pin(RecordBatchStreamAdapter::new(
        Arc::new(Schema::empty()),
        stream,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::StreamExt;
    use tokio::sync::oneshot;

    use super::*;
    use crate::id::JobId;

    fn key() -> TaskKey {
        TaskKey {
            job_id: JobId::from(1),
            stage: 0,
            partition: 0,
            attempt: 0,
        }
    }

    #[tokio::test]
    async fn dropping_an_unpolled_stream_does_not_prepare() {
        let started = Arc::new(AtomicBool::new(false));
        let flag = started.clone();
        let stream = preparation_stream(key(), move |_| {
            flag.store(true, Ordering::SeqCst);
            Err(ExecutionError::InternalError("should never start".into()))
        });
        assert!(stream.schema().fields().is_empty());
        drop(stream);
        assert!(!started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn preparation_errors_and_panics_are_stream_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = preparation_stream(key(), |_| {
            Err(ExecutionError::InvalidArgument("bad plan".into()))
        });
        assert!(
            stream
                .next()
                .await
                .ok_or("missing preparation error")?
                .is_err()
        );
        assert!(stream.next().await.is_none());
        let mut stream = preparation_stream(key(), |_| {
            std::panic::resume_unwind(Box::new("construction panic"))
        });
        assert!(stream.next().await.ok_or("missing panic error")?.is_err());
        Ok(())
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn cancellation_during_preparation_drops_the_abandoned_result()
    -> Result<(), Box<dyn std::error::Error>> {
        let (started, ready) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let (observed, canceled) = oneshot::channel();
        let (dropped, result_dropped) = oneshot::channel();
        let mut stream = preparation_stream(key(), move |token| {
            let _ = started.send(());
            blocked
                .recv()
                .map_err(|e| ExecutionError::InternalError(e.to_string()))?;
            let _ = observed.send(token.is_cancelled());
            let guard = DropSignal(Some(dropped));
            let inner = futures::stream::once(async move {
                let _guard = guard;
                futures::future::pending().await
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                inner,
            )))
        });
        let mut next = Box::pin(stream.next());
        assert!(futures::poll!(next.as_mut()).is_pending());
        ready.await?;
        drop(next);
        drop(stream);
        release.send(())?;
        assert!(canceled.await?);
        tokio::time::timeout(std::time::Duration::from_secs(5), result_dropped).await??;
        Ok(())
    }
}
