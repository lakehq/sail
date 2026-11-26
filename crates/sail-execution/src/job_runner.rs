// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::common::spawn_buffered;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use futures::{StreamExt, TryStreamExt, stream};
use sail_common::actor::ActorSystem;
use sail_common_datafusion::session::job::{JobRunner, QueryCancellation};
use sail_physical_plan::range_repartition::{RangeRepartitionExec, resolve_range_partitions};
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverComponents, DriverHandle, DriverMessage, DriverOptions};
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;

fn explain_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    shuffle_backend: ShuffleBackendKind,
) -> Result<String> {
    JobGraph::try_new(plan, JobGraphOptions { shuffle_backend })
        .map(|graph| graph.to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub struct LocalJobRunner {
    session_id: String,
    next_job_id: AtomicU64,
    stopped: AtomicBool,
}

impl LocalJobRunner {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            next_job_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
        }
    }
}

impl Default for LocalJobRunner {
    fn default() -> Self {
        Self::new(String::new())
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(
            plan,
            ShuffleBackendKind::Flight {
                compression: crate::shuffle::ShuffleCompression::None,
                connection_count: std::num::NonZeroUsize::MIN,
                initial_stream_window_size: None,
                initial_connection_window_size: None,
            },
        )
    }

    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if self.stopped.load(Ordering::Relaxed) {
            return internal_err!("job runner is stopped");
        }
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let options = TracingExecOptions {
            metrics: global_metrics(),
            session_id: Some(self.session_id.clone()),
            job_id: Some(job_id),
            stage: None,
            attempt: None,
            operator_id: None,
        };
        let context = ctx.task_ctx();
        let has_range_repartition = plan.exists(|node| Ok(node.is::<RangeRepartitionExec>()))?;
        let input: SendableRecordBatchStream = if has_range_repartition {
            // Resolve partitions on first poll so dropping the stream also stops the sample.
            let schema = plan.schema();
            let stream = stream::once(async move {
                let plan = resolve_range_partitions(plan, Arc::clone(&context)).await?;
                execute_stream(trace_execution_plan(plan, options)?, context)
            })
            .try_flatten();
            Box::pin(RecordBatchStreamAdapter::new(schema, stream))
        } else {
            execute_stream(trace_execution_plan(plan, options)?, context)?
        };

        let schema = input.schema();
        // Start the producer lazily on the runtime that created the stream. The
        // consumer can observe shutdown while the producer finishes a batch.
        let runtime = Handle::try_current().ok();
        let output = stream::once(async move {
            let _guard = runtime.as_ref().map(Handle::enter);
            spawn_buffered(input, 1)
        })
        .flatten();
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, output));
        Ok(QueryCancellation::cancel_stream_on_shutdown(ctx, stream))
    }

    async fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

pub struct ClusterJobRunner {
    driver: DriverHandle,
    shuffle_backend: ShuffleBackendKind,
}

impl ClusterJobRunner {
    pub fn new(
        system: &mut ActorSystem,
        options: DriverOptions,
        components: DriverComponents,
    ) -> Self {
        let shuffle_backend = options.shuffle_backend.clone();
        let driver = DriverHandle::new(system.spawn::<DriverActor>((options, components)));
        Self {
            driver,
            shuffle_backend,
        }
    }

    pub fn driver(&self) -> DriverHandle {
        self.driver.clone()
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(plan, self.shuffle_backend.clone())
    }

    /// Executes a plan on the cluster. This is where the cool stuff happens.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = execute_job(&self.driver, plan, ctx.task_ctx()).await?;
        Ok(QueryCancellation::cancel_stream_on_shutdown(ctx, stream))
    }

    async fn stop(&self) {
        let _ = self.driver.shutdown_and_wait().await;
    }
}

async fn execute_job(
    driver: &DriverHandle,
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let (tx, rx) = oneshot::channel();
    driver
        .send(DriverMessage::ExecuteJob {
            plan,
            context,
            result: tx,
        })
        .await
        .map_err(|e| internal_datafusion_err!("{e}"))?;
    rx.await
        .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
        .map_err(|e| internal_datafusion_err!("{e}"))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use futures::StreamExt;
    use sail_physical_plan::range_repartition::RangeSample;
    use sail_physical_plan::range_sample::RangePartitionSampleExec;

    use super::*;

    /// A source whose streams never return, which counts the streams that are open.
    #[derive(Debug)]
    struct Pending {
        schema: SchemaRef,
        open: Arc<AtomicUsize>,
    }

    struct Open(Arc<AtomicUsize>);

    impl Drop for Open {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl PartitionStream for Pending {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            self.open.fetch_add(1, Ordering::SeqCst);
            let open = Open(Arc::clone(&self.open));
            let stream = futures::stream::pending::<Result<RecordBatch>>().map(move |batch| {
                let _ = &open;
                batch
            });
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            ))
        }
    }

    #[tokio::test]
    async fn range_sample_runs_in_the_returned_stream() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let open = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(Pending {
            schema: Arc::clone(&schema),
            open: Arc::clone(&open),
        });
        let input: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![source],
            None,
            vec![],
            false,
            None,
        )?);
        let sample = Arc::new(RangeSample::try_new(
            Arc::new(RangePartitionSampleExec::try_new(Arc::clone(&input), 10)?),
            20.0,
            vec![SortOptions::default()],
            2,
        )?);
        let ordering = [PhysicalSortExpr::new_default(col("k", &schema)?)].into();
        let plan = Arc::new(RangeRepartitionExec::try_new(input, sample, ordering)?);
        let runner = LocalJobRunner::default();
        // The sample never ends, so the stream is returned before the sample runs, and
        // the executor of the stream can stop the sample.
        let ctx = SessionContext::new();
        let execute = runner.execute(&ctx, plan);
        let mut stream = tokio::time::timeout(Duration::from_secs(5), execute)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))??;
        assert_eq!(open.load(Ordering::SeqCst), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err()
        );
        assert_eq!(open.load(Ordering::SeqCst), 1, "the sample reads the input");
        drop(stream);
        assert_eq!(open.load(Ordering::SeqCst), 0, "the sample stops");
        Ok(())
    }
}
