use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce::LimitedBatchCoalescer;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryStreamExt};

use crate::stream::reader::TaskStreamReader;

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
    preserve_batches: bool,
}

impl ShuffleReadExec {
    pub fn new(
        reader: Arc<dyn TaskStreamReader>,
        properties: Arc<PlanProperties>,
        preserve_batches: bool,
    ) -> Self {
        Self {
            properties,
            reader,
            preserve_batches,
        }
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReadExec: partitioning={}",
            self.properties.output_partitioning(),
        )
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("ShuffleReadExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let reader = self.reader.clone();
        let output = futures::stream::once(async move {
            let source = reader.open(partition).await?;
            Ok::<_, datafusion::error::DataFusionError>(source.map(|item| {
                item.map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))
            }))
        })
        .try_flatten();
        let output: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(self.schema(), output));
        // Shared replay already caps oversized batches in Tee. Preserve smaller batches
        // so each reader does not enlarge its downstream expression working set or read
        // past a valid prefix before returning it. Unbounded inputs must emit promptly.
        if self.preserve_batches || self.properties.boundedness.is_unbounded() {
            return Ok(output);
        }
        // The reader has merged the producers for this partition. Share one
        // coalescer across them instead of buffering each producer separately.
        let coalescer =
            LimitedBatchCoalescer::new(self.schema(), context.session_config().batch_size(), None);
        let output = futures::stream::try_unfold(
            (output, coalescer, false),
            |(mut input, mut coalescer, mut done)| async move {
                loop {
                    if let Some(batch) = coalescer.next_completed_batch() {
                        return Ok::<_, datafusion::error::DataFusionError>(Some((
                            batch,
                            (input, coalescer, done),
                        )));
                    }
                    if done {
                        return Ok(None);
                    }
                    match input.try_next().await? {
                        Some(batch) => {
                            coalescer.push_batch(batch)?;
                        }
                        None => {
                            coalescer.finish()?;
                            done = true;
                        }
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatchOptions;
    use datafusion::execution::config::SessionConfig;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::limit::GlobalLimitExec;

    use super::*;
    use crate::stream::error::{TaskStreamError, TaskStreamResult};
    use crate::stream::reader::TaskStreamSource;

    #[derive(Debug)]
    struct Reader {
        items: Vec<TaskStreamResult<RecordBatch>>,
        reads: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl TaskStreamReader for Reader {
        async fn open(&self, _partition: usize) -> Result<TaskStreamSource> {
            let reads = self.reads.clone();
            Ok(Box::pin(futures::stream::iter(self.items.clone()).inspect(
                move |_| {
                    reads.fetch_add(1, Ordering::SeqCst);
                },
            )))
        }
    }

    fn batch(rows: usize, zero_columns: bool) -> RecordBatch {
        if zero_columns {
            RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap()
        } else {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))],
            )
            .unwrap()
        }
    }

    fn plan(
        items: Vec<TaskStreamResult<RecordBatch>>,
        preserve_batches: bool,
        boundedness: Boundedness,
    ) -> (Arc<ShuffleReadExec>, Arc<AtomicUsize>) {
        let schema = items[0].as_ref().unwrap().schema();
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(Reader {
            items,
            reads: reads.clone(),
        });
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            boundedness,
        ));
        (
            Arc::new(ShuffleReadExec::new(reader, properties, preserve_batches)),
            reads,
        )
    }

    fn context() -> Arc<TaskContext> {
        Arc::new(
            TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(8192)),
        )
    }

    #[tokio::test]
    async fn shared_reads_preserve_batches_and_ordinary_reads_coalesce() -> Result<()> {
        for zero_columns in [false, true] {
            let batches = vec![batch(1024, zero_columns), batch(976, zero_columns)];
            for (preserve, boundedness, sizes) in [
                (false, Boundedness::Bounded, vec![2000]),
                (true, Boundedness::Bounded, vec![1024, 976]),
                (
                    false,
                    Boundedness::Unbounded {
                        requires_infinite_memory: false,
                    },
                    vec![1024, 976],
                ),
            ] {
                let (input, _) = plan(
                    batches.iter().cloned().map(Ok).collect(),
                    preserve,
                    boundedness,
                );
                let output = collect(input, context()).await?;
                assert_eq!(
                    output.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                    sizes
                );
                if preserve && !zero_columns {
                    for (actual, original) in output.iter().zip(&batches) {
                        assert!(Arc::ptr_eq(actual.column(0), original.column(0)));
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn shared_read_limit_stops_before_a_later_error() -> Result<()> {
        let (input, reads) = plan(
            vec![
                Ok(batch(1024, false)),
                Err(TaskStreamError::Unknown("later error".into())),
            ],
            true,
            Boundedness::Bounded,
        );
        let limited = Arc::new(GlobalLimitExec::new(input.clone(), 0, Some(1)));
        let output = collect(limited, context()).await?;
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        // A reader that drains the same source still receives the terminal error.
        let mut full = input.execute(0, context())?;
        assert_eq!(full.try_next().await?.unwrap().num_rows(), 1024);
        assert!(
            full.try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("later error")
        );
        Ok(())
    }
}
