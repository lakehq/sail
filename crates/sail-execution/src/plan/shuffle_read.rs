use std::fmt::Formatter;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce::LimitedBatchCoalescer;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryStreamExt};

use crate::id::{TaskKey, TaskKeyDisplay};
use crate::stream::reader::TaskStreamReader;

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
    key: TaskKey,
    input_stage: usize,
}

impl ShuffleReadExec {
    pub fn new(
        reader: Arc<dyn TaskStreamReader>,
        properties: Arc<PlanProperties>,
        key: TaskKey,
        input_stage: usize,
    ) -> Self {
        Self {
            properties,
            reader,
            key,
            input_stage,
        }
    }

    fn profile_read(&self, stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let key = self.key.clone();
        let input_stage = self.input_stage;
        let profile = ShuffleReadProfile {
            stream,
            poll_wait: Duration::ZERO,
            batches: 0,
            rows: 0,
            array_bytes: 0,
        };
        let output = futures::stream::unfold(profile, move |mut profile| {
            let key = key.clone();
            async move {
                let started = Instant::now();
                let next = profile.stream.next().await;
                profile.poll_wait += started.elapsed();
                match next {
                    Some(Ok(batch)) => {
                        profile.batches += 1;
                        profile.rows += batch.num_rows() as u64;
                        profile.array_bytes += batch.get_array_memory_size() as u64;
                        Some((Ok(batch), profile))
                    }
                    Some(Err(error)) => {
                        log::info!(
                            "{} shuffle read input_stage={input_stage} batches={} rows={} array_bytes={} poll_wait={:?} outcome=error",
                            TaskKeyDisplay(&key),
                            profile.batches,
                            profile.rows,
                            profile.array_bytes,
                            profile.poll_wait
                        );
                        Some((Err(error), profile))
                    }
                    None => {
                        log::info!(
                            "{} shuffle read input_stage={input_stage} batches={} rows={} array_bytes={} poll_wait={:?} outcome=complete",
                            TaskKeyDisplay(&key),
                            profile.batches,
                            profile.rows,
                            profile.array_bytes,
                            profile.poll_wait
                        );
                        None
                    }
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, output))
    }
}

struct ShuffleReadProfile {
    stream: SendableRecordBatchStream,
    poll_wait: Duration,
    batches: u64,
    rows: u64,
    array_bytes: u64,
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
        // Unbounded inputs must emit partial batches promptly.
        if self.properties.boundedness.is_unbounded() {
            return Ok(self.profile_read(output));
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
        let output = Box::pin(RecordBatchStreamAdapter::new(self.schema(), output));
        Ok(self.profile_read(output))
    }
}
