// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use sail_common_datafusion::session::repartition::DEFAULT_REPARTITION_BUFFER_SIZE;

use crate::partition_pair_join::PartitionPairJoinExec;
use crate::repartition::ExplicitRepartitionExec;
use crate::replay::ReplayExec;

#[derive(Debug)]
struct CountingExec {
    input: Arc<dyn ExecutionPlan>,
    calls: Vec<AtomicUsize>,
}

impl CountingExec {
    fn new(partitions: usize) -> Result<Arc<Self>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batches = (0..partitions)
            .map(|p| {
                Ok(vec![RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![p as i64]))],
                )?])
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self {
            input: MemorySourceConfig::try_new_exec(&batches, schema, None)?,
            calls: (0..partitions).map(|_| AtomicUsize::new(0)).collect(),
        }))
    }
    fn assert_once(&self) {
        assert!(self.calls.iter().all(|c| c.load(Ordering::SeqCst) == 1));
    }
}
impl DisplayAs for CountingExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CountingExec")
    }
}
impl ExecutionPlan for CountingExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if self.calls[partition].fetch_add(1, Ordering::SeqCst) != 0 {
            return datafusion::common::exec_err!("changing source was evaluated twice");
        }
        let stream = self
            .input
            .execute(partition, context)?
            .then(|batch| async move {
                tokio::task::yield_now().await;
                batch
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[tokio::test]
async fn concurrent_pair_tasks_materialize_each_source_partition_once() -> Result<()> {
    let context = Arc::new(TaskContext::default());
    let left = CountingExec::new(3)?;
    let right = CountingExec::new(2)?;
    let left_replay = Arc::new(ReplayExec::try_new(left.clone())?);
    assert_eq!(left_replay.properties().emission_type, EmissionType::Final);
    let join = PartitionPairJoinExec::try_new(
        left_replay,
        Arc::new(ReplayExec::try_new(right.clone())?),
        None,
        Some(vec![1, 0]),
    )?;
    let streams = (0..6)
        .map(|p| join.execute(p, context.clone()))
        .collect::<Result<Vec<_>>>()?;
    let batches =
        futures::future::try_join_all(streams.into_iter().map(|s| s.try_collect::<Vec<_>>()))
            .await?;
    for (p, batches) in batches.iter().enumerate() {
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(Scalar::from_batch(&batches[0], 0)?, p as i64 % 2);
        assert_eq!(Scalar::from_batch(&batches[0], 1)?, p as i64 / 2);
    }
    left.assert_once();
    right.assert_once();
    assert!(context.runtime_env().disk_manager.used_disk_space() > 0);
    drop(join);
    assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
    assert_eq!(context.memory_pool().reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn pair_preparation_drains_bounded_repartition_outputs_concurrently() -> Result<()> {
    let context = Arc::new(TaskContext::default());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![0, 1]))])?;
    let batches = DEFAULT_REPARTITION_BUFFER_SIZE + 2;
    let source = MemorySourceConfig::try_new_exec(&[vec![batch; batches]], schema, None)?;
    let repartition = Arc::new(ExplicitRepartitionExec::new(
        source,
        Partitioning::RoundRobinBatch(2),
    ));
    let pair = PartitionPairJoinExec::try_new(
        Arc::new(ReplayExec::try_new(repartition)?),
        Arc::new(ReplayExec::try_new(CountingExec::new(1)?)?),
        None,
        None,
    )?;
    for partition in 0..2 {
        let stream = pair.execute(partition, context.clone())?;
        let result = tokio::time::timeout(Duration::from_secs(10), stream.try_collect::<Vec<_>>())
            .await
            .map_err(|_| {
                datafusion::common::exec_datafusion_err!(
                    "timed out reading partition pair {partition}"
                )
            })??;
        assert_eq!(
            result.iter().map(RecordBatch::num_rows).sum::<usize>(),
            batches
        );
    }
    drop(pair);
    assert_eq!(context.memory_pool().reserved(), 0);
    assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
    Ok(())
}

struct Scalar;
impl Scalar {
    fn from_batch(batch: &RecordBatch, column: usize) -> Result<i64> {
        match datafusion::common::ScalarValue::try_from_array(batch.column(column), 0)? {
            datafusion::common::ScalarValue::Int64(Some(value)) => Ok(value),
            _ => datafusion::common::exec_err!("expected int64"),
        }
    }
}
