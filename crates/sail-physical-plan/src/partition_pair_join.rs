use std::sync::{Arc, OnceLock};

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, JoinType, Result, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, TryStreamExt, stream};
use sail_common_datafusion::replay::{check_bounded, replay_error};

use crate::replay::ReplayExec;

type PreparedInputs = Shared<BoxFuture<'static, Result<(), Arc<DataFusionError>>>>;

/// One inner nested-loop join per pair of producer partitions. Children must be
/// replayable: ReplayExec locally, committed stage inputs on workers.
#[derive(Debug)]
pub struct PartitionPairJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    filter: Option<JoinFilter>,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
    prepared: OnceLock<PreparedInputs>,
}

impl PartitionPairJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: Option<JoinFilter>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        check_bounded(left.as_ref())?;
        check_bounded(right.as_ref())?;
        let count = pair_count(
            left.output_partitioning().partition_count(),
            right.output_partitioning().partition_count(),
        )?;
        let join = NestedLoopJoinExec::try_new(
            left.clone(),
            right.clone(),
            filter.clone(),
            &JoinType::Inner,
            projection.clone(),
        )?;
        let mut properties = join
            .properties()
            .as_ref()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(count));
        properties.eq_properties.clear_orderings();
        properties.eq_properties.clear_per_partition_constants();
        Ok(Self {
            left,
            right,
            filter,
            projection,
            properties: Arc::new(properties),
            prepared: OnceLock::new(),
        })
    }

    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }
    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }
}

fn pair_count(left: usize, right: usize) -> Result<usize> {
    if left == 0 || right == 0 {
        return plan_err!("partition-pair join requires nonzero input partition counts");
    }
    left.checked_mul(right).ok_or_else(|| {
        datafusion::common::plan_datafusion_err!(
            "partition-pair join partition count overflow: {left} * {right}"
        )
    })
}

fn pair_indices(partition: usize, left: usize, right: usize) -> Result<(usize, usize)> {
    let count = pair_count(left, right)?;
    if partition >= count {
        return exec_err!("partition-pair join index {partition} exceeds {count} partitions");
    }
    Ok((partition / right, partition % right))
}

impl DisplayAs for PartitionPairJoinExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PartitionPairJoinExec: partitions={}x{}",
            self.left.output_partitioning().partition_count(),
            self.right.output_partitioning().partition_count()
        )?;
        if let Some(filter) = &self.filter {
            write!(f, ", filter={}", filter.expression())?;
        }
        if let Some(projection) = &self.projection {
            write!(f, ", projection={projection:?}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for PartitionPairJoinExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        match &self.filter {
            Some(filter) => f(filter.expression()),
            None => Ok(TreeNodeRecursion::Continue),
        }
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [left, right]: [Arc<dyn ExecutionPlan>; 2] = children.try_into().map_err(|_| {
            datafusion::common::plan_datafusion_err!("PartitionPairJoinExec expects two children")
        })?;
        Ok(Arc::new(Self::try_new(
            left,
            right,
            self.filter.clone(),
            self.projection.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (left, right) = pair_indices(
            partition,
            self.left.output_partitioning().partition_count(),
            self.right.output_partitioning().partition_count(),
        )?;
        let prepared = self
            .prepared
            .get_or_init(|| {
                let inputs = [self.left.clone(), self.right.clone()];
                let context = context.clone();
                async move {
                    // Complete local spooling before any join consumes the memory pool.
                    // Workers already have this barrier at their materialized input stages.
                    for input in inputs {
                        if let Some(replay) = input.downcast_ref::<ReplayExec>() {
                            for partition in 0..input.output_partitioning().partition_count() {
                                replay
                                    .materialize(partition, context.clone())
                                    .await
                                    .map_err(Arc::new)?;
                            }
                        }
                    }
                    Ok(())
                }
                .boxed()
                .shared()
            })
            .clone();
        let left = Arc::new(PartitionExec::new(self.left.clone(), left));
        let right = Arc::new(PartitionExec::new(self.right.clone(), right));
        let filter = self.filter.clone();
        let projection = self.projection.clone();
        let stream = stream::once(async move {
            prepared.await.map_err(replay_error)?;
            // A fresh NLJ owns its OnceAsync/build bitmap and spill state for this pair only.
            NestedLoopJoinExec::try_new(left, right, filter, &JoinType::Inner, projection)?
                .execute(0, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[derive(Debug)]
struct PartitionExec {
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    properties: Arc<PlanProperties>,
}

impl PartitionExec {
    fn new(input: Arc<dyn ExecutionPlan>, partition: usize) -> Self {
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_partitioning(Partitioning::UnknownPartitioning(1)),
        );
        Self {
            input,
            partition,
            properties,
        }
    }
}

impl DisplayAs for PartitionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PartitionExec: partition={}", self.partition)
    }
}

impl ExecutionPlan for PartitionExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(input), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("PartitionExec expects one child");
        };
        Ok(Arc::new(Self::new(input, self.partition)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("single partition adapter received partition {partition}");
        }
        self.input.execute(self.partition, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inner_pair_spills_its_build_partition() -> Result<()> {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::SessionConfig;
        use futures::TryStreamExt;

        use crate::replay::ReplayExec;

        let context = Arc::new(
            TaskContext::default()
                .with_runtime(Arc::new(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024)))
                        .build()?,
                ))
                .with_session_config(SessionConfig::new().with_batch_size(512)),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..512)),
                Arc::new(StringArray::from(vec!["x".repeat(512); 512])),
            ],
        )?;
        let left =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone(); 80]], schema.clone(), None)?;
        let right = MemorySourceConfig::try_new_exec(&[vec![batch.slice(0, 1)]], schema, None)?;
        let pair = PartitionPairJoinExec::try_new(
            Arc::new(ReplayExec::try_new(left)?),
            Arc::new(ReplayExec::try_new(right)?),
            None,
            Some(vec![1]),
        )?;
        let mut stream = pair.execute(0, context.clone())?;
        let mut rows = 0;
        while let Some(batch) = stream.try_next().await? {
            rows += batch.num_rows();
            let payload = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::common::exec_datafusion_err!("expected payload"))?;
            assert!(payload.iter().flatten().all(|s| s.len() == 512));
        }
        assert_eq!(rows, 512 * 80);
        drop(stream);
        drop(pair);
        assert_eq!(context.memory_pool().reserved(), 0);
        assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
        Ok(())
    }

    #[test]
    fn partition_mapping_and_overflow() -> Result<()> {
        assert_eq!(pair_count(3, 2)?, 6);
        assert_eq!(
            (0..6)
                .map(|p| pair_indices(p, 3, 2))
                .collect::<Result<Vec<_>>>()?,
            vec![(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)]
        );
        assert!(pair_indices(6, 3, 2).is_err());
        assert!(pair_count(usize::MAX, 2).is_err());
        assert!(pair_count(1, 0).is_err());
        Ok(())
    }
}
