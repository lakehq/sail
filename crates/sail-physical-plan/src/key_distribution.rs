// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    apply_expression_roots,
};
use datafusion_common::{Result, Statistics, plan_err};

use crate::repartition::{ExplicitRepartitionState, initialize_exchange, take_exchange_output};

/// Distributes the rows of its input among its output partitions by the values
/// of key expressions, so that the rows with the same values are in one partition.
///
/// This is the distribution that an operator above prefers and that the query
/// did not ask for. It is planned below an operator that works best when it
/// has all rows of a key in one partition, such as a writer that writes one file
/// for each key in each of its partitions. The physical optimizer removes it
/// when the query defines the partitions of its input, see `KeepExplicitPartitions`.
///
/// Each output partition buffers a limited number of batches of the batch size
/// of the session, and the input is read only as fast as the output partitions
/// are read. The rows that are held are therefore limited also when few output
/// partitions receive all rows, which is the case when the input has fewer keys
/// than there are partitions.
/// The output partitions must be read independently of each other.
#[derive(Debug)]
pub struct KeyDistributionExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    state: Arc<Mutex<ExplicitRepartitionState>>,
}

impl KeyDistributionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        keys: Vec<Arc<dyn PhysicalExpr>>,
        partitions: usize,
    ) -> Self {
        let mut eq_properties = input.equivalence_properties().clone();
        if input.output_partitioning().partition_count() > 1 {
            eq_properties.clear_orderings();
            eq_properties.clear_per_partition_constants();
        }
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                Partitioning::Hash(keys, partitions),
                input.pipeline_behavior(),
                input.boundedness(),
            )
            .with_scheduling_type(SchedulingType::Cooperative)
            .with_evaluation_type(EvaluationType::Eager),
        );
        Self {
            input,
            properties,
            state: Arc::new(Mutex::new(ExplicitRepartitionState::default())),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The expressions whose values determine the output partition of a row.
    pub fn keys(&self) -> &[Arc<dyn PhysicalExpr>] {
        match &self.properties.partitioning {
            Partitioning::Hash(keys, _) => keys,
            _ => &[],
        }
    }
}

impl DisplayAs for KeyDistributionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "{}: partitioning={}, input_partitions={}",
                Self::static_name(),
                self.properties.partitioning,
                self.input.output_partitioning().partition_count(),
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "partitioning_scheme={}", self.properties.partitioning)
            }
        }
    }
}

impl ExecutionPlan for KeyDistributionExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.keys(), f)
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
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(Self::new(
            child,
            self.keys().to_vec(),
            self.properties.partitioning.partition_count(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        initialize_exchange(
            &self.state,
            &self.input,
            &self.properties.partitioning,
            context,
        )?;
        let stream = take_exchange_output(&self.state, partition)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![if partition.is_none() {
            ChildStats::At(None)
        } else {
            ChildStats::Skip
        }]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        match (args.partition(), input_stats.first()) {
            (None, Some(statistics)) => Ok(Arc::clone(statistics)),
            _ => Ok(Arc::new(Statistics::new_unknown(&self.schema()))),
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
    use datafusion::execution::config::SessionConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use futures::{StreamExt, TryStreamExt};
    use sail_common_datafusion::session::repartition::DEFAULT_REPARTITION_BUFFER_SIZE;

    use super::*;

    const BATCHES: usize = 1000;
    const ROWS: usize = 100;

    /// An input partition that counts the batches that are read from it.
    #[derive(Debug)]
    struct CountedInput {
        schema: SchemaRef,
        keys: i64,
        read: Arc<AtomicUsize>,
    }

    impl PartitionStream for CountedInput {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let (schema, keys, read) =
                (Arc::clone(&self.schema), self.keys, Arc::clone(&self.read));
            let batches = futures::stream::iter(0..BATCHES).map(move |i| {
                read.fetch_add(1, Ordering::SeqCst);
                let rows = (i * ROWS) as i64..((i + 1) * ROWS) as i64;
                let k: ArrayRef =
                    Arc::new(Int64Array::from_iter_values(rows.clone().map(|x| x % keys)));
                let v: ArrayRef = Arc::new(Int64Array::from_iter_values(rows));
                Ok(RecordBatch::try_new(Arc::clone(&schema), vec![k, v])?)
            });
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                batches,
            ))
        }
    }

    /// A plan that distributes the rows of the given number of input partitions
    /// among eight output partitions by a column with the given number of values,
    /// and the number of batches that have been read from the input.
    fn distribution(inputs: usize, keys: i64) -> Result<(KeyDistributionExec, Arc<AtomicUsize>)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let read = Arc::new(AtomicUsize::new(0));
        let partitions = (0..inputs)
            .map(|_| {
                Arc::new(CountedInput {
                    schema: Arc::clone(&schema),
                    keys,
                    read: Arc::clone(&read),
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        let input = StreamingTableExec::try_new(schema, partitions, None, [], false, None)?;
        let keys: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("k", 0))];
        Ok((KeyDistributionExec::new(Arc::new(input), keys, 8), read))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_rows_of_a_key_are_in_one_partition() -> Result<()> {
        let (plan, _) = distribution(3, 50)?;
        let context = Arc::new(TaskContext::default());
        let streams = (0..8)
            .map(|i| plan.execute(i, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        let partitions = futures::future::try_join_all(
            streams
                .into_iter()
                .map(|stream| tokio::spawn(stream.try_collect::<Vec<_>>())),
        )
        .await
        .map_err(|e| datafusion_common::DataFusionError::ExecutionJoin(Box::new(e)))?;
        let mut seen: HashSet<i64> = HashSet::new();
        let mut rows = 0;
        let mut full = 0;
        for batches in partitions {
            let mut keys = HashSet::new();
            let batches = batches?;
            // The rows of a partition are collected into batches of the batch size,
            // although each batch of the input has few rows for each partition.
            let batch_size = context.session_config().batch_size();
            let sizes: Vec<_> = batches.iter().map(|x| x.num_rows()).collect();
            assert!(sizes.iter().rev().skip(1).all(|x| *x == batch_size));
            full += sizes.len().saturating_sub(1);
            for batch in batches {
                rows += batch.num_rows();
                keys.extend(batch.column(0).as_primitive::<Int64Type>().values().iter());
            }
            // No key is in two partitions.
            assert!(keys.iter().all(|key| seen.insert(*key)));
        }
        assert_eq!(seen.len(), 50);
        assert!(full > 0);
        assert_eq!(rows, 3 * BATCHES * ROWS);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_input_is_read_only_as_fast_as_the_partitions_are_read() -> Result<()> {
        // All rows have the same key, so one output partition receives all rows
        // and the other partitions stay empty.
        const INPUTS: usize = 3;
        let (plan, read) = distribution(INPUTS, 1)?;
        // The batches of the input have the batch size, so they are sent as they are.
        let config = SessionConfig::new().with_batch_size(ROWS);
        let context = Arc::new(TaskContext::default().with_session_config(config));
        let streams = (0..8)
            .map(|i| plan.execute(i, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        // The partitions are not read. Each input partition is read until
        // the buffer of the output partition is full and one more batch waits for it.
        let limit = DEFAULT_REPARTITION_BUFFER_SIZE + INPUTS;
        for _ in 0..6000 {
            if read.load(Ordering::SeqCst) >= limit {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(read.load(Ordering::SeqCst), limit);
        // The partitions are read independently of each other, as the tasks of
        // a writer read them. The input is read as the partition with the rows is read.
        let readers = streams.into_iter().map(|mut stream| {
            let read = Arc::clone(&read);
            tokio::spawn(async move {
                let mut batches = 0;
                while let Some(batch) = stream.next().await {
                    assert_eq!(batch?.num_rows(), ROWS);
                    batches += 1;
                    assert!(read.load(Ordering::SeqCst) <= batches + limit);
                }
                Ok::<_, datafusion_common::DataFusionError>(batches)
            })
        });
        let batches = futures::future::try_join_all(readers)
            .await
            .map_err(|e| datafusion_common::DataFusionError::ExecutionJoin(Box::new(e)))?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(batches.iter().sum::<usize>(), INPUTS * BATCHES);
        assert_eq!(batches.iter().filter(|x| **x > 0).count(), 1);
        assert_eq!(read.load(Ordering::SeqCst), INPUTS * BATCHES);
        Ok(())
    }
}
