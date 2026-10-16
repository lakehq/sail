use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, RecordBatch, RecordBatchOptions, UInt64Array};
use datafusion::arrow::compute::{filter, filter_record_batch, take_record_batch};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{Result, exec_err, plan_err};
use futures::{StreamExt, TryStreamExt};
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::schema::{
    to_flow_event_schema, try_from_flow_event_schema,
};
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;

use super::accumulator::{AccumulatorFactory, StreamAccumulator, checked_count};
use super::state::{Row, RowMultiset, rows_to_batch};

/// Maintains a grouped dynamic table and emits -old/+new aggregate rows.
/// Each group holds retractable accumulators rather than its live input rows.
#[derive(Debug)]
pub struct StreamAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    aggregate: Arc<AggregateExec>,
    accumulators: Arc<Vec<AccumulatorFactory>>,
    properties: Arc<PlanProperties>,
}

impl StreamAggregateExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, aggregate: Arc<AggregateExec>) -> Result<Self> {
        let data_schema = try_from_flow_event_schema(&input.schema())?;
        if &data_schema != aggregate.input().schema().as_ref() {
            return plan_err!("stream aggregate input schema does not match its expressions");
        }
        if aggregate.mode() != &AggregateMode::Single || !aggregate.group_expr().is_single() {
            return plan_err!(
                "stream aggregate requires a single grouping set and single-stage expressions"
            );
        }
        let mut accumulators = Vec::new();
        for expr in aggregate.aggr_expr() {
            if !expr.order_bys().is_empty() {
                return plan_err!("ordered streaming aggregates are not supported");
            }
            accumulators.push(AccumulatorFactory::try_new(
                expr,
                aggregate.input().schema(),
            )?);
        }
        let boundedness = match input.boundedness() {
            Boundedness::Bounded => Boundedness::Bounded,
            _ => Boundedness::Unbounded {
                requires_infinite_memory: true,
            },
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(to_flow_event_schema(&aggregate.schema()))),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            boundedness,
        ));
        Ok(Self {
            input,
            aggregate,
            accumulators: Arc::new(accumulators),
            properties,
        })
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn aggregate(&self) -> &Arc<AggregateExec> {
        &self.aggregate
    }
}

impl DisplayAs for StreamAggregateExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "Stream")?;
        self.aggregate.fmt_as(t, f)
    }
}

impl ExecutionPlan for StreamAggregateExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.aggregate.apply_expressions(f)
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
            return plan_err!("StreamAggregateExec expects one input");
        };
        Ok(Arc::new(Self::try_new(input, self.aggregate.clone())?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 || self.input.output_partitioning().partition_count() != 1 {
            return exec_err!("StreamAggregateExec requires a single input partition");
        }
        let stream = DecodedFlowEventStream::try_new(self.input.execute(0, context.clone())?)?;
        let mut state = AggregateState {
            aggregate: self.aggregate.clone(),
            accumulators: self.accumulators.clone(),
            groups: HashMap::new(),
            memory: MemoryConsumer::new("StreamAggregateExec").register(context.memory_pool()),
        };
        let initial = state.apply(FlowEvent::append_only_data(RecordBatch::new_empty(
            self.aggregate.input().schema(),
        )))?;
        let stream = stream
            .scan(state, |state, event| {
                futures::future::ready(Some(event.and_then(|event| state.apply(event))))
            })
            .map_ok(futures::stream::iter)
            .try_flatten();
        let stream = futures::stream::iter(initial).chain(stream);
        Ok(Box::pin(EncodedFlowEventStream::new(Box::pin(
            FlowEventStreamAdapter::new(self.aggregate.schema(), stream),
        ))))
    }
}

struct GroupState {
    count: usize,
    accumulators: Vec<StreamAccumulator>,
    output: Option<Row>,
}

impl GroupState {
    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .accumulators
                .iter()
                .map(StreamAccumulator::size)
                .sum::<usize>()
            + self.output.as_ref().map_or(0, RowMultiset::row_size)
    }

    fn evaluate(&mut self, key: &Row) -> Result<Row> {
        let mut output = key.clone();
        for accumulator in &mut self.accumulators {
            output.push(accumulator.evaluate()?);
        }
        Ok(output)
    }
}

struct AggregateState {
    aggregate: Arc<AggregateExec>,
    accumulators: Arc<Vec<AccumulatorFactory>>,
    groups: HashMap<Row, GroupState>,
    memory: MemoryReservation,
}

impl AggregateState {
    fn apply(&mut self, event: FlowEvent) -> Result<Vec<Result<FlowEvent>>> {
        let FlowEvent::Data { batch, retracted } = event else {
            return Ok(vec![Ok(event)]);
        };
        let keys = self
            .aggregate
            .group_expr()
            .expr()
            .iter()
            .map(|(expr, _)| expr.evaluate(&batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        let global = keys.is_empty();
        // Only this incoming batch is partitioned into groups. No input rows survive it.
        let mut touched: HashMap<Row, Vec<u64>> = HashMap::new();
        if global && self.groups.is_empty() {
            touched.insert(vec![], vec![]);
        }
        for i in 0..batch.num_rows() {
            let key = keys
                .iter()
                .map(|array| datafusion_common::ScalarValue::try_from_array(array, i))
                .collect::<Result<Row>>()?;
            touched.entry(key).or_default().push(i as u64);
        }
        let mut rows = Vec::new();
        let mut flags = Vec::new();
        for (key, indices) in touched {
            let key = key
                .into_iter()
                .map(datafusion_common::ScalarValue::compacted)
                .collect::<Row>();
            let group = match self.groups.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let group = GroupState {
                        count: 0,
                        accumulators: self
                            .accumulators
                            .iter()
                            .map(AccumulatorFactory::create)
                            .collect::<Result<_>>()?,
                        output: None,
                    };
                    self.memory
                        .try_grow(RowMultiset::row_size(entry.key()) + group.size())?;
                    entry.insert(group)
                }
            };
            let group_flags = BooleanArray::from(
                indices
                    .iter()
                    .map(|i| retracted.value(*i as usize))
                    .collect::<Vec<_>>(),
            );
            for retracted in group_flags.values() {
                group.count = checked_count(group.count, retracted)?;
            }
            // COUNT(*) can prune every input column while retaining the row count.
            let input = if batch.num_columns() == 0 {
                RecordBatch::try_new_with_options(
                    batch.schema(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(indices.len())),
                )?
            } else {
                take_record_batch(&batch, &UInt64Array::from(indices))?
            };
            for ((expr, predicate), accumulator) in self
                .aggregate
                .aggr_expr()
                .iter()
                .zip(self.aggregate.filter_expr())
                .zip(&mut group.accumulators)
            {
                let (input, retracted) = if let Some(predicate) = predicate {
                    let values = predicate.evaluate(&input)?.into_array(input.num_rows())?;
                    let predicate =
                        values
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                datafusion_common::exec_datafusion_err!(
                                    "aggregate filter must be boolean"
                                )
                            })?;
                    let flags = filter(&group_flags, predicate)?;
                    let flags = flags
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            datafusion_common::internal_datafusion_err!(
                                "invalid aggregate retraction flags"
                            )
                        })?;
                    (filter_record_batch(&input, predicate)?, flags.clone())
                } else {
                    (input.clone(), group_flags.clone())
                };
                if input.num_rows() == 0 {
                    continue;
                }
                let values = expr
                    .expressions()
                    .iter()
                    .map(|arg| arg.evaluate(&input)?.into_array(input.num_rows()))
                    .collect::<Result<Vec<_>>>()?;
                for i in 0..input.num_rows() {
                    let old_size = accumulator.size();
                    accumulator.apply(&values, i, retracted.value(i))?;
                    self.memory
                        .try_resize(self.memory.size() - old_size + accumulator.size())?;
                }
            }
            let old_size = group.size();
            let next = if global || group.count > 0 {
                Some(group.evaluate(&key)?)
            } else {
                None
            };
            if next != group.output {
                if let Some(old) = group.output.take() {
                    rows.push(old);
                    flags.push(true);
                }
                if let Some(new) = &next {
                    rows.push(new.clone());
                    flags.push(false);
                }
                group.output = next;
            }
            self.memory
                .try_resize(self.memory.size() - old_size + group.size())?;
            if !global && group.count == 0 {
                let (stored_key, group) = self.groups.remove_entry(&key).ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!("missing streaming group")
                })?;
                self.memory
                    .shrink(RowMultiset::row_size(&stored_key) + group.size());
                if self.groups.is_empty() || self.groups.len() < self.groups.capacity() / 4 {
                    self.groups.shrink_to_fit();
                }
            }
        }
        let mut events = Vec::new();
        for (rows, flags) in rows.chunks(1024).zip(flags.chunks(1024)) {
            events.push(Ok(FlowEvent::Data {
                batch: rows_to_batch(self.aggregate.schema(), rows)?,
                retracted: BooleanArray::from(flags.to_vec()),
            }));
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::functions_aggregate::{average, count, min_max, sum};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    fn state(distinct: bool, extrema: bool, budget: usize) -> Result<AggregateState> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Float64, true),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let mut functions = vec![count::count_udaf(), sum::sum_udaf(), average::avg_udaf()];
        if extrema {
            functions.extend([min_max::min_udaf(), min_max::max_udaf()]);
        }
        let expressions = functions
            .into_iter()
            .map(|function| {
                let name = function.name().to_string();
                AggregateExprBuilder::new(function, vec![Arc::new(Column::new("v", 1))])
                    .schema(schema.clone())
                    .alias(name)
                    .with_distinct(distinct)
                    .build()
                    .map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?;
        let factories = expressions
            .iter()
            .map(|expr| AccumulatorFactory::try_new(expr, schema.clone()))
            .collect::<Result<Vec<_>>>()?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(Arc::new(Column::new("g", 0)), "g".into())]),
            expressions.clone(),
            vec![None; expressions.len()],
            Arc::new(EmptyExec::new(schema.clone())),
            schema,
        )?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
        Ok(AggregateState {
            aggregate: Arc::new(aggregate),
            accumulators: Arc::new(factories),
            groups: HashMap::new(),
            memory: MemoryConsumer::new("test").register(&pool),
        })
    }

    fn apply(state: &mut AggregateState, values: Vec<f64>, retracted: bool) -> Result<()> {
        let len = values.len();
        let batch = RecordBatch::try_new(
            state.aggregate.input().schema(),
            vec![
                Arc::new(Int64Array::from(vec![0; len])),
                Arc::new(Float64Array::from(values)),
                // Unused, distinct payloads must never become retained aggregate state.
                Arc::new(StringArray::from_iter_values(
                    (0..len).map(|i| format!("{i:04096}")),
                )),
            ],
        )?;
        state.apply(FlowEvent::Data {
            batch,
            retracted: BooleanArray::from(vec![retracted; len]),
        })?;
        Ok(())
    }

    #[test]
    fn arithmetic_state_is_constant_size_and_groups_release_memory() -> Result<()> {
        let mut state = state(false, false, 16 * 1024)?;
        apply(&mut state, vec![1.0], false)?;
        let bytes = state.memory.size();
        apply(&mut state, (2..1002).map(f64::from).collect(), false)?;
        assert_eq!(state.memory.size(), bytes);
        assert_eq!(state.groups.len(), 1);
        apply(&mut state, (1..1002).rev().map(f64::from).collect(), true)?;
        assert!(state.groups.is_empty());
        assert_eq!(state.memory.size(), 0);
        Ok(())
    }

    #[test]
    fn distinct_and_extrema_state_tracks_values_not_occurrences() -> Result<()> {
        let mut state = state(true, true, 16 * 1024)?;
        apply(&mut state, vec![1.0], false)?;
        let bytes = state.memory.size();
        apply(&mut state, vec![1.0; 1000], false)?;
        assert_eq!(state.memory.size(), bytes);
        apply(&mut state, vec![1.0; 1000], true)?;
        assert_eq!(state.memory.size(), bytes);
        apply(&mut state, vec![1.0], true)?;
        assert!(state.groups.is_empty());
        assert_eq!(state.memory.size(), 0);
        Ok(())
    }

    #[test]
    fn distinct_values_respect_memory_budget() -> Result<()> {
        let mut state = state(true, true, 16 * 1024)?;
        assert!(matches!(
            apply(&mut state, (0..1000).map(f64::from).collect(), false),
            Err(datafusion_common::DataFusionError::ResourcesExhausted(_))
        ));
        Ok(())
    }
}
