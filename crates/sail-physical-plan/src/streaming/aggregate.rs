use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
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

use super::state::{Row, RowMultiset, row_at, rows_to_batch};

/// Maintains a grouped dynamic table and emits -old/+new aggregate rows.
/// Live inputs are retained so arbitrary deletions also work for DISTINCT and extrema.
#[derive(Debug)]
pub struct StreamAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    aggregate: Arc<AggregateExec>,
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
        for expr in aggregate.aggr_expr() {
            if !expr.order_bys().is_empty() {
                return plan_err!("ordered streaming aggregates are not supported");
            }
            expr.create_accumulator()?;
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

#[derive(Default)]
struct GroupState {
    rows: RowMultiset,
    output: Option<Row>,
}

struct AggregateState {
    aggregate: Arc<AggregateExec>,
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
        let mut touched = HashSet::new();
        if global && self.groups.is_empty() {
            self.memory.try_grow(RowMultiset::row_size(&vec![]))?;
            self.groups.insert(vec![], GroupState::default());
            touched.insert(vec![]);
        }
        for i in 0..batch.num_rows() {
            let key = keys
                .iter()
                .map(|array| datafusion_common::ScalarValue::try_from_array(array, i))
                .collect::<Result<Row>>()?;
            if !self.groups.contains_key(&key) {
                self.memory.try_grow(RowMultiset::row_size(&key))?;
                self.groups.insert(key.clone(), GroupState::default());
            }
            let group = self.groups.get_mut(&key).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!("missing streaming group")
            })?;
            group
                .rows
                .apply(row_at(&batch, i)?, retracted.value(i), &mut self.memory)?;
            touched.insert(key);
        }
        let mut rows = Vec::new();
        let mut flags = Vec::new();
        for key in touched {
            let group = self.groups.get_mut(&key).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!("missing streaming group")
            })?;
            let next = if global || !group.rows.is_empty() {
                Some(evaluate_group(&self.aggregate, &key, &group.rows)?)
            } else {
                None
            };
            if next != group.output {
                if let Some(old) = group.output.take() {
                    self.memory.shrink(RowMultiset::row_size(&old));
                    rows.push(old);
                    flags.push(true);
                }
                if let Some(new) = &next {
                    self.memory.try_grow(RowMultiset::row_size(new))?;
                    rows.push(new.clone());
                    flags.push(false);
                }
                group.output = next;
            }
            if !global && group.rows.is_empty() {
                self.groups.remove(&key);
                if self.groups.is_empty() || self.groups.len() < self.groups.capacity() / 4 {
                    self.groups.shrink_to_fit();
                }
                self.memory.shrink(RowMultiset::row_size(&key));
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

fn evaluate_group(aggregate: &AggregateExec, key: &Row, rows: &RowMultiset) -> Result<Row> {
    let mut accumulators = aggregate
        .aggr_expr()
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()?;
    let mut input = rows.rows();
    loop {
        let chunk = input.by_ref().take(1024).cloned().collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let batch = rows_to_batch(aggregate.input().schema(), &chunk)?;
        for ((expr, filter), accumulator) in aggregate
            .aggr_expr()
            .iter()
            .zip(aggregate.filter_expr())
            .zip(&mut accumulators)
        {
            let filtered = match filter {
                Some(filter) => {
                    let values = filter.evaluate(&batch)?.into_array(batch.num_rows())?;
                    let predicate =
                        values
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                datafusion_common::exec_datafusion_err!(
                                    "aggregate filter must be boolean"
                                )
                            })?;
                    filter_record_batch(&batch, predicate)?
                }
                None => batch.clone(),
            };
            if filtered.num_rows() == 0 {
                continue;
            }
            let values = expr
                .expressions()
                .iter()
                .map(|arg| arg.evaluate(&filtered)?.into_array(filtered.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            accumulator.update_batch(&values)?;
        }
    }
    let mut output = key.clone();
    for accumulator in &mut accumulators {
        output.push(accumulator.evaluate()?);
    }
    Ok(output)
}
