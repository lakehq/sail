use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{Array, BinaryArray, Float64Array, UInt64Array};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::compare_rows;
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
    RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::{RangeExpr, RepartitionExec};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message;
use sail_common_datafusion::replay::check_bounded;

use crate::replay::ReplayExec;

#[cfg(test)]
mod tests;

const PARTITION_SAMPLES: usize = 1024;
const REDUCER_CANDIDATES: usize = 65536;
const DESTINATION: &str = "__sail_range_destination";

fn properties(schema: SchemaRef, partitions: usize) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(partitions),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

fn key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("priority", DataType::Float64, false),
    ]))
}

fn bounds_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Binary,
        false,
    )]))
}

/// A sampled range exchange. The payload and evaluated keys share one replay
/// materialization, including when expressions or source data are nondeterministic.
#[derive(Debug)]
pub struct SampledRangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<PhysicalSortExpr>,
    partitions: usize,
    properties: Arc<PlanProperties>,
    local: OnceLock<Arc<dyn ExecutionPlan>>,
}

impl SampledRangeRepartitionExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        keys: Vec<PhysicalSortExpr>,
        partitions: usize,
    ) -> Result<Self> {
        check_bounded(input.as_ref())?;
        if partitions == 0 {
            return plan_err!("range repartition requires at least one output partition");
        }
        Ok(Self {
            properties: properties(input.schema(), partitions),
            input,
            keys,
            partitions,
            local: OnceLock::new(),
        })
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn keys(&self) -> &[PhysicalSortExpr] {
        &self.keys
    }
    pub fn partitions(&self) -> usize {
        self.partitions
    }

    pub fn prepared_input(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut expressions = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                (
                    Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        expressions.extend(
            self.keys
                .iter()
                .enumerate()
                .map(|(i, key)| (key.expr.clone(), format!("__sail_range_key_{i}"))),
        );
        Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
    }

    pub fn key_columns(&self) -> Vec<(usize, SortOptions)> {
        let offset = self.input.schema().fields().len();
        self.keys
            .iter()
            .enumerate()
            .map(|(i, key)| (offset + i, key.options))
            .collect()
    }

    pub fn finish_plan(&self, assigned: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let partitioning =
            destination_partitioning(assigned.schema().fields().len() - 1, self.partitions)?;
        let shuffle = Arc::new(RepartitionExec::try_new(
            assigned,
            Partitioning::Range(partitioning),
        )?);
        self.drop_internal_columns(shuffle)
    }

    pub fn drop_internal_columns(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let expressions = self
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                (
                    Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
    }

    fn local_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(plan) = self.local.get() {
            return Ok(plan.clone());
        }
        let replay = Arc::new(ReplayExec::try_new(
            self.prepared_input(self.input.clone())?,
        )?);
        let samples = Arc::new(RangeSampleExec::try_new(
            replay.clone(),
            self.key_columns(),
        )?);
        let bounds = Arc::new(ReplayExec::try_new(Arc::new(RangeBoundsExec::try_new(
            samples,
            self.partitions,
            self.keys.iter().map(|key| key.options).collect(),
        )?))?);
        let assigned = Arc::new(RangeAssignExec::try_new(
            replay,
            bounds,
            self.key_columns(),
        )?);
        let plan = self.finish_plan(assigned)?;
        let _ = self.local.set(plan);
        self.local.get().cloned().ok_or_else(|| {
            datafusion::common::internal_datafusion_err!("range plan initialization failed")
        })
    }
}

impl DisplayAs for SampledRangeRepartitionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SampledRangeRepartitionExec: partitions={}, keys={:?}",
            self.partitions, self.keys
        )
    }
}
impl ExecutionPlan for SampledRangeRepartitionExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for key in &self.keys {
            if f(&key.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
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
            return plan_err!("sampled range expects one child");
        };
        Ok(Arc::new(Self::try_new(
            input,
            self.keys.clone(),
            self.partitions,
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.local_plan()?.execute(partition, context)
    }
}

pub fn destination_partitioning(column: usize, partitions: usize) -> Result<RangePartitioning> {
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
        DESTINATION,
        column,
    )))])
    .ok_or_else(|| datafusion::common::internal_datafusion_err!("missing destination ordering"))?;
    RangePartitioning::try_new(
        ordering,
        (1..partitions)
            .map(|i| SplitPoint::new(vec![ScalarValue::UInt64(Some(i as u64))]))
            .collect(),
    )
}

fn validate_keys(schema: &Schema, keys: &[(usize, SortOptions)]) -> Result<()> {
    for (i, _) in keys {
        if *i >= schema.fields().len() {
            return plan_err!(
                "range key index {i} exceeds schema width {}",
                schema.fields().len()
            );
        }
    }
    Ok(())
}

// Transport typed scalar values, not RowConverter's instance-specific row encoding.
// Nested/dictionary values are serialized here so samples cannot pin source buffers.
#[derive(prost::Message)]
struct SampleKey {
    #[prost(message, repeated, tag = "1")]
    values: Vec<datafusion_proto::protobuf::ScalarValue>,
}

fn encode_key(batch: &RecordBatch, keys: &[(usize, SortOptions)], row: usize) -> Result<Vec<u8>> {
    let values = keys
        .iter()
        .map(|(index, _)| {
            let value = ScalarValue::try_from_array(batch.column(*index), row)?;
            (&value).try_into().map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SampleKey { values }.encode_to_vec())
}

fn decode_key(bytes: &[u8]) -> Result<Vec<ScalarValue>> {
    SampleKey::decode(bytes)
        .map_err(|e| datafusion::common::exec_datafusion_err!("invalid sampled range key: {e}"))?
        .values
        .iter()
        .map(|value| value.try_into().map_err(Into::into))
        .collect()
}

fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// Compact KEY-only reservoir with its source population weight. Small partitions
/// retain all keys. Selected rows own bytes rather than slices of payload buffers.
#[derive(Debug)]
pub struct RangeSampleExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<(usize, SortOptions)>,
    properties: Arc<PlanProperties>,
}
impl RangeSampleExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, keys: Vec<(usize, SortOptions)>) -> Result<Self> {
        check_bounded(input.as_ref())?;
        validate_keys(&input.schema(), &keys)?;
        Ok(Self {
            properties: properties(key_schema(), input.output_partitioning().partition_count()),
            input,
            keys,
        })
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn keys(&self) -> &[(usize, SortOptions)] {
        &self.keys
    }
}
impl DisplayAs for RangeSampleExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangeSampleExec: keys={:?}, capacity={PARTITION_SAMPLES}",
            self.keys
        )
    }
}
impl ExecutionPlan for RangeSampleExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
            return plan_err!("range sample expects one child");
        };
        Ok(Arc::new(Self::try_new(input, self.keys.clone())?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context.clone())?;
        let keys = self.keys.clone();
        let schema = self.schema();
        let output_schema = schema.clone();
        let stream = stream::once(async move {
            let reservation = MemoryConsumer::new("RangeSample").register(context.memory_pool());
            let mut samples: Vec<Vec<u8>> = Vec::new();
            let mut bytes = 0;
            let mut count: u64 = 0;
            let mut seed = partition as u64;
            while let Some(batch) = input.next().await {
                let batch = batch?;
                if keys.is_empty() {
                    continue;
                }
                for row in 0..batch.num_rows() {
                    count = count.checked_add(1).ok_or_else(|| {
                        datafusion::common::exec_datafusion_err!("range sample population overflow")
                    })?;
                    let index = if samples.len() < PARTITION_SAMPLES {
                        samples.len()
                    } else {
                        (random(&mut seed) % count) as usize
                    };
                    if index >= PARTITION_SAMPLES {
                        continue;
                    }
                    let key = encode_key(&batch, &keys, row)?;
                    let old = samples.get(index).map_or(0, |s| s.len());
                    bytes = bytes - old + key.len();
                    // Account for the owned sample and the eventual Arrow output copy.
                    reservation.try_resize(2 * bytes + PARTITION_SAMPLES * 64)?;
                    if index == samples.len() {
                        samples.push(key);
                    } else {
                        samples[index] = key;
                    }
                }
            }
            let weight = if samples.is_empty() {
                0.0
            } else {
                count as f64 / samples.len() as f64
            };
            let mut seed = (partition as u64) ^ 0x6a09e667f3bcc909;
            let priorities = (0..samples.len())
                .map(|_| {
                    let uniform =
                        ((random(&mut seed) >> 11) as f64 + 1.0) / ((1u64 << 53) as f64 + 1.0);
                    weight / uniform
                })
                .collect::<Vec<_>>();
            RecordBatch::try_new(
                output_schema,
                vec![
                    Arc::new(BinaryArray::from_iter_values(
                        samples.iter().map(Vec::as_slice),
                    )),
                    Arc::new(Float64Array::from(vec![weight; samples.len()])),
                    Arc::new(Float64Array::from(priorities)),
                ],
            )
            .map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct Candidate {
    key: Vec<u8>,
    values: Vec<ScalarValue>,
    weight: f64,
    priority: f64,
}
impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.key == other.key
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .total_cmp(&self.priority)
            .then_with(|| other.key.cmp(&self.key))
    }
}

/// Bounded weighted priority sampling prevents the reducer from retaining an
/// unbounded number of producer samples. No reduction occurs for small inputs.
struct Candidates {
    heap: BinaryHeap<Candidate>,
    threshold: f64,
    bytes: usize,
    reservation: MemoryReservation,
}
impl Candidates {
    fn new(context: &TaskContext) -> Self {
        Self {
            heap: BinaryHeap::new(),
            threshold: 0.0,
            bytes: 0,
            reservation: MemoryConsumer::new("RangeBounds").register(context.memory_pool()),
        }
    }
    fn insert(&mut self, key: &[u8], weight: f64, priority: f64) -> Result<()> {
        if !weight.is_finite() || weight <= 0.0 || !priority.is_finite() || priority < weight {
            return exec_err!("invalid range sample weight or priority");
        }
        if self.heap.len() == REDUCER_CANDIDATES {
            if self
                .heap
                .peek()
                .is_some_and(|candidate| priority <= candidate.priority)
            {
                self.threshold = self.threshold.max(priority);
                return Ok(());
            }
            if let Some(removed) = self.heap.pop() {
                self.bytes -= removed.key.len() + ScalarValue::size_of_vec(&removed.values);
                self.threshold = self.threshold.max(removed.priority);
            }
        }
        let values = decode_key(key)?;
        self.bytes += key.len() + ScalarValue::size_of_vec(&values);
        self.reservation
            .try_resize(3 * self.bytes + (self.heap.len() + 1) * 128)?;
        self.heap.push(Candidate {
            key: key.to_vec(),
            values,
            weight,
            priority,
        });
        Ok(())
    }
    fn bounds(&mut self, partitions: usize, options: &[SortOptions]) -> Result<Vec<Vec<u8>>> {
        let mut candidates = self.heap.drain().collect::<Vec<_>>();
        if candidates.iter().any(|c| c.values.len() != options.len()) {
            return exec_err!("range sample key width does not match ordering");
        }
        let mut error = None;
        candidates.sort_unstable_by(|a, b| match compare_rows(&a.values, &b.values, options) {
            Ok(order) => order,
            Err(e) => {
                error = Some(e);
                Ordering::Equal
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        let total = candidates
            .iter()
            .map(|c| c.weight.max(self.threshold))
            .sum::<f64>();
        let step = total / partitions as f64;
        let mut next = step;
        let mut cumulative = 0.0;
        let mut bounds = Vec::new();
        let mut previous: Option<&[ScalarValue]> = None;
        for candidate in &candidates {
            // DF ranges are lower-inclusive. A split starts the later group,
            // never the peer group whose cumulative weight just crossed target.
            let distinct = match previous {
                Some(previous) => {
                    compare_rows(previous, &candidate.values, options)? != Ordering::Equal
                }
                None => false,
            };
            if distinct && cumulative >= next && bounds.len() < partitions - 1 {
                bounds.push(candidate.key.clone());
                next += step;
            }
            cumulative += candidate.weight.max(self.threshold);
            previous = Some(&candidate.values);
        }
        Ok(bounds)
    }
}

#[derive(Debug)]
pub struct RangeBoundsExec {
    input: Arc<dyn ExecutionPlan>,
    partitions: usize,
    ordering: Vec<SortOptions>,
    properties: Arc<PlanProperties>,
}
impl RangeBoundsExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitions: usize,
        ordering: Vec<SortOptions>,
    ) -> Result<Self> {
        if partitions == 0 || input.schema() != key_schema() {
            return plan_err!("invalid range boundary input or partition count");
        }
        check_bounded(input.as_ref())?;
        Ok(Self {
            input,
            partitions,
            ordering,
            properties: properties(bounds_schema(), 1),
        })
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn partitions(&self) -> usize {
        self.partitions
    }
    pub fn ordering(&self) -> &[SortOptions] {
        &self.ordering
    }
}
impl DisplayAs for RangeBoundsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangeBoundsExec: partitions={}, capacity={REDUCER_CANDIDATES}",
            self.partitions
        )
    }
}
impl ExecutionPlan for RangeBoundsExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
            return plan_err!("range bounds expects one child");
        };
        Ok(Arc::new(Self::try_new(
            input,
            self.partitions,
            self.ordering.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("range bounds requires partition zero");
        }
        let input = self.input.clone();
        let partitions = self.partitions;
        let ordering = self.ordering.clone();
        let schema = self.schema();
        let output_schema = schema.clone();
        let stream = stream::once(async move {
            let mut candidates = Candidates::new(&context);
            // Bounded concurrency locally; distributed inputs are already committed.
            for partition in 0..input.output_partitioning().partition_count() {
                let mut stream = input.execute(partition, context.clone())?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    let keys = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            datafusion::common::exec_datafusion_err!("invalid sample keys")
                        })?;
                    let weights = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            datafusion::common::exec_datafusion_err!("invalid sample weights")
                        })?;
                    let priorities = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            datafusion::common::exec_datafusion_err!("invalid sample priorities")
                        })?;
                    for i in 0..batch.num_rows() {
                        candidates.insert(keys.value(i), weights.value(i), priorities.value(i))?;
                    }
                }
            }
            let bounds = candidates.bounds(partitions, &ordering)?;
            RecordBatch::try_new(
                output_schema,
                vec![Arc::new(BinaryArray::from_iter_values(
                    bounds.iter().map(Vec::as_slice),
                ))],
            )
            .map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
pub struct RangeAssignExec {
    input: Arc<dyn ExecutionPlan>,
    bounds: Arc<dyn ExecutionPlan>,
    keys: Vec<(usize, SortOptions)>,
    properties: Arc<PlanProperties>,
}
impl RangeAssignExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        bounds: Arc<dyn ExecutionPlan>,
        keys: Vec<(usize, SortOptions)>,
    ) -> Result<Self> {
        check_bounded(input.as_ref())?;
        validate_keys(&input.schema(), &keys)?;
        if bounds.schema() != bounds_schema() {
            return plan_err!("invalid range bounds schema");
        }
        let schema = input.schema();
        let fields = schema
            .fields()
            .iter()
            .cloned()
            .chain(std::iter::once(Arc::new(Field::new(
                DESTINATION,
                DataType::UInt64,
                false,
            ))))
            .collect::<Vec<_>>();
        let properties = properties(
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            input.output_partitioning().partition_count(),
        );
        Ok(Self {
            input,
            bounds,
            keys,
            properties,
        })
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn bounds(&self) -> &Arc<dyn ExecutionPlan> {
        &self.bounds
    }
    pub fn keys(&self) -> &[(usize, SortOptions)] {
        &self.keys
    }
}
impl DisplayAs for RangeAssignExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RangeAssignExec: keys={:?}", self.keys)
    }
}
impl ExecutionPlan for RangeAssignExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input, &self.bounds]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input, bounds]: [Arc<dyn ExecutionPlan>; 2] = children.try_into().map_err(|_| {
            datafusion::common::plan_datafusion_err!("range assign expects two children")
        })?;
        Ok(Arc::new(Self::try_new(input, bounds, self.keys.clone())?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut bounds_input = self.bounds.execute(0, context.clone())?;
        let input = self.input.clone();
        let keys = self.keys.clone();
        let schema = self.schema();
        let output_schema = schema.clone();
        let stream = stream::once(async move {
            let reservation =
                MemoryConsumer::new("RangeAssignBounds").register(context.memory_pool());
            let mut bounds: Vec<SplitPoint> = Vec::new();
            let mut bytes = 0;
            while let Some(batch) = bounds_input.next().await {
                let batch = batch?;
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        datafusion::common::exec_datafusion_err!("invalid range bounds")
                    })?;
                for key in array.iter().flatten() {
                    let values = decode_key(key)?;
                    if values.len() != keys.len() {
                        return exec_err!("range boundary key width does not match ordering");
                    }
                    bytes += ScalarValue::size_of_vec(&values);
                    reservation.try_resize(3 * bytes)?;
                    bounds.push(SplitPoint::new(values));
                }
            }
            let ordering = LexOrdering::new(keys.iter().map(|(i, options)| {
                PhysicalSortExpr::new(
                    Arc::new(Column::new(input.schema().field(*i).name(), *i)),
                    *options,
                )
            }));
            let expression = ordering
                .map(|ordering| {
                    let expressions = ordering.iter().map(|key| key.expr.clone()).collect();
                    let partitioning = RangePartitioning::try_new(ordering, bounds)?;
                    RangeExpr::try_new(expressions, &partitioning)
                })
                .transpose()?;
            let input = input.execute(partition, context)?;
            Ok(input.map(move |batch| {
                let _keep_reservation = &reservation;
                let batch = batch?;
                let ids = match &expression {
                    Some(expression) => {
                        expression.evaluate(&batch)?.into_array(batch.num_rows())?
                    }
                    None => Arc::new(UInt64Array::from(vec![0; batch.num_rows()])),
                };
                let mut columns = batch.columns().to_vec();
                columns.push(ids);
                RecordBatch::try_new(output_schema.clone(), columns).map_err(Into::into)
            }))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
