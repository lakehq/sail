use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::{SortOptions, filter_record_batch, partition};
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, OrderingRequirements, Partitioning,
    PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::{Result, ScalarValue, Statistics, exec_err, internal_err};
use futures::Stream;

/// Fused physical operator for Spark `session_window` aggregation (phase 2, the
/// `MergingSessionsExec`-equivalent).
///
/// Given input hash-partitioned on the group keys and locally sorted by
/// `(keys, time)`, it merges sessions AND drives the user aggregates in a single
/// pass, emitting one row per session. Only the open session's accumulator state is
/// held (O(1) per session) — no per-session row buffering.
#[derive(Debug)]
pub struct SessionAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    time_column: String,
    end_column: String,
    /// Output group columns in order; exactly one equals the session struct column.
    group_columns: Vec<String>,
    /// Name of the `{start, end}` struct column among `group_columns`.
    session_output: String,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    /// Optional `FILTER (WHERE ...)` predicate per aggregate, aligned with
    /// `aggregates`. `None` means the aggregate consumes every session row.
    filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Required `(partition_columns..., time)` input ordering, resolved eagerly
    /// so a missing column fails at construction instead of silently dropping
    /// the sort requirement.
    required_ordering: Option<OrderingRequirements>,
    metrics: ExecutionPlanMetricsSet,
}

/// The emitted per-partition ordering: the `partition_columns` prefix —
/// sessions close in scan order of the `(keys, time)`-sorted input. The output
/// is also ordered by `session.start` within each key, but declaring that
/// component needs a `get_field` expression built with the session's
/// `ConfigOptions` (`ScalarFunctionExpr` equality compares config options, so
/// one built from defaults never matches planner-built expressions and only
/// adds comparison cost); threading the config here is a follow-up. Returns
/// `None` when there are no keys.
fn output_ordering(schema: &SchemaRef, partition_columns: &[String]) -> Option<LexOrdering> {
    let options = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let mut sort_exprs: Vec<PhysicalSortExpr> = Vec::with_capacity(partition_columns.len());
    for name in partition_columns {
        let idx = schema.index_of(name).ok()?;
        sort_exprs.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(name, idx)),
            options,
        });
    }
    LexOrdering::new(sort_exprs)
}

impl SessionAggregateExec {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_columns: Vec<String>,
        time_column: String,
        end_column: String,
        group_columns: Vec<String>,
        session_output: String,
        aggregates: Vec<Arc<AggregateFunctionExpr>>,
        filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        if filters.len() != aggregates.len() {
            return internal_err!(
                "SessionAggregateExec expects one filter slot per aggregate ({} vs {})",
                filters.len(),
                aggregates.len()
            );
        }
        if !group_columns.iter().any(|c| c == &session_output) {
            return internal_err!(
                "SessionAggregateExec group columns {group_columns:?} must contain the session \
                 output column {session_output:?}"
            );
        }
        // Rows shrink to one per session but stay key-partitioned; report the
        // input partitioning and the keys-prefix ordering so parents can reuse
        // both. The input hash exprs are bound to input-schema indices,
        // so rebind them to the output schema (columns move and most input
        // columns disappear); anything that does not survive degrades to
        // unknown partitioning instead of advertising dangling indices.
        let eq_properties = match output_ordering(&schema, &partition_columns) {
            Some(ordering) => {
                EquivalenceProperties::new_with_orderings(schema.clone(), vec![ordering])
            }
            None => EquivalenceProperties::new(schema.clone()),
        };
        let partitioning = match input.output_partitioning() {
            Partitioning::Hash(exprs, n) => {
                let remapped = exprs
                    .iter()
                    .map(|e| {
                        e.downcast_ref::<Column>().and_then(|c| {
                            schema.index_of(c.name()).ok().map(|idx| {
                                Arc::new(Column::new(c.name(), idx)) as Arc<dyn PhysicalExpr>
                            })
                        })
                    })
                    .collect::<Option<Vec<_>>>();
                match remapped {
                    Some(exprs) => Partitioning::Hash(exprs, *n),
                    None => Partitioning::UnknownPartitioning(*n),
                }
            }
            other => other.clone(),
        };
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        // The merge relies on rows of the same group being adjacent and ordered
        // by time, so require a local ordering by `(partition_columns..., time)`.
        let input_schema = input.schema();
        let mut sort_exprs = Vec::with_capacity(partition_columns.len() + 1);
        for name in partition_columns
            .iter()
            .chain(std::iter::once(&time_column))
        {
            let idx = input_schema.index_of(name)?;
            sort_exprs.push(PhysicalSortExpr {
                expr: Arc::new(Column::new(name, idx)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            });
        }
        let required_ordering = LexOrdering::new(sort_exprs).map(OrderingRequirements::from);
        Ok(Self {
            input,
            partition_columns,
            time_column,
            end_column,
            group_columns,
            session_output,
            aggregates,
            filters,
            schema,
            properties,
            required_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn time_column(&self) -> &str {
        &self.time_column
    }

    pub fn end_column(&self) -> &str {
        &self.end_column
    }

    pub fn group_columns(&self) -> &[String] {
        &self.group_columns
    }

    pub fn session_output(&self) -> &str {
        &self.session_output
    }

    pub fn aggregates(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggregates
    }

    pub fn filters(&self) -> &[Option<Arc<dyn PhysicalExpr>>] {
        &self.filters
    }

    fn partition_exprs(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        let input_schema = self.input.schema();
        self.partition_columns
            .iter()
            .map(|name| {
                let idx = input_schema.index_of(name)?;
                Ok(Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>)
            })
            .collect()
    }
}

impl DisplayAs for SessionAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SessionAggregateExec: partition_by=[{}], time={}, end={}, aggs={}",
            self.partition_columns.join(", "),
            self.time_column,
            self.end_column,
            self.aggregates.len()
        )
    }
}

impl ExecutionPlan for SessionAggregateExec {
    fn name(&self) -> &'static str {
        "SessionAggregateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.partition_exprs() {
            Ok(exprs) if !exprs.is_empty() => vec![Distribution::HashPartitioned(exprs)],
            _ => vec![Distribution::SinglePartition],
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.required_ordering.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("SessionAggregateExec requires exactly one child");
        };
        Ok(Arc::new(SessionAggregateExec::try_new(
            Arc::clone(input),
            self.partition_columns.clone(),
            self.time_column.clone(),
            self.end_column.clone(),
            self.group_columns.clone(),
            self.session_output.clone(),
            self.aggregates.clone(),
            self.filters.clone(),
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_schema = self.input.schema();
        let partition_indices = self
            .partition_columns
            .iter()
            .map(|name| input_schema.index_of(name))
            .collect::<Result<Vec<_>, _>>()?;
        let time_idx = input_schema.index_of(&self.time_column)?;
        let end_idx = input_schema.index_of(&self.end_column)?;

        // Locate the session struct field and its timezone in the output schema.
        let session_idx = self.schema.index_of(&self.session_output)?;
        let DataType::Struct(struct_fields) = self.schema.field(session_idx).data_type() else {
            return exec_err!("SessionAggregateExec session column must be a struct");
        };
        let tz = match struct_fields.first().map(|f| f.data_type()) {
            Some(DataType::Timestamp(TimeUnit::Microsecond, tz)) => tz.clone(),
            _ => return exec_err!("SessionAggregateExec struct fields must be Timestamp(us, *)"),
        };

        // Non-session group columns line up 1:1 with the key tuple (both follow
        // grouping order), so assign key positions sequentially.
        let mut group_sources = Vec::with_capacity(self.group_columns.len());
        let mut key_count = 0usize;
        for name in &self.group_columns {
            if name == &self.session_output {
                group_sources.push(GroupSource::Session);
            } else {
                group_sources.push(GroupSource::Key(key_count));
                key_count += 1;
            }
        }
        if key_count != partition_indices.len() {
            return exec_err!(
                "SessionAggregateExec: {key_count} non-session group columns but \
                 {} partition keys",
                partition_indices.len()
            );
        }

        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let num_sessions = MetricBuilder::new(&self.metrics).counter("num_sessions", partition);
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SessionAggregateStream {
            input,
            baseline,
            num_sessions,
            output_schema: self.schema.clone(),
            struct_fields: struct_fields.clone(),
            tz,
            partition_indices,
            time_idx,
            end_idx,
            group_sources,
            aggregates: self.aggregates.clone(),
            filters: self.filters.clone(),
            accumulators: Vec::new(),
            cur_key: None,
            cur_start: 0,
            cur_end: 0,
            finished: false,
        }))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        // Row count collapses to one-per-session, not known here.
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

/// One finalized session: its key values, bounds, and aggregate results.
struct SessionRow {
    key: Vec<ScalarValue>,
    start: i64,
    end: i64,
    aggs: Vec<ScalarValue>,
}

/// How `build_batch` materializes each output group column: the session struct
/// or key-tuple position `i`. Precomputed once per stream.
enum GroupSource {
    Session,
    Key(usize),
}

struct SessionAggregateStream {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    struct_fields: Fields,
    tz: Option<Arc<str>>,
    partition_indices: Vec<usize>,
    time_idx: usize,
    end_idx: usize,
    /// One entry per output group column, in schema order.
    group_sources: Vec<GroupSource>,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    /// Optional `FILTER (WHERE ...)` predicate per aggregate, aligned with
    /// `aggregates`.
    filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// Accumulators for the currently-open session (one per aggregate).
    accumulators: Vec<Box<dyn Accumulator>>,
    cur_key: Option<Vec<ScalarValue>>,
    cur_start: i64,
    cur_end: i64,
    baseline: BaselineMetrics,
    /// Closed (emitted) sessions.
    num_sessions: Count,
    finished: bool,
}

impl SessionAggregateStream {
    fn row_key(&self, batch: &RecordBatch, i: usize) -> Result<Vec<ScalarValue>> {
        self.partition_indices
            .iter()
            .map(|&idx| ScalarValue::try_from_array(batch.column(idx), i))
            .collect()
    }

    /// Open a fresh session at row `i`, creating new accumulators.
    fn start_session(&mut self, key: Vec<ScalarValue>, start: i64, end: i64) -> Result<()> {
        self.accumulators = self
            .aggregates
            .iter()
            .map(|a| a.create_accumulator())
            .collect::<Result<Vec<_>>>()?;
        self.cur_key = Some(key);
        self.cur_start = start;
        self.cur_end = end;
        Ok(())
    }

    /// Feed a contiguous run of the open session's rows to every accumulator,
    /// applying each aggregate's `FILTER (WHERE ...)` predicate if it has one.
    fn update(&mut self, slice: &RecordBatch) -> Result<()> {
        if slice.num_rows() == 0 {
            return Ok(());
        }
        for ((agg, filter), acc) in self
            .aggregates
            .iter()
            .zip(self.filters.iter())
            .zip(self.accumulators.iter_mut())
        {
            // `FILTER (WHERE p)` masks rows out of this aggregate only; session
            // boundaries come from the time/end columns and are unaffected.
            let rows: Cow<RecordBatch> = match filter {
                Some(predicate) => {
                    let evaluated = predicate.evaluate(slice)?.into_array(slice.num_rows())?;
                    let mask = evaluated
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "session aggregate FILTER predicate must be boolean".to_string(),
                            )
                        })?;
                    Cow::Owned(filter_record_batch(slice, &normalize_mask(mask))?)
                }
                None => Cow::Borrowed(slice),
            };
            if rows.num_rows() == 0 {
                continue;
            }
            let args = agg
                .expressions()
                .iter()
                .map(|e| e.evaluate(&rows)?.into_array(rows.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            acc.update_batch(&args)?;
        }
        Ok(())
    }

    /// Finalize the open session into a [`SessionRow`].
    fn finalize(&mut self) -> Result<Option<SessionRow>> {
        let Some(key) = self.cur_key.take() else {
            return Ok(None);
        };
        let aggs = self
            .accumulators
            .iter_mut()
            .map(|acc| acc.evaluate())
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(SessionRow {
            key,
            start: self.cur_start,
            end: self.cur_end,
            aggs,
        }))
    }

    /// Group-key runs come from arrow's vectorized `partition` kernel, so
    /// per-row work is only the `i64` time comparisons.
    fn process_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(None);
        }
        let times = as_micros(batch.column(self.time_idx))?;
        let ends = as_micros(batch.column(self.end_idx))?;
        // Runs of equal group keys in this batch (the whole batch when there are
        // no group keys).
        let key_columns = self
            .partition_indices
            .iter()
            .map(|&idx| Arc::clone(batch.column(idx)))
            .collect::<Vec<_>>();
        let ranges = if key_columns.is_empty() {
            std::iter::once(0..n).collect()
        } else {
            partition(&key_columns)?.ranges()
        };

        let mut closed: Vec<SessionRow> = Vec::new();
        let mut seg_start = 0usize;

        for (range_idx, range) in ranges.iter().enumerate() {
            // The open session can only continue into this batch's FIRST key run;
            // every later run starts a different key by construction.
            let continues_open = range_idx == 0
                && match &self.cur_key {
                    Some(k) => *k == self.row_key(&batch, range.start)?,
                    None => false,
                };
            let merge_from = if continues_open {
                range.start
            } else {
                // Close the open session: first feed its rows in this batch,
                // then open a new session at the run's first row.
                if self.cur_key.is_some() {
                    if range.start > seg_start {
                        self.update(&batch.slice(seg_start, range.start - seg_start))?;
                    }
                    if let Some(row) = self.finalize()? {
                        closed.push(row);
                    }
                }
                let key = self.row_key(&batch, range.start)?;
                self.start_session(key, times.value(range.start), ends.value(range.start))?;
                seg_start = range.start;
                range.start + 1
            };
            for i in merge_from..range.end {
                // Spark merges when `time <= cur_end` (a row exactly on the
                // end still joins); only `time > cur_end` opens a new session.
                if times.value(i) > self.cur_end {
                    if i > seg_start {
                        self.update(&batch.slice(seg_start, i - seg_start))?;
                    }
                    if let Some(row) = self.finalize()? {
                        closed.push(row);
                    }
                    let key = self.row_key(&batch, i)?;
                    self.start_session(key, times.value(i), ends.value(i))?;
                    seg_start = i;
                } else {
                    self.cur_end = self.cur_end.max(ends.value(i));
                }
            }
        }

        // Feed the open session's trailing rows; it stays open across batches.
        if n > seg_start {
            self.update(&batch.slice(seg_start, n - seg_start))?;
        }

        if closed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.build_batch(closed)?))
        }
    }

    /// Build one output batch from finalized sessions, in output schema order:
    /// the group columns (the session struct among them), then the aggregates.
    fn build_batch(&self, rows: Vec<SessionRow>) -> Result<RecordBatch> {
        self.num_sessions.add(rows.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.output_schema.fields().len());

        for source in &self.group_sources {
            match source {
                GroupSource::Session => {
                    let starts = TimestampMicrosecondArray::from(
                        rows.iter().map(|r| r.start).collect::<Vec<_>>(),
                    )
                    .with_timezone_opt(self.tz.clone());
                    let ends = TimestampMicrosecondArray::from(
                        rows.iter().map(|r| r.end).collect::<Vec<_>>(),
                    )
                    .with_timezone_opt(self.tz.clone());
                    columns.push(Arc::new(StructArray::new(
                        self.struct_fields.clone(),
                        vec![Arc::new(starts) as ArrayRef, Arc::new(ends) as ArrayRef],
                        None,
                    )));
                }
                GroupSource::Key(pos) => {
                    columns.push(ScalarValue::iter_to_array(
                        rows.iter().map(|r| r.key[*pos].clone()),
                    )?);
                }
            }
        }

        for j in 0..self.aggregates.len() {
            columns.push(ScalarValue::iter_to_array(
                rows.iter().map(|r| r.aggs[j].clone()),
            )?);
        }

        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }
}

/// Normalize a FILTER predicate mask so null entries are treated as `false`
/// (SQL `FILTER (WHERE p)` keeps only rows where `p` is TRUE). Returns the mask
/// unchanged when it has no nulls.
fn normalize_mask(mask: &BooleanArray) -> BooleanArray {
    if mask.null_count() == 0 {
        mask.clone()
    } else {
        mask.iter().map(|v| Some(v.unwrap_or(false))).collect()
    }
}

fn as_micros(array: &ArrayRef) -> Result<&TimestampMicrosecondArray> {
    array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "session_window expected a Timestamp(Microsecond) column".to_string(),
            )
        })
}

impl RecordBatchStream for SessionAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for SessionAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Polling a terminated stream again is formally unspecified.
        if self.finished {
            return Poll::Ready(None);
        }
        let baseline = self.baseline.clone();
        loop {
            match self.input.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(batch))) => {
                    let _timer = baseline.elapsed_compute().timer();
                    match self.process_batch(batch) {
                        Ok(Some(out)) => {
                            return baseline.record_poll(Poll::Ready(Some(Ok(out))));
                        }
                        Ok(None) => continue,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(None) => {
                    if self.finished {
                        return Poll::Ready(None);
                    }
                    self.finished = true;
                    let _timer = baseline.elapsed_compute().timer();
                    match self.finalize() {
                        Ok(Some(row)) => match self.build_batch(vec![row]) {
                            Ok(out) => {
                                return baseline.record_poll(Poll::Ready(Some(Ok(out))));
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        },
                        Ok(None) => return Poll::Ready(None),
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array, Int64Array, StructArray};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use futures::StreamExt;

    use super::*;

    /// One row per (key, time, value); the end candidate is `time + 10`.
    fn batch(schema: &SchemaRef, rows: &[(i32, i64, i64)]) -> RecordBatch {
        let keys = Int32Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
        let times = TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        let ends =
            TimestampMicrosecondArray::from(rows.iter().map(|r| r.1 + 10).collect::<Vec<_>>());
        let values = Int64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(keys),
                Arc::new(times),
                Arc::new(ends),
                Arc::new(values),
            ],
        )
        .expect("input batch")
    }

    /// The fused operator must produce one row per session with correct
    /// accumulator state when session rows span input batches, including a
    /// `FILTER (WHERE ...)` mask applied across the boundary.
    #[tokio::test]
    async fn fused_aggregation_survives_batch_boundaries() -> Result<()> {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("#t", ts.clone(), true),
            Field::new("#e0", ts.clone(), true),
            Field::new("v", DataType::Int64, true),
        ]));
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("start", ts.clone(), true),
            Field::new("end", ts, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("#w", struct_type, false),
            Field::new("cnt", DataType::Int64, true),
            Field::new("cnt_pos", DataType::Int64, true),
        ]));
        let v_col = || Arc::new(Column::new("v", 3)) as Arc<dyn PhysicalExpr>;
        let cnt = AggregateExprBuilder::new(count_udaf(), vec![v_col()])
            .schema(input_schema.clone())
            .alias("cnt")
            .build()?;
        let cnt_pos = AggregateExprBuilder::new(count_udaf(), vec![v_col()])
            .schema(input_schema.clone())
            .alias("cnt_pos")
            .build()?;
        let positive: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            v_col(),
            Operator::Gt,
            lit(ScalarValue::Int64(Some(0))),
        ));
        // Same session layout as the `SessionWindowExec` test: key 2's session
        // spans the second batch boundary; its negative value lands in the
        // second batch, so the filter mask must apply across the carry-over.
        let batches = vec![
            batch(&input_schema, &[(1, 0, 1), (1, 5, -1)]),
            batch(&input_schema, &[(1, 30, 2), (2, 100, 3)]),
            batch(&input_schema, &[(2, 105, -3), (3, 200, 5)]),
        ];
        let input = MemorySourceConfig::try_new_exec(&[batches], input_schema.clone(), None)?;
        let exec = SessionAggregateExec::try_new(
            input,
            vec!["k".to_string()],
            "#t".to_string(),
            "#e0".to_string(),
            vec!["k".to_string(), "#w".to_string()],
            "#w".to_string(),
            vec![Arc::new(cnt), Arc::new(cnt_pos)],
            vec![None, Some(positive)],
            output_schema.clone(),
        )?;
        let ctx = SessionContext::new().task_ctx();
        let mut stream = exec.execute(0, ctx)?;
        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        let output = concat_batches(&output_schema, &batches)?;
        assert_eq!(output.num_rows(), 4);
        let keys = output
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key column");
        let sessions = output
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("session struct column");
        let starts = as_micros(sessions.column(0))?;
        let ends = as_micros(sessions.column(1))?;
        let cnt = output
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cnt column");
        let cnt_pos = output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cnt_pos column");
        let expected = [
            (1, 0, 15, 2, 1),
            (1, 30, 40, 1, 1),
            (2, 100, 115, 2, 1),
            (3, 200, 210, 1, 1),
        ];
        for (i, (key, start, end, count, count_pos)) in expected.iter().enumerate() {
            assert_eq!(
                (
                    keys.value(i),
                    starts.value(i),
                    ends.value(i),
                    cnt.value(i),
                    cnt_pos.value(i)
                ),
                (*key, *start, *end, *count, *count_pos),
                "session {i}"
            );
        }
        let metrics = exec.metrics().expect("metrics");
        assert_eq!(metrics.output_rows(), Some(4));
        assert_eq!(
            metrics.sum_by_name("num_sessions").map(|m| m.as_usize()),
            Some(4)
        );
        Ok(())
    }

    /// The advertised output partitioning must be rebound to the output
    /// schema: group columns move (the session struct is first here), so the
    /// input's hash exprs carry stale indices.
    #[test]
    fn output_partitioning_rebinds_to_output_schema() -> Result<()> {
        use datafusion::physical_plan::repartition::RepartitionExec;

        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("#t", ts.clone(), true),
            Field::new("#e0", ts.clone(), true),
            Field::new("v", DataType::Int64, true),
        ]));
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("start", ts.clone(), true),
            Field::new("end", ts, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("#w", struct_type, false),
            Field::new("k", DataType::Int32, true),
            Field::new("cnt", DataType::Int64, true),
        ]));
        let source = MemorySourceConfig::try_new_exec(&[vec![]], input_schema.clone(), None)?;
        let input = Arc::new(RepartitionExec::try_new(
            source,
            Partitioning::Hash(
                vec![Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>],
                4,
            ),
        )?);
        let cnt = AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(Column::new("v", 3)) as Arc<dyn PhysicalExpr>],
        )
        .schema(input_schema.clone())
        .alias("cnt")
        .build()?;
        let exec = SessionAggregateExec::try_new(
            input,
            vec!["k".to_string()],
            "#t".to_string(),
            "#e0".to_string(),
            vec!["#w".to_string(), "k".to_string()],
            "#w".to_string(),
            vec![Arc::new(cnt)],
            vec![None],
            output_schema,
        )?;
        let Partitioning::Hash(exprs, 4) = exec.properties().output_partitioning() else {
            return internal_err!("expected hash output partitioning");
        };
        let column = exprs[0]
            .downcast_ref::<Column>()
            .ok_or_else(|| datafusion_common::DataFusionError::Internal("not a column".into()))?;
        assert_eq!((column.name(), column.index()), ("k", 1));
        Ok(())
    }

    /// A partition or time column missing from the input schema must fail at
    /// construction, not silently drop the sort requirement later.
    #[test]
    fn try_new_rejects_missing_columns() -> Result<()> {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("#t", ts.clone(), true),
            Field::new("#e0", ts, true),
        ]));
        let input = MemorySourceConfig::try_new_exec(&[vec![]], input_schema.clone(), None)?;
        let result = SessionAggregateExec::try_new(
            input,
            vec!["#missing".to_string()],
            "#t".to_string(),
            "#e0".to_string(),
            vec!["#w".to_string()],
            "#w".to_string(),
            vec![],
            vec![],
            input_schema,
        );
        match result {
            Err(e) => assert!(e.to_string().contains("#missing"), "{e}"),
            Ok(_) => return internal_err!("expected a missing-column error"),
        }
        Ok(())
    }
}
