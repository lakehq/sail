use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayRef, StructArray, TimestampMicrosecondArray};
use datafusion::arrow::compute::{SortOptions, concat_batches, partition};
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::{
    Result, ScalarValue, Statistics, exec_err, internal_datafusion_err, internal_err,
};
use futures::Stream;

/// Physical operator for Spark `session_window` (`UpdatingSessionsExec`-style):
/// appends a `{start, end}` struct column to its input. Rows merge per group of
/// `partition_columns` while the next row's time is at or before the session's
/// end (`end_column` = `time + gap`, precomputed by the resolver). Requires the
/// input hash-partitioned on the group keys and sorted by `(keys..., time)`;
/// the one-pass merge buffers only the currently-open session's rows.
#[derive(Debug)]
pub struct SessionWindowExec {
    input: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    time_column: String,
    end_column: String,
    output_column: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Required `(partition_columns..., time)` input ordering, resolved at
    /// construction so a missing column fails early.
    required_ordering: Option<OrderingRequirements>,
    metrics: ExecutionPlanMetricsSet,
}

impl SessionWindowExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_columns: Vec<String>,
        time_column: String,
        end_column: String,
        output_column: String,
        schema: SchemaRef,
    ) -> Result<Self> {
        // Rows never move or reorder: the child's partitioning and orderings
        // pass through (matching `maintains_input_order`).
        let eq_properties = EquivalenceProperties::new_with_orderings(
            schema.clone(),
            input
                .equivalence_properties()
                .oeq_class()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
        );
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
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
            output_column,
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

    pub fn output_column(&self) -> &str {
        &self.output_column
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

impl DisplayAs for SessionWindowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SessionWindowExec: partition_by=[{}], time={}, end={}, output={}",
            self.partition_columns.join(", "),
            self.time_column,
            self.end_column,
            self.output_column
        )
    }
}

impl ExecutionPlan for SessionWindowExec {
    fn name(&self) -> &'static str {
        "SessionWindowExec"
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

    fn maintains_input_order(&self) -> Vec<bool> {
        // One output row per input row, in input order.
        vec![true]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // A session must never span partitions: hash-partition by the group
        // keys, or a single partition when there are none.
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
            return internal_err!("SessionWindowExec requires exactly one child");
        };
        Ok(Arc::new(SessionWindowExec::try_new(
            Arc::clone(input),
            self.partition_columns.clone(),
            self.time_column.clone(),
            self.end_column.clone(),
            self.output_column.clone(),
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

        // The appended struct field and its timestamp timezone.
        let out_field = self
            .schema
            .field(self.schema.index_of(&self.output_column)?);
        let DataType::Struct(struct_fields) = out_field.data_type() else {
            return exec_err!("SessionWindowExec output column must be a struct");
        };
        let tz = match struct_fields.first().map(|f| f.data_type()) {
            Some(DataType::Timestamp(TimeUnit::Microsecond, tz)) => tz.clone(),
            _ => return exec_err!("SessionWindowExec struct fields must be Timestamp(us, *)"),
        };

        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let num_sessions = MetricBuilder::new(&self.metrics).counter("num_sessions", partition);
        let reservation = MemoryConsumer::new(format!("SessionWindowStream[{partition}]"))
            .register(context.memory_pool());
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SessionWindowStream {
            input,
            input_schema,
            output_schema: self.schema.clone(),
            struct_fields: struct_fields.clone(),
            tz,
            partition_indices,
            time_idx,
            end_idx,
            open_rows: Vec::new(),
            reservation,
            buffered_bytes: 0,
            baseline,
            num_sessions,
            cur_key: None,
            cur_start: 0,
            cur_end: 0,
            finished: false,
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        // Row count is preserved; column stats shift due to the added column.
        let stats = self.input.partition_statistics(partition)?;
        Ok(Arc::new(
            Statistics::new_unknown(&self.schema).with_num_rows(stats.num_rows),
        ))
    }
}

struct SessionWindowStream {
    input: SendableRecordBatchStream,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    struct_fields: Fields,
    tz: Option<Arc<str>>,
    partition_indices: Vec<usize>,
    time_idx: usize,
    end_idx: usize,
    open_rows: Vec<RecordBatch>,
    /// Tracks `open_rows` against the task memory pool: a session spanning many
    /// batches retains them all until it closes, so an oversized session must
    /// fail with a resources error instead of aborting the process.
    reservation: MemoryReservation,
    buffered_bytes: usize,
    baseline: BaselineMetrics,
    num_sessions: Count,
    cur_key: Option<Vec<ScalarValue>>,
    cur_start: i64,
    cur_end: i64,
    finished: bool,
}

impl SessionWindowStream {
    /// Buffers a slice that outlives its input batch, charging the memory pool
    /// for the retained size (a slice keeps its parent batch's buffers alive in
    /// full). Slices consumed within one `process_batch` call skip the pool.
    fn buffer_rows(&mut self, rows: RecordBatch) -> Result<()> {
        let bytes = rows.get_array_memory_size();
        self.reservation.try_grow(bytes)?;
        self.buffered_bytes += bytes;
        self.open_rows.push(rows);
        Ok(())
    }

    fn row_key(&self, batch: &RecordBatch, i: usize) -> Result<Vec<ScalarValue>> {
        self.partition_indices
            .iter()
            .map(|&idx| ScalarValue::try_from_array(batch.column(idx), i))
            .collect()
    }

    /// Builds one output batch for a closed session: every buffered row gets the
    /// same `{start, end}` struct. Returns `None` if the session held no rows.
    fn build_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.open_rows.is_empty() {
            return Ok(None);
        }
        let batches = std::mem::take(&mut self.open_rows);
        self.reservation.shrink(self.buffered_bytes);
        self.buffered_bytes = 0;
        self.num_sessions.add(1);
        let input_batch = concat_batches(&self.input_schema, &batches)?;
        let n = input_batch.num_rows();
        if n == 0 {
            return Ok(None);
        }
        // Same start/end for every row in the session.
        let start = TimestampMicrosecondArray::from(vec![self.cur_start; n])
            .with_timezone_opt(self.tz.clone());
        let end = TimestampMicrosecondArray::from(vec![self.cur_end; n])
            .with_timezone_opt(self.tz.clone());
        let struct_arr = StructArray::new(
            self.struct_fields.clone(),
            vec![Arc::new(start) as ArrayRef, Arc::new(end) as ArrayRef],
            None,
        );
        let mut cols = input_batch.columns().to_vec();
        cols.push(Arc::new(struct_arr));
        Ok(Some(RecordBatch::try_new(
            self.output_schema.clone(),
            cols,
        )?))
    }

    /// Processes one input batch, returning the rows of sessions that closed in
    /// it (`None` if nothing closed); the trailing open session is carried in
    /// `open_rows`.
    fn process_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(None);
        }
        let times = as_micros(batch.column(self.time_idx))?;
        let ends = as_micros(batch.column(self.end_idx))?;
        // Runs of equal group keys (the whole batch when there are no keys).
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

        // Output batches for every session that closes during this input batch.
        let mut closed: Vec<RecordBatch> = Vec::new();
        // Start (in this batch) of the open session's rows; carried-over rows
        // live in `open_rows`.
        let mut seg_start = 0usize;

        for (range_idx, range) in ranges.iter().enumerate() {
            // Only the batch's first key run can continue the open session.
            let continues_open = range_idx == 0
                && match &self.cur_key {
                    Some(k) => *k == self.row_key(&batch, range.start)?,
                    None => false,
                };
            let merge_from = if continues_open {
                range.start
            } else {
                // Close the open session and open a new one at this run's
                // first row.
                if range.start > seg_start {
                    self.open_rows
                        .push(batch.slice(seg_start, range.start - seg_start));
                }
                if let Some(out) = self.build_output()? {
                    closed.push(out);
                }
                self.cur_key = Some(self.row_key(&batch, range.start)?);
                self.cur_start = times.value(range.start);
                self.cur_end = ends.value(range.start);
                seg_start = range.start;
                range.start + 1
            };
            for i in merge_from..range.end {
                // Spark merges when `time <= cur_end` (a row exactly on the
                // end still joins); only `time > cur_end` opens a new session.
                if times.value(i) > self.cur_end {
                    if i > seg_start {
                        self.open_rows.push(batch.slice(seg_start, i - seg_start));
                    }
                    if let Some(out) = self.build_output()? {
                        closed.push(out);
                    }
                    self.cur_start = times.value(i);
                    self.cur_end = ends.value(i);
                    seg_start = i;
                } else {
                    // Merge: extend the session end if this row reaches further.
                    self.cur_end = self.cur_end.max(ends.value(i));
                }
            }
        }

        // Carry the open session's trailing rows over to the next batch.
        if n > seg_start {
            self.buffer_rows(batch.slice(seg_start, n - seg_start))?;
        }

        if closed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(concat_batches(&self.output_schema, &closed)?))
        }
    }
}

/// Reads a `Timestamp(Microsecond, *)` column as raw `i64` micros; the
/// resolver's filter guarantees non-null values.
fn as_micros(array: &ArrayRef) -> Result<&TimestampMicrosecondArray> {
    array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            internal_datafusion_err!("session_window expected a Timestamp(Microsecond) column")
        })
}

impl RecordBatchStream for SessionWindowStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for SessionWindowStream {
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
                        // Whole batch merged into the still-open session; poll for more.
                        Ok(None) => continue,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(None) => {
                    // Input exhausted: emit the final still-open session.
                    self.finished = true;
                    let _timer = baseline.elapsed_compute().timer();
                    match self.build_output() {
                        Ok(Some(out)) => {
                            return baseline.record_poll(Poll::Ready(Some(Ok(out))));
                        }
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
    use datafusion::arrow::array::{Array, Int32Array, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn schemas() -> (SchemaRef, SchemaRef) {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("start", ts.clone(), true),
            Field::new("end", ts.clone(), true),
        ]));
        let input = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("#t", ts.clone(), true),
            Field::new("#e0", ts.clone(), true),
        ]));
        let output = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("#t", ts.clone(), true),
            Field::new("#e0", ts, true),
            Field::new("#w", struct_type, false),
        ]));
        (input, output)
    }

    /// One row per (key, time); the end candidate is `time + 10` (gap = 10).
    fn batch(rows: &[(i32, i64)]) -> RecordBatch {
        let keys = Int32Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
        let times = TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        let ends =
            TimestampMicrosecondArray::from(rows.iter().map(|r| r.1 + 10).collect::<Vec<_>>());
        RecordBatch::try_new(
            schemas().0,
            vec![Arc::new(keys), Arc::new(times), Arc::new(ends)],
        )
        .expect("input batch")
    }

    fn session_exec(batches: Vec<RecordBatch>) -> Result<Arc<SessionWindowExec>> {
        let (input_schema, output_schema) = schemas();
        let input = MemorySourceConfig::try_new_exec(&[batches], input_schema, None)?;
        Ok(Arc::new(SessionWindowExec::try_new(
            input,
            vec!["k".to_string()],
            "#t".to_string(),
            "#e0".to_string(),
            "#w".to_string(),
            output_schema,
        )?))
    }

    async fn run(exec: Arc<SessionWindowExec>, ctx: Arc<TaskContext>) -> Result<RecordBatch> {
        let schema = exec.schema();
        Ok(concat_batches(&schema, &collect(exec, ctx).await?)?)
    }

    /// The `(start, end)` pairs of the output's session struct column.
    fn session_bounds(output: &RecordBatch) -> Result<Vec<(i64, i64)>> {
        let sessions = output
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("session struct column");
        let starts = as_micros(sessions.column(0))?;
        let ends = as_micros(sessions.column(1))?;
        Ok(starts
            .values()
            .iter()
            .zip(ends.values().iter())
            .map(|(s, e)| (*s, *e))
            .collect())
    }

    #[tokio::test]
    async fn sessions_survive_batch_boundaries() -> Result<()> {
        // Sorted by (k, t), gap = 10: key 1 merges t=0 and t=5, then t=30 opens
        // a new session; key 2 merges t=100 and t=105 across the second batch
        // boundary; key 3 is flushed at end of input. The first batch boundary
        // splits key 1's open session from its closing row; the key 1 -> 2
        // change happens inside a batch and the 2 -> 3 change at a boundary.
        let exec = session_exec(vec![
            batch(&[(1, 0), (1, 5)]),
            batch(&[(1, 30), (2, 100)]),
            batch(&[(2, 105), (3, 200)]),
        ])?;
        let output = run(exec.clone(), SessionContext::new().task_ctx()).await?;
        assert_eq!(
            session_bounds(&output)?,
            [
                (0, 15),
                (0, 15),
                (30, 40),
                (100, 115),
                (100, 115),
                (200, 210)
            ]
        );
        let metrics = exec.metrics().expect("metrics");
        assert_eq!(metrics.output_rows(), Some(6));
        assert_eq!(
            metrics.sum_by_name("num_sessions").map(|m| m.as_usize()),
            Some(4)
        );
        Ok(())
    }

    #[tokio::test]
    async fn merge_boundaries_and_segment_offsets() -> Result<()> {
        // Batch 1: key 1 merges t=10 exactly on the session end (inclusive
        // boundary), t=25 opens a second session extended by t=30, and t=50 a
        // third (a mid-run close at a nonzero slice offset); key 2 changes
        // mid-batch. Batch 2 starts with a key change at the batch boundary
        // whose time (104) is within key 2's still-open gap.
        let exec = session_exec(vec![
            batch(&[(1, 0), (1, 10), (1, 25), (1, 30), (1, 50), (2, 100)]),
            batch(&[(3, 104), (3, 120)]),
        ])?;
        let output = run(exec.clone(), SessionContext::new().task_ctx()).await?;
        assert_eq!(
            session_bounds(&output)?,
            [
                (0, 20),
                (0, 20),
                (25, 40),
                (25, 40),
                (50, 60),
                (100, 110),
                (104, 114),
                (120, 130)
            ]
        );
        let metrics = exec.metrics().expect("metrics");
        assert_eq!(
            metrics.sum_by_name("num_sessions").map(|m| m.as_usize()),
            Some(6)
        );
        Ok(())
    }

    #[tokio::test]
    async fn reservation_is_released_when_sessions_close() -> Result<()> {
        // Every session spans one batch boundary (one carried slice) and then
        // closes; 300 sequential sessions never hold more than one carry, so
        // the run must fit a modest pool unless accounting leaks.
        let mut batches = Vec::new();
        for i in 0..300i64 {
            batches.push(batch(&[(1, i * 1000)]));
            batches.push(batch(&[(1, i * 1000 + 5)]));
        }
        let exec = session_exec(batches)?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(32 * 1024, 1.0)
            .build_arc()?;
        let ctx = SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx();
        assert_eq!(run(exec, ctx).await?.num_rows(), 600);
        Ok(())
    }

    #[test]
    fn try_new_rejects_missing_columns() -> Result<()> {
        let input_schema = schemas().0;
        let input = MemorySourceConfig::try_new_exec(&[vec![]], input_schema.clone(), None)?;
        let result = SessionWindowExec::try_new(
            input,
            vec!["k".to_string()],
            "#missing".to_string(),
            "#e0".to_string(),
            "#w".to_string(),
            input_schema,
        );
        let err = result.expect_err("expected a missing-column error");
        assert!(err.to_string().contains("#missing"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn oversized_session_fails_with_resources_error() -> Result<()> {
        // One key, consecutive times within the gap: a single session spanning
        // every batch, so `open_rows` retains them all.
        let batches = (0..200i64)
            .map(|i| batch(&[(1, i * 5), (1, i * 5 + 1)]))
            .collect();
        let exec = session_exec(batches)?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(8 * 1024, 1.0)
            .build_arc()?;
        let ctx = SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx();
        let err = run(exec, ctx)
            .await
            .expect_err("expected a resources-exhausted error");
        assert!(err.to_string().contains("Resources exhausted"), "{err}");
        Ok(())
    }
}
