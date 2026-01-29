use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::compute::{cast, concat, SortOptions};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::{stream, TryStreamExt};
use url::Url;

use crate::physical_plan::{COL_LOG_IS_REMOVE, COL_LOG_VERSION, COL_REPLAY_PATH};

const OUTPUT_BATCH_ROWS: usize = 8192;

/// A unary node that filters Delta log rows into the active set (tombstone replay).
///
/// Input:
/// - `__sail_delta_replay_path` (Utf8): derived file path key
/// - `__sail_delta_is_remove` (Boolean): derived marker for `remove(path)`
/// - `__sail_delta_log_version` (Int64): derived log version from `_delta_log` filename prefix
/// - payload columns: any additional columns carried through for the winning `add` row
///
/// Notes:
/// - We replay actions in **newest-first** order (by `__sail_delta_log_version`) within each
///   `__sail_delta_replay_path`. The first action we observe for a path decides the final state
///   (newer actions override older ones).
/// - This exec is designed to be **spill-friendly** by requiring the input to be hash-partitioned
///   and sorted by `__sail_delta_replay_path`, enabling streaming replay without materializing the
///   full active set in memory.
#[derive(Debug, Clone)]
pub struct DeltaLogReplayExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    version: i64,
    partition_columns: Vec<String>,
    // purely for observability (EXPLAIN); populated by the planner when available
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    cache: PlanProperties,
}

impl DeltaLogReplayExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        version: i64,
        partition_columns: Vec<String>,
        checkpoint_files: Vec<String>,
        commit_files: Vec<String>,
    ) -> Self {
        let schema = Self::output_schema(&input.schema());
        let output_partitions = input.output_partitioning().partition_count().max(1);
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(output_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            table_url,
            version,
            partition_columns,
            checkpoint_files,
            commit_files,
            cache,
        }
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn checkpoint_files(&self) -> &[String] {
        &self.checkpoint_files
    }

    pub fn commit_files(&self) -> &[String] {
        &self.commit_files
    }

    fn output_schema(input_schema: &SchemaRef) -> SchemaRef {
        let mut fields = Vec::with_capacity(input_schema.fields().len());
        for f in input_schema.fields() {
            if f.name() == COL_REPLAY_PATH
                || f.name() == COL_LOG_IS_REMOVE
                || f.name() == COL_LOG_VERSION
            {
                continue;
            }
            fields.push(f.as_ref().clone());
        }
        Arc::new(Schema::new(fields))
    }
}

struct ReplayState {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    output_col_indices: Vec<usize>,

    // current group state (per replay_path)
    current_path: Option<String>,
    current_removed: bool,
    current_row: Option<Vec<ArrayRef>>,

    // output builders for the next RecordBatch
    out_col_slices: Vec<Vec<ArrayRef>>,
    out_rows: usize,
    finished: bool,
}

impl ReplayState {
    fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        _partition_columns: Vec<String>,
    ) -> Self {
        let input_schema = input.schema();
        let mut output_col_indices = Vec::with_capacity(input_schema.fields().len());
        for (i, f) in input_schema.fields().iter().enumerate() {
            if f.name() == COL_REPLAY_PATH
                || f.name() == COL_LOG_IS_REMOVE
                || f.name() == COL_LOG_VERSION
            {
                continue;
            }
            output_col_indices.push(i);
        }
        let out_cols = output_schema.fields().len();
        Self {
            input,
            out_col_slices: vec![Vec::new(); out_cols],
            output_schema,
            out_rows: 0,
            output_col_indices,
            current_path: None,
            current_removed: false,
            current_row: None,
            finished: false,
        }
    }

    fn flush_current_group(&mut self) {
        if let Some(row) = self.current_row.take() {
            for (i, col) in row.into_iter().enumerate() {
                // Safety: current_row is created with exactly `out_col_slices.len()` columns.
                if let Some(dst) = self.out_col_slices.get_mut(i) {
                    dst.push(col);
                }
            }
            self.out_rows += 1;
        }
        self.current_removed = false;
    }

    fn take_output_batch(&mut self) -> Result<RecordBatch> {
        if self.out_rows == 0 {
            return internal_err!("DeltaLogReplayExec produced an empty output batch");
        }
        let mut cols = Vec::with_capacity(self.out_col_slices.len());
        for slices in &mut self.out_col_slices {
            if slices.is_empty() {
                return internal_err!(
                    "DeltaLogReplayExec produced an incomplete output batch (missing column slices)"
                );
            }
            let parts: Vec<&dyn Array> = slices.iter().map(|a| a.as_ref()).collect();
            let col = concat(&parts).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            slices.clear();
            cols.push(col);
        }
        self.out_rows = 0;
        RecordBatch::try_new(Arc::clone(&self.output_schema), cols)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn process_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // The planner is expected to materialize `COL_REPLAY_PATH`, `COL_LOG_IS_REMOVE`, and
        // `COL_LOG_VERSION` (via a projection) for distribution/sorting and tombstone logic.
        let replay_path = batch.column_by_name(COL_REPLAY_PATH).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DeltaLogReplayExec input must have Utf8 column '{COL_REPLAY_PATH}'"
            ))
        })?;
        let replay_path = cast(replay_path.as_ref(), &DataType::Utf8)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let replay_path = replay_path
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "DeltaLogReplayExec '{COL_REPLAY_PATH}' must be Utf8"
                ))
            })?;

        let is_remove = batch.column_by_name(COL_LOG_IS_REMOVE).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DeltaLogReplayExec input must have Boolean column '{COL_LOG_IS_REMOVE}'"
            ))
        })?;
        let is_remove = cast(is_remove.as_ref(), &DataType::Boolean)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let is_remove = is_remove
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "DeltaLogReplayExec '{COL_LOG_IS_REMOVE}' must be Boolean"
                ))
            })?;

        // Require a log version column so the planner can enforce newest-first replay semantics.
        let log_version = batch.column_by_name(COL_LOG_VERSION).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DeltaLogReplayExec input must have Int64 column '{COL_LOG_VERSION}'"
            ))
        })?;
        let log_version = cast(log_version.as_ref(), &DataType::Int64)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let _log_version = log_version
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "DeltaLogReplayExec '{COL_LOG_VERSION}' must be Int64"
                ))
            })?;

        for row in 0..batch.num_rows() {
            if replay_path.is_null(row) {
                continue;
            }
            let key = replay_path.value(row).to_string();
            match &self.current_path {
                None => {
                    self.current_path = Some(key);
                }
                Some(cur) if cur != &key => {
                    self.flush_current_group();
                    self.current_path = Some(key);
                }
                _ => {}
            }

            // Apply tombstone first (remove).
            //
            // Input is expected to be sorted by (replay_path ASC, log_version DESC), so the first
            // decision we make for a given path must be final (older actions cannot override it).
            if self.current_removed || self.current_row.is_some() {
                continue;
            }

            if !is_remove.is_null(row) && is_remove.value(row) {
                self.current_removed = true;
                continue;
            }

            // Capture the first non-remove row's payload (newest add wins).
            let mut out = Vec::with_capacity(self.output_col_indices.len());
            for idx in &self.output_col_indices {
                out.push(batch.column(*idx).slice(row, 1));
            }
            self.current_row = Some(out);
        }

        Ok(())
    }
}

#[async_trait]
impl ExecutionPlan for DeltaLogReplayExec {
    fn name(&self) -> &'static str {
        "DeltaLogReplayExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Log replay is only correct if all actions for the same `path` are co-located in the
        // same partition. We express this as a required hash distribution over the derived
        // `COL_REPLAY_PATH` column, which is expected to be produced by the planner.
        //
        // If the column isn't present (e.g. an unexpected upstream), fall back to single
        // partition to preserve correctness.
        let idx = match self.input.schema().index_of(COL_REPLAY_PATH) {
            Ok(i) => i,
            Err(_) => return vec![Distribution::SinglePartition],
        };
        let expr: Arc<dyn datafusion_physical_expr::PhysicalExpr> = Arc::new(
            datafusion_physical_expr::expressions::Column::new(COL_REPLAY_PATH, idx),
        );
        vec![Distribution::HashPartitioned(vec![expr])]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // The streaming replay logic relies on all rows for the same `COL_REPLAY_PATH`
        // being adjacent within each partition, so we require a local ordering by
        // (COL_REPLAY_PATH ASC, COL_LOG_VERSION DESC).
        // TODO: Add COL_LOG_IS_REMOVE ASC as a tie-breaker so Add beats Remove within
        // the same path/version (needed for DV updates: Remove(old dv) + Add(new dv)).
        let replay_idx = match self.input.schema().index_of(COL_REPLAY_PATH) {
            Ok(i) => i,
            Err(_) => return vec![None],
        };
        let version_idx = match self.input.schema().index_of(COL_LOG_VERSION) {
            Ok(i) => i,
            Err(_) => return vec![None],
        };

        let Some(ordering) = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new(COL_REPLAY_PATH, replay_idx)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new(COL_LOG_VERSION, version_idx)),
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
        ]) else {
            return vec![None];
        };

        vec![Some(OrderingRequirements::from(ordering))]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaLogReplayExec expects exactly one child");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.version,
            self.partition_columns.clone(),
            self.checkpoint_files.clone(),
            self.commit_files.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let output_schema = self.schema();
        let partition_columns = self.partition_columns.clone();

        let state = ReplayState::new(input_stream, Arc::clone(&output_schema), partition_columns);

        let s = stream::try_unfold(state, |mut st| async move {
            loop {
                if st.out_rows >= OUTPUT_BATCH_ROWS {
                    let out = st.take_output_batch()?;
                    return Ok(Some((out, st)));
                }

                if st.finished {
                    // Final flush.
                    st.flush_current_group();
                    if st.out_rows > 0 {
                        let out = st.take_output_batch()?;
                        return Ok(Some((out, st)));
                    }
                    return Ok(None);
                }

                match st.input.try_next().await? {
                    Some(batch) => {
                        st.process_batch(&batch)?;
                        continue;
                    }
                    None => {
                        st.finished = true;
                        continue;
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, s)))
    }
}

impl DisplayAs for DeltaLogReplayExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaLogReplayExec(table_path={}, version={})",
                    self.table_url, self.version
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                writeln!(f, "table_path={}", self.table_url)?;
                writeln!(f, "version={}", self.version)?;
                if !self.checkpoint_files.is_empty() {
                    writeln!(f, "checkpoint_files=[{}]", self.checkpoint_files.join(", "))?;
                }
                if !self.commit_files.is_empty() {
                    writeln!(f, "commit_files=[{}]", self.commit_files.join(", "))?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, NullBufferBuilder, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};
    use futures::TryStreamExt;

    use super::*;

    #[derive(Debug)]
    struct OneBatchExec {
        batch: RecordBatch,
        cache: PlanProperties,
    }

    impl OneBatchExec {
        fn new(batch: RecordBatch) -> Self {
            let schema = batch.schema();
            let cache = PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            );
            Self { batch, cache }
        }
    }

    impl DisplayAs for OneBatchExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "OneBatchExec")
                }
                DisplayFormatType::TreeRender => write!(f, "OneBatchExec"),
            }
        }
    }

    #[async_trait]
    impl ExecutionPlan for OneBatchExec {
        fn name(&self) -> &'static str {
            "OneBatchExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return internal_err!("OneBatchExec does not support children");
            }
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if partition != 0 {
                return internal_err!("OneBatchExec only supports a single partition");
            }
            let schema = self.schema();
            let batch = self.batch.clone();
            let s = stream::once(async move { Ok(batch) });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
        }
    }

    fn struct_array_with_validity(
        fields: Fields,
        columns: Vec<ArrayRef>,
        validity: Vec<bool>,
    ) -> StructArray {
        let mut b = NullBufferBuilder::new_with_len(0);
        for v in validity {
            b.append(v);
        }
        let nulls = b.finish();
        StructArray::new(fields, columns, nulls)
    }

    #[tokio::test]
    async fn replay_add_and_remove_produces_active_set() -> Result<()> {
        // Newest-first within a path:
        // Row0: remove a (v1)
        // Row1: add a (v0)  -> should be ignored due to tombstone at v1
        // Row2: add b

        let add_fields: Fields = vec![
            Arc::new(Field::new("path", DataType::Utf8, true)),
            Arc::new(Field::new("size", DataType::Int64, true)),
            Arc::new(Field::new("modificationTime", DataType::Int64, true)),
        ]
        .into();

        let add_path = Arc::new(StringArray::from(vec![None, Some("a"), Some("b")])) as ArrayRef;
        let add_size = Arc::new(Int64Array::from(vec![None, Some(1), Some(2)])) as ArrayRef;
        let add_mod = Arc::new(Int64Array::from(vec![None, Some(10), Some(20)])) as ArrayRef;
        let add_struct = struct_array_with_validity(
            add_fields,
            vec![add_path, add_size, add_mod],
            vec![false, true, true],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("add", add_struct.data_type().clone(), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));
        let replay_path =
            Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("b")])) as ArrayRef;
        let is_remove = Arc::new(BooleanArray::from(vec![true, false, false])) as ArrayRef;
        let log_version = Arc::new(Int64Array::from(vec![1, 0, 2])) as ArrayRef;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(add_struct) as ArrayRef,
                replay_path,
                is_remove,
                log_version,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let input: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(batch));
        let exec = Arc::new(DeltaLogReplayExec::new(
            input,
            #[allow(clippy::unwrap_used)]
            Url::parse("file:///tmp/delta").unwrap(),
            0,
            vec![],
            vec![],
            vec![],
        ));

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx)?;
        #[allow(clippy::unwrap_used)]
        let out = stream.try_next().await?.unwrap();
        #[allow(clippy::unwrap_used)]
        let add = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        #[allow(clippy::unwrap_used)]
        let path_col = add
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(path_col.value(0), "b");
        Ok(())
    }

    #[tokio::test]
    async fn replay_path_reuse_add_after_remove_is_active() -> Result<()> {
        // Newest-first within a path:
        // Row0: add a (v2)    -> should win
        // Row1: remove a (v1) -> must be ignored (older)

        let add_fields: Fields = vec![
            Arc::new(Field::new("path", DataType::Utf8, true)),
            Arc::new(Field::new("size", DataType::Int64, true)),
            Arc::new(Field::new("modificationTime", DataType::Int64, true)),
        ]
        .into();

        let add_path = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
        let add_size = Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef;
        let add_mod = Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef;
        let add_struct = struct_array_with_validity(
            add_fields,
            vec![add_path, add_size, add_mod],
            vec![true, false],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("add", add_struct.data_type().clone(), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));
        let replay_path = Arc::new(StringArray::from(vec![Some("a"), Some("a")])) as ArrayRef;
        let is_remove = Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef;
        let log_version = Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(add_struct) as ArrayRef,
                replay_path,
                is_remove,
                log_version,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let input: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(batch));
        let exec = Arc::new(DeltaLogReplayExec::new(
            input,
            #[allow(clippy::unwrap_used)]
            Url::parse("file:///tmp/delta").unwrap(),
            0,
            vec![],
            vec![],
            vec![],
        ));

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx)?;
        #[allow(clippy::unwrap_used)]
        let out = stream.try_next().await?.unwrap();
        #[allow(clippy::unwrap_used)]
        let add = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        #[allow(clippy::unwrap_used)]
        let path_col = add
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(path_col.value(0), "a");
        Ok(())
    }
}
