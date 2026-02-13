use std::any::Any;
use std::collections::HashMap;
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
const MAX_COMMIT_REPLAY_ENTRIES: usize = 5_000_000;

#[derive(Debug, Clone)]
enum ReplayMode {
    /// Sort-based replay (spill-friendly). Requires local ordering on
    /// (replay_path ASC, log_version DESC, is_remove ASC).
    Sort { input: Arc<dyn ExecutionPlan> },
    /// Hash-based replay to avoid checkpoint-side SortExec pipeline breakers:
    /// build a small map from commits, stream checkpoint rows, then emit commit-only adds.
    Hash {
        checkpoint: Arc<dyn ExecutionPlan>,
        commits: Arc<dyn ExecutionPlan>,
    },
}

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
    mode: ReplayMode,
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
            mode: ReplayMode::Sort { input },
            table_url,
            version,
            partition_columns,
            checkpoint_files,
            commit_files,
            cache,
        }
    }

    pub fn new_hash(
        checkpoint: Arc<dyn ExecutionPlan>,
        commits: Arc<dyn ExecutionPlan>,
        table_url: Url,
        version: i64,
        partition_columns: Vec<String>,
        checkpoint_files: Vec<String>,
        commit_files: Vec<String>,
    ) -> Self {
        let schema = Self::output_schema(&checkpoint.schema());
        let output_partitions = checkpoint
            .output_partitioning()
            .partition_count()
            .max(commits.output_partitioning().partition_count())
            .max(1);
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(output_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            mode: ReplayMode::Hash {
                checkpoint,
                commits,
            },
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

fn required_replay_columns(
    batch: &RecordBatch,
) -> Result<(Arc<StringArray>, Arc<BooleanArray>, Arc<Int64Array>)> {
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

    let log_version = batch.column_by_name(COL_LOG_VERSION).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "DeltaLogReplayExec input must have Int64 column '{COL_LOG_VERSION}'"
        ))
    })?;
    let log_version = cast(log_version.as_ref(), &DataType::Int64)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let log_version = log_version
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DeltaLogReplayExec '{COL_LOG_VERSION}' must be Int64"
            ))
        })?;

    Ok((
        Arc::new(replay_path.clone()),
        Arc::new(is_remove.clone()),
        Arc::new(log_version.clone()),
    ))
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
        let (replay_path, is_remove, _log_version) = required_replay_columns(batch)?;

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

#[derive(Debug, Clone)]
struct ReplayEntry {
    log_version: i64,
    is_remove: bool,
    payload: Option<Vec<ArrayRef>>,
}

enum HashReplayStage {
    Build,
    Probe,
    Emit,
    Done,
}

struct HashReplayState {
    commits: SendableRecordBatchStream,
    checkpoint: SendableRecordBatchStream,
    output_schema: SchemaRef,
    output_col_indices: Vec<usize>,

    map: HashMap<String, ReplayEntry>,

    // output builders for the next RecordBatch
    out_col_slices: Vec<Vec<ArrayRef>>,
    out_rows: usize,

    stage: HashReplayStage,
    // materialized after probe finishes
    emit_rows: Option<std::vec::IntoIter<Vec<ArrayRef>>>,
}

impl HashReplayState {
    fn new(
        commits: SendableRecordBatchStream,
        checkpoint: SendableRecordBatchStream,
        output_schema: SchemaRef,
    ) -> Self {
        let input_schema = checkpoint.schema();
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
            commits,
            checkpoint,
            out_col_slices: vec![Vec::new(); out_cols],
            output_schema,
            output_col_indices,
            map: HashMap::new(),
            out_rows: 0,
            stage: HashReplayStage::Build,
            emit_rows: None,
        }
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

    fn push_payload_row(&mut self, row: Vec<ArrayRef>) {
        for (i, col) in row.into_iter().enumerate() {
            // Safety: `row` is constructed with exactly `out_col_slices.len()` columns.
            if let Some(dst) = self.out_col_slices.get_mut(i) {
                dst.push(col);
            }
        }
        self.out_rows += 1;
    }

    fn process_commits_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let (replay_path, is_remove, log_version) = required_replay_columns(batch)?;
        for row in 0..batch.num_rows() {
            if replay_path.is_null(row) {
                continue;
            }
            let key = replay_path.value(row).to_string();
            let removed = !is_remove.is_null(row) && is_remove.value(row);
            let version = if log_version.is_null(row) {
                i64::MIN
            } else {
                log_version.value(row)
            };

            let should_replace = match self.map.get(&key) {
                None => true,
                Some(existing) => {
                    // Compare-and-swap winner rule:
                    // - Higher version wins
                    // - At equal version, Add beats Remove (DV update pattern)
                    version > existing.log_version
                        || (version == existing.log_version && existing.is_remove && !removed)
                }
            };
            if !should_replace {
                continue;
            }

            let payload = if removed {
                None
            } else {
                let mut out = Vec::with_capacity(self.output_col_indices.len());
                for idx in &self.output_col_indices {
                    out.push(batch.column(*idx).slice(row, 1));
                }
                Some(out)
            };

            self.map.insert(
                key,
                ReplayEntry {
                    log_version: version,
                    is_remove: removed,
                    payload,
                },
            );
            if self.map.len() > MAX_COMMIT_REPLAY_ENTRIES {
                return Err(DataFusionError::Execution(format!(
                    "DeltaLogReplayExec hash replay exceeded MAX_COMMIT_REPLAY_ENTRIES={MAX_COMMIT_REPLAY_ENTRIES}"
                )));
            }
        }
        Ok(())
    }

    fn process_checkpoint_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let (replay_path, is_remove, _log_version) = required_replay_columns(batch)?;
        for row in 0..batch.num_rows() {
            if replay_path.is_null(row) {
                continue;
            }
            if !is_remove.is_null(row) && is_remove.value(row) {
                // Checkpoints can include tombstones; they are not part of the active add set.
                continue;
            }

            let key = replay_path.value(row).to_string();
            if self.map.contains_key(&key) {
                continue;
            }

            let mut out = Vec::with_capacity(self.output_col_indices.len());
            for idx in &self.output_col_indices {
                out.push(batch.column(*idx).slice(row, 1));
            }
            self.push_payload_row(out);
        }

        Ok(())
    }

    fn finalize_emit_rows(&mut self) {
        if self.emit_rows.is_some() {
            return;
        }
        let mut rows: Vec<Vec<ArrayRef>> = Vec::new();
        for (_k, entry) in self.map.drain() {
            if entry.is_remove {
                continue;
            }
            if let Some(payload) = entry.payload {
                rows.push(payload);
            }
        }
        self.emit_rows = Some(rows.into_iter());
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
        let dist_for = |plan: &Arc<dyn ExecutionPlan>| -> Distribution {
            let idx = match plan.schema().index_of(COL_REPLAY_PATH) {
                Ok(i) => i,
                Err(_) => return Distribution::SinglePartition,
            };
            let expr: Arc<dyn datafusion_physical_expr::PhysicalExpr> = Arc::new(
                datafusion_physical_expr::expressions::Column::new(COL_REPLAY_PATH, idx),
            );
            Distribution::HashPartitioned(vec![expr])
        };

        match &self.mode {
            ReplayMode::Sort { input } => vec![dist_for(input)],
            ReplayMode::Hash {
                checkpoint,
                commits,
            } => {
                vec![dist_for(checkpoint), dist_for(commits)]
            }
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        match &self.mode {
            ReplayMode::Hash { .. } => vec![None, None],
            ReplayMode::Sort { input } => {
                // The streaming replay logic relies on all rows for the same `COL_REPLAY_PATH`
                // being adjacent within each partition, so we require a local ordering by:
                // (COL_REPLAY_PATH ASC, COL_LOG_VERSION DESC, COL_LOG_IS_REMOVE ASC).
                //
                // The extra `COL_LOG_IS_REMOVE` tie-break makes Add beat Remove for the same
                // path/version (DV update pattern: Remove(old dv) + Add(new dv) in one commit).
                let replay_idx = match input.schema().index_of(COL_REPLAY_PATH) {
                    Ok(i) => i,
                    Err(_) => return vec![None],
                };
                let version_idx = match input.schema().index_of(COL_LOG_VERSION) {
                    Ok(i) => i,
                    Err(_) => return vec![None],
                };
                let is_remove_idx = match input.schema().index_of(COL_LOG_IS_REMOVE) {
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
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new(COL_LOG_IS_REMOVE, is_remove_idx)),
                        options: SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    },
                ]) else {
                    return vec![None];
                };

                vec![Some(OrderingRequirements::from(ordering))]
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.mode {
            ReplayMode::Sort { input } => vec![input],
            ReplayMode::Hash {
                checkpoint,
                commits,
            } => vec![checkpoint, commits],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (&self.mode, children.len()) {
            (ReplayMode::Sort { .. }, 1) => Ok(Arc::new(Self::new(
                Arc::clone(&children[0]),
                self.table_url.clone(),
                self.version,
                self.partition_columns.clone(),
                self.checkpoint_files.clone(),
                self.commit_files.clone(),
            ))),
            (ReplayMode::Hash { .. }, 2) => Ok(Arc::new(Self::new_hash(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.table_url.clone(),
                self.version,
                self.partition_columns.clone(),
                self.checkpoint_files.clone(),
                self.commit_files.clone(),
            ))),
            (ReplayMode::Sort { .. }, _) => {
                internal_err!("DeltaLogReplayExec (sort) expects exactly one child")
            }
            (ReplayMode::Hash { .. }, _) => {
                internal_err!("DeltaLogReplayExec (hash) expects exactly two children")
            }
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_schema = self.schema();

        match &self.mode {
            ReplayMode::Sort { input } => {
                let input_stream = input.execute(partition, context)?;
                let partition_columns = self.partition_columns.clone();
                let state =
                    ReplayState::new(input_stream, Arc::clone(&output_schema), partition_columns);

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
            ReplayMode::Hash {
                checkpoint,
                commits,
            } => {
                let commits_stream = commits.execute(partition, Arc::clone(&context))?;
                let checkpoint_stream = checkpoint.execute(partition, context)?;

                let state = HashReplayState::new(
                    commits_stream,
                    checkpoint_stream,
                    Arc::clone(&output_schema),
                );

                let s = stream::try_unfold(state, |mut st| async move {
                    loop {
                        if st.out_rows >= OUTPUT_BATCH_ROWS {
                            let out = st.take_output_batch()?;
                            return Ok(Some((out, st)));
                        }

                        match st.stage {
                            HashReplayStage::Build => match st.commits.try_next().await? {
                                Some(batch) => {
                                    st.process_commits_batch(&batch)?;
                                    continue;
                                }
                                None => {
                                    st.stage = HashReplayStage::Probe;
                                    continue;
                                }
                            },
                            HashReplayStage::Probe => match st.checkpoint.try_next().await? {
                                Some(batch) => {
                                    st.process_checkpoint_batch(&batch)?;
                                    continue;
                                }
                                None => {
                                    st.stage = HashReplayStage::Emit;
                                    continue;
                                }
                            },
                            HashReplayStage::Emit => {
                                st.finalize_emit_rows();
                                let mut iter = match st.emit_rows.take() {
                                    Some(it) => it,
                                    None => {
                                        st.stage = HashReplayStage::Done;
                                        continue;
                                    }
                                };

                                while st.out_rows < OUTPUT_BATCH_ROWS {
                                    match iter.next() {
                                        Some(row) => {
                                            st.push_payload_row(row);
                                            continue;
                                        }
                                        None => break,
                                    }
                                }

                                let is_exhausted = iter.as_slice().is_empty();
                                st.emit_rows = Some(iter);
                                if is_exhausted {
                                    st.stage = HashReplayStage::Done;
                                    continue;
                                }
                                // If we produced rows, let the outer loop flush as needed.
                                continue;
                            }
                            HashReplayStage::Done => {
                                if st.out_rows > 0 {
                                    let out = st.take_output_batch()?;
                                    return Ok(Some((out, st)));
                                }
                                return Ok(None);
                            }
                        }
                    }
                });

                Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, s)))
            }
        }
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
                match &self.mode {
                    ReplayMode::Sort { .. } => writeln!(f, "mode=sort")?,
                    ReplayMode::Hash { .. } => writeln!(f, "mode=hash")?,
                }
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

    #[tokio::test]
    async fn hash_replay_checkpoint_hit_is_dropped() -> Result<()> {
        // Checkpoint: add a, add b (v0)
        // Commits: remove a (v1) -> should hide checkpoint a

        let add_fields: Fields = vec![
            Arc::new(Field::new("path", DataType::Utf8, true)),
            Arc::new(Field::new("size", DataType::Int64, true)),
        ]
        .into();

        let cp_add_path = Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef;
        let cp_add_size = Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let cp_add_struct = struct_array_with_validity(
            add_fields.clone(),
            vec![cp_add_path, cp_add_size],
            vec![true, true],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("add", cp_add_struct.data_type().clone(), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let checkpoint_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cp_add_struct) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
                Arc::new(Int64Array::from(vec![0, 0])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let commit_add_struct = struct_array_with_validity(
            add_fields,
            vec![
                Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            ],
            vec![false],
        );
        let commits_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(commit_add_struct) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let checkpoint_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(checkpoint_batch));
        let commits_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(commits_batch));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
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
    async fn hash_replay_checkpoint_miss_is_passed_through() -> Result<()> {
        // Checkpoint: add a
        // Commits: empty

        let add_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let add_struct = struct_array_with_validity(
            add_fields,
            vec![Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef],
            vec![true],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("add", add_struct.data_type().clone(), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let checkpoint_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(add_struct) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let empty_commits_batch = RecordBatch::new_empty(schema);
        let checkpoint_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(checkpoint_batch));
        let commits_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(empty_commits_batch));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
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

    #[tokio::test]
    async fn hash_replay_emits_commit_only_adds_after_checkpoint() -> Result<()> {
        // Checkpoint: add a
        // Commits: add c (newer)

        let add_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields.clone()), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let cp_add_struct = struct_array_with_validity(
            add_fields.clone(),
            vec![Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef],
            vec![true],
        );
        let checkpoint_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cp_add_struct) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let commit_add_struct = struct_array_with_validity(
            add_fields,
            vec![Arc::new(StringArray::from(vec![Some("c")])) as ArrayRef],
            vec![true],
        );
        let commits_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(commit_add_struct) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("c")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let checkpoint_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(checkpoint_batch));
        let commits_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(commits_batch));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
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
        assert_eq!(out.num_rows(), 2);

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
        let mut got = vec![path_col.value(0).to_string(), path_col.value(1).to_string()];
        got.sort();
        assert_eq!(got, vec!["a".to_string(), "c".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn hash_replay_tie_break_add_beats_remove_same_version() -> Result<()> {
        // Commits (same path/version), intentionally unordered:
        // - remove a (v1)
        // - add a (v1)
        // Add should still win even without commit-side ordering.

        let add_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields.clone()), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let checkpoint_batch = RecordBatch::new_empty(schema.clone());

        let add_struct = struct_array_with_validity(
            add_fields.clone(),
            vec![Arc::new(StringArray::from(vec![None, Some("a")])) as ArrayRef],
            vec![false, true],
        );
        let commits_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(add_struct) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("a")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let checkpoint_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(checkpoint_batch));
        let commits_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(commits_batch));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
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
        assert_eq!(out.num_rows(), 1);
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
        assert_eq!(path_col.value(0), "a");
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn hash_replay_unordered_same_version_remove_then_add_add_wins() -> Result<()> {
        let add_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields.clone()), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let checkpoint_batch = RecordBatch::new_empty(schema.clone());
        let commits_add = struct_array_with_validity(
            add_fields,
            vec![Arc::new(StringArray::from(vec![None, Some("a")])) as ArrayRef],
            vec![false, true],
        );
        let commits_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(commits_add) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("a")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let checkpoint_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(checkpoint_batch));
        let commits_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(commits_batch));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
            Url::parse("file:///tmp/delta").unwrap(),
            0,
            vec![],
            vec![],
            vec![],
        ));

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx)?;
        let out = stream.try_next().await?.unwrap();
        assert_eq!(out.num_rows(), 1);
        let add = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let path_col = add
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "a");
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn hash_replay_unordered_higher_version_overrides_lower() -> Result<()> {
        let add_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields.clone()), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let checkpoint_batch = RecordBatch::new_empty(schema.clone());
        let commits_add = struct_array_with_validity(
            add_fields,
            vec![Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef],
            vec![true, false],
        );
        let commits_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(commits_add) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("a")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let checkpoint_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(checkpoint_batch));
        let commits_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(commits_batch));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
            Url::parse("file:///tmp/delta").unwrap(),
            0,
            vec![],
            vec![],
            vec![],
        ));

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx)?;
        assert!(stream.try_next().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn hash_replay_unordered_lower_version_cannot_override() -> Result<()> {
        let add_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields.clone()), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let checkpoint_batch = RecordBatch::new_empty(schema.clone());
        let commits_add = struct_array_with_validity(
            add_fields,
            vec![Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef],
            vec![true, false],
        );
        let commits_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(commits_add) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("a")])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let checkpoint_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(checkpoint_batch));
        let commits_plan: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(commits_batch));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
            Url::parse("file:///tmp/delta").unwrap(),
            0,
            vec![],
            vec![],
            vec![],
        ));

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx)?;
        let out = stream.try_next().await?.unwrap();
        assert_eq!(out.num_rows(), 1);
        let add = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let path_col = add
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "a");
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn hash_replay_required_input_ordering_is_none_for_hash_mode() -> Result<()> {
        let add_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Struct(add_fields), true),
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new(COL_LOG_IS_REMOVE, DataType::Boolean, true),
            Field::new(COL_LOG_VERSION, DataType::Int64, false),
        ]));

        let checkpoint_plan: Arc<dyn ExecutionPlan> =
            Arc::new(OneBatchExec::new(RecordBatch::new_empty(schema.clone())));
        let commits_plan: Arc<dyn ExecutionPlan> =
            Arc::new(OneBatchExec::new(RecordBatch::new_empty(schema)));

        let exec = Arc::new(DeltaLogReplayExec::new_hash(
            checkpoint_plan,
            commits_plan,
            Url::parse("file:///tmp/delta").unwrap(),
            0,
            vec![],
            vec![],
            vec![],
        ));

        let ordering = exec.required_input_ordering();
        assert_eq!(ordering.len(), 2);
        assert!(ordering[0].is_none());
        assert!(ordering[1].is_none());
        Ok(())
    }
}
