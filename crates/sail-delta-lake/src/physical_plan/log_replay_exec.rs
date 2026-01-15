use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Builder, MapArray, StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::compute::{cast, SortOptions};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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

use crate::datasource::PATH_COLUMN;
use crate::physical_plan::COL_REPLAY_PATH;

const COL_SIZE_BYTES: &str = "size_bytes";
const COL_MODIFICATION_TIME: &str = "modification_time";
const COL_STATS_JSON: &str = "stats_json";

const COL_ADD: &str = "add";
const COL_REMOVE: &str = "remove";
const COL_PATH: &str = "path";
const COL_STATS: &str = "stats";
const COL_STATS_JSON_IN: &str = "stats_json";
const COL_PARTITION_VALUES_CAMEL: &str = "partitionValues";
const COL_PARTITION_VALUES_SNAKE: &str = "partition_values";
const COL_MODIFICATION_TIME_CAMEL: &str = "modificationTime";
const COL_MODIFICATION_TIME_SNAKE: &str = "modification_time";

#[derive(Debug, Clone)]
struct ActiveFile {
    size_bytes: i64,
    modification_time: i64,
    partition_values: Vec<Option<String>>,
    stats_json: Option<String>,
}

const OUTPUT_BATCH_ROWS: usize = 8192;

/// A unary node that replays raw Delta log Add/Remove actions into the active file set.
///
/// Input: raw log rows (checkpoint parquet + commit json via DataSourceExec/Union)
/// Output: per-file metadata table (path/size/modification_time + partition columns)
///
/// Notes:
/// - We treat `remove(path)` as a tombstone for `path` and ignore any `add` for the same path.
///   This matches the (common) Delta invariant that data file paths are unique and not re-used.
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
        let schema = Self::output_schema(&partition_columns);
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

    fn output_schema(partition_columns: &[String]) -> SchemaRef {
        // Partition values are typed by the kernel evaluator, so we mark them nullable here.
        let mut fields = vec![
            Arc::new(Field::new(PATH_COLUMN, DataType::Utf8, false)),
            Arc::new(Field::new(COL_SIZE_BYTES, DataType::Int64, false)),
            Arc::new(Field::new(COL_MODIFICATION_TIME, DataType::Int64, false)),
        ];
        for col in partition_columns {
            fields.push(Arc::new(Field::new(col, DataType::Utf8, true)));
        }
        fields.push(Arc::new(Field::new(COL_STATS_JSON, DataType::Utf8, true)));
        Arc::new(Schema::new(fields))
    }

    fn struct_field<'a>(s: &'a StructArray, name: &str) -> Option<&'a ArrayRef> {
        s.column_by_name(name)
    }

    fn get_struct_column(batch: &RecordBatch, col: &str) -> Result<Option<Arc<StructArray>>> {
        let Some(arr) = batch.column_by_name(col) else {
            return Ok(None);
        };
        let s = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DeltaLogReplayExec input column '{col}' must be a Struct"
            ))
        })?;
        Ok(Some(Arc::new(s.clone())))
    }

    fn as_string_array(a: &ArrayRef, name: &str) -> Result<Arc<StringArray>> {
        let casted = cast(a.as_ref(), &DataType::Utf8)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let s = casted
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!("DeltaLogReplayExec '{name}' must be Utf8"))
            })?;
        Ok(Arc::new(s.clone()))
    }

    fn as_i64_array(a: &ArrayRef, name: &str) -> Result<Arc<datafusion::arrow::array::Int64Array>> {
        let casted = cast(a.as_ref(), &DataType::Int64)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let n = casted
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!("DeltaLogReplayExec '{name}' must be Int64"))
            })?;
        Ok(Arc::new(n.clone()))
    }

    fn map_lookup_string_value(map: &MapArray, row: usize, key: &str) -> Result<Option<String>> {
        if map.is_null(row) {
            return Ok(None);
        }
        let entries = map.entries();
        let entries = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("partitionValues map entries must be Struct".into())
            })?;
        let keys = entries
            .column_by_name("keys")
            .or_else(|| entries.column_by_name("key"))
            .ok_or_else(|| DataFusionError::Plan("partitionValues map missing keys".into()))?;
        let values = entries
            .column_by_name("values")
            .or_else(|| entries.column_by_name("value"))
            .ok_or_else(|| DataFusionError::Plan("partitionValues map missing values".into()))?;
        let keys = keys
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("partitionValues keys must be Utf8".into()))?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("partitionValues values must be Utf8".into()))?;

        let start = map.value_offsets()[row] as usize;
        let end = map.value_offsets()[row + 1] as usize;
        for i in start..end {
            if keys.is_null(i) {
                continue;
            }
            if keys.value(i) == key {
                if values.is_null(i) {
                    return Ok(None);
                }
                return Ok(Some(values.value(i).to_string()));
            }
        }
        Ok(None)
    }

    fn parse_add_row(
        add: &StructArray,
        row: usize,
        partition_columns: &[String],
    ) -> Result<Option<(String, ActiveFile)>> {
        if add.is_null(row) {
            return Ok(None);
        }

        let path = Self::struct_field(add, COL_PATH).ok_or_else(|| {
            DataFusionError::Plan("DeltaLogReplayExec add missing 'path'".to_string())
        })?;
        let path = Self::as_string_array(path, "add.path")?;
        if path.is_null(row) {
            return Err(DataFusionError::Plan(
                "DeltaLogReplayExec add.path cannot be null".to_string(),
            ));
        }
        let path_s = path.value(row).to_string();

        let size = Self::struct_field(add, "size").ok_or_else(|| {
            DataFusionError::Plan("DeltaLogReplayExec add missing 'size'".to_string())
        })?;
        let size = Self::as_i64_array(size, "add.size")?;
        let size_bytes = if size.is_null(row) {
            0
        } else {
            size.value(row)
        };

        let mod_time = Self::struct_field(add, COL_MODIFICATION_TIME_CAMEL)
            .or_else(|| Self::struct_field(add, COL_MODIFICATION_TIME_SNAKE))
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "DeltaLogReplayExec add missing modification time".to_string(),
                )
            })?;
        let mod_time = Self::as_i64_array(mod_time, "add.modificationTime")?;
        let modification_time = if mod_time.is_null(row) {
            0
        } else {
            mod_time.value(row)
        };

        let part_values = Self::struct_field(add, COL_PARTITION_VALUES_CAMEL)
            .or_else(|| Self::struct_field(add, COL_PARTITION_VALUES_SNAKE))
            .and_then(|a| a.as_any().downcast_ref::<MapArray>().cloned());

        let mut partitions = Vec::with_capacity(partition_columns.len());
        for col in partition_columns {
            if let Some(map) = &part_values {
                partitions.push(Self::map_lookup_string_value(map, row, col)?);
            } else {
                partitions.push(None);
            }
        }

        let stats = Self::struct_field(add, COL_STATS)
            .or_else(|| Self::struct_field(add, COL_STATS_JSON_IN))
            .and_then(|a| Self::as_string_array(a, "add.stats").ok());
        let stats_json = stats.and_then(|s| {
            if s.is_null(row) {
                None
            } else {
                Some(s.value(row).to_string())
            }
        });

        Ok(Some((
            path_s.clone(),
            ActiveFile {
                size_bytes,
                modification_time,
                partition_values: partitions,
                stats_json,
            },
        )))
    }

    fn parse_remove_row(remove: &StructArray, row: usize) -> Result<Option<String>> {
        if remove.is_null(row) {
            return Ok(None);
        }
        let path = Self::struct_field(remove, COL_PATH).ok_or_else(|| {
            DataFusionError::Plan("DeltaLogReplayExec remove missing 'path'".to_string())
        })?;
        let path = Self::as_string_array(path, "remove.path")?;
        if path.is_null(row) {
            return Err(DataFusionError::Plan(
                "DeltaLogReplayExec remove.path cannot be null".to_string(),
            ));
        }
        Ok(Some(path.value(row).to_string()))
    }
}

struct ReplayState {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    partition_columns: Vec<String>,

    // current group state (per replay_path)
    current_path: Option<String>,
    current_removed: bool,
    current_file: Option<ActiveFile>,

    // output builders for the next RecordBatch
    out_rows: usize,
    out_path: StringBuilder,
    out_size: Int64Builder,
    out_mod_time: Int64Builder,
    out_parts: Vec<StringBuilder>,
    out_stats: StringBuilder,
    finished: bool,
}

impl ReplayState {
    fn new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> Self {
        let out_parts = partition_columns
            .iter()
            .map(|_| StringBuilder::new())
            .collect();
        Self {
            input,
            output_schema,
            partition_columns,
            current_path: None,
            current_removed: false,
            current_file: None,
            out_rows: 0,
            out_path: StringBuilder::new(),
            out_size: Int64Builder::new(),
            out_mod_time: Int64Builder::new(),
            out_parts,
            out_stats: StringBuilder::new(),
            finished: false,
        }
    }

    fn flush_current_group(&mut self) {
        if let (Some(path), false, Some(file)) = (
            self.current_path.take(),
            self.current_removed,
            self.current_file.take(),
        ) {
            self.append_output_row(path, file);
        } else {
            self.current_path.take();
            self.current_file.take();
        }
        self.current_removed = false;
    }

    fn append_output_row(&mut self, path: String, file: ActiveFile) {
        self.out_path.append_value(path);
        self.out_size.append_value(file.size_bytes);
        self.out_mod_time.append_value(file.modification_time);
        for (i, v) in file.partition_values.into_iter().enumerate() {
            match v {
                Some(s) => self.out_parts[i].append_value(s),
                None => self.out_parts[i].append_null(),
            }
        }
        match file.stats_json {
            Some(s) => self.out_stats.append_value(s),
            None => self.out_stats.append_null(),
        }
        self.out_rows += 1;
    }

    fn take_output_batch(&mut self) -> Result<RecordBatch> {
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(4 + self.out_parts.len());
        cols.push(Arc::new(self.out_path.finish()) as ArrayRef);
        cols.push(Arc::new(self.out_size.finish()) as ArrayRef);
        cols.push(Arc::new(self.out_mod_time.finish()) as ArrayRef);
        for b in self.out_parts.iter_mut() {
            cols.push(Arc::new(b.finish()) as ArrayRef);
        }
        cols.push(Arc::new(self.out_stats.finish()) as ArrayRef);

        self.out_rows = 0;
        Ok(RecordBatch::try_new(Arc::clone(&self.output_schema), cols)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?)
    }

    fn process_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // The planner is expected to place `DeltaLogPathExtractExec` under this node to
        // materialize `COL_REPLAY_PATH` for distribution/sorting.
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

        let add_struct = DeltaLogReplayExec::get_struct_column(batch, COL_ADD)?;
        let remove_struct = DeltaLogReplayExec::get_struct_column(batch, COL_REMOVE)?;

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

            // Apply remove first (tombstone).
            if let Some(remove) = &remove_struct {
                if let Some(_rm) = DeltaLogReplayExec::parse_remove_row(remove.as_ref(), row)? {
                    self.current_removed = true;
                    self.current_file = None;
                }
            }

            if self.current_removed {
                continue;
            }

            if let Some(add) = &add_struct {
                if let Some((_path, file)) =
                    DeltaLogReplayExec::parse_add_row(add.as_ref(), row, &self.partition_columns)?
                {
                    // Under the path uniqueness assumption, the first add we see is sufficient.
                    if self.current_file.is_none() {
                        self.current_file = Some(file);
                    }
                }
            }
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
        // `COL_REPLAY_PATH` column, which is expected to be produced by `DeltaLogPathExtractExec`.
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
        // `COL_REPLAY_PATH`.
        let idx = match self.input.schema().index_of(COL_REPLAY_PATH) {
            Ok(i) => i,
            Err(_) => return vec![None],
        };

        let ordering = LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new(COL_REPLAY_PATH, idx)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }])
        .expect("non-degenerate ordering");

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
    use datafusion::arrow::array::{Int64Array, NullBufferBuilder};
    use datafusion::arrow::datatypes::Fields;
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
        // Row0: add a
        // Row1: remove a
        // Row2: add b

        let add_fields: Fields = vec![
            Arc::new(Field::new("path", DataType::Utf8, true)),
            Arc::new(Field::new("size", DataType::Int64, true)),
            Arc::new(Field::new("modificationTime", DataType::Int64, true)),
        ]
        .into();

        let add_path = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef;
        let add_size = Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])) as ArrayRef;
        let add_mod = Arc::new(Int64Array::from(vec![Some(10), None, Some(20)])) as ArrayRef;
        let add_struct = struct_array_with_validity(
            add_fields,
            vec![add_path, add_size, add_mod],
            vec![true, false, true],
        );

        let remove_fields: Fields = vec![Arc::new(Field::new("path", DataType::Utf8, true))].into();
        let rm_path = Arc::new(StringArray::from(vec![None, Some("a"), None])) as ArrayRef;
        let remove_struct =
            struct_array_with_validity(remove_fields, vec![rm_path], vec![false, true, false]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(COL_REPLAY_PATH, DataType::Utf8, false),
            Field::new("add", add_struct.data_type().clone(), true),
            Field::new("remove", remove_struct.data_type().clone(), true),
        ]));
        let replay_path =
            Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("b")])) as ArrayRef;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                replay_path,
                Arc::new(add_struct) as ArrayRef,
                Arc::new(remove_struct) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let input: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(batch));
        let exec = Arc::new(DeltaLogReplayExec::new(
            input,
            Url::parse("file:///tmp/delta").unwrap(),
            0,
            vec![],
            vec![],
            vec![],
        ));

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx)?;
        let out = stream.try_next().await?.unwrap();

        let path_col = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(path_col.value(0), "b");
        Ok(())
    }
}
