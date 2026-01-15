use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Builder, MapArray, StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::{stream, TryStreamExt};
use url::Url;

use crate::datasource::PATH_COLUMN;

const COL_SIZE_BYTES: &str = "size_bytes";
const COL_MODIFICATION_TIME: &str = "modification_time";

const COL_ADD: &str = "add";
const COL_REMOVE: &str = "remove";
const COL_PATH: &str = "path";
const COL_PARTITION_VALUES_CAMEL: &str = "partitionValues";
const COL_PARTITION_VALUES_SNAKE: &str = "partition_values";
const COL_MODIFICATION_TIME_CAMEL: &str = "modificationTime";
const COL_MODIFICATION_TIME_SNAKE: &str = "modification_time";

#[derive(Debug, Clone)]
struct ActiveFile {
    size_bytes: i64,
    modification_time: i64,
    partition_values: Vec<Option<String>>,
}

/// A unary node that replays raw Delta log Add/Remove actions into the active file set.
///
/// Input: raw log rows (checkpoint parquet + commit json via DataSourceExec/Union)
/// Output: per-file metadata table (path/size/modification_time + partition columns)
///
/// Notes:
/// - We treat `remove(path)` as a tombstone for `path` and ignore later adds for the same path.
///   This matches Delta's normal behavior where data file paths are unique and not re-used.
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
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
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

        Ok(Some((
            path_s.clone(),
            ActiveFile {
                size_bytes,
                modification_time,
                partition_values: partitions,
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
        vec![Distribution::SinglePartition]
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
        if partition != 0 {
            return internal_err!("DeltaLogReplayExec can only be executed in a single partition");
        }

        let schema = self.schema();
        let input = Arc::clone(&self.input);
        let partition_columns = self.partition_columns.clone();

        let future = async move {
            let mut stream = input.execute(0, context)?;
            let mut active: HashMap<String, ActiveFile> = HashMap::new();
            let mut tombstones: HashSet<String> = HashSet::new();

            while let Some(batch) = stream.try_next().await? {
                if batch.num_rows() == 0 {
                    continue;
                }

                let add_struct = Self::get_struct_column(&batch, COL_ADD)?;
                let remove_struct = Self::get_struct_column(&batch, COL_REMOVE)?;

                for row in 0..batch.num_rows() {
                    if let Some(remove) = &remove_struct {
                        if let Some(rm) = Self::parse_remove_row(remove.as_ref(), row)? {
                            tombstones.insert(rm.clone());
                            active.remove(rm.as_str());
                        }
                    }

                    if let Some(add) = &add_struct {
                        if let Some((path, file)) =
                            Self::parse_add_row(add.as_ref(), row, &partition_columns)?
                        {
                            if !tombstones.contains(&path) {
                                active.insert(path, file);
                            }
                        }
                    }
                }
            }

            if active.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let mut path_builder = StringBuilder::new();
            let mut size_builder = Int64Builder::new();
            let mut mod_builder = Int64Builder::new();
            let mut part_builders: Vec<StringBuilder> = partition_columns
                .iter()
                .map(|_| StringBuilder::new())
                .collect();

            // Deterministic ordering for stable output/explain snapshots.
            let mut entries = active.into_iter().collect::<Vec<_>>();
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            for (path, f) in entries {
                path_builder.append_value(path);
                size_builder.append_value(f.size_bytes);
                mod_builder.append_value(f.modification_time);
                for (i, v) in f.partition_values.into_iter().enumerate() {
                    match v {
                        Some(s) => part_builders[i].append_value(s),
                        None => part_builders[i].append_null(),
                    }
                }
            }

            let mut cols: Vec<ArrayRef> = Vec::with_capacity(3 + part_builders.len());
            cols.push(Arc::new(path_builder.finish()) as ArrayRef);
            cols.push(Arc::new(size_builder.finish()) as ArrayRef);
            cols.push(Arc::new(mod_builder.finish()) as ArrayRef);
            for mut b in part_builders {
                cols.push(Arc::new(b.finish()) as ArrayRef);
            }

            RecordBatch::try_new(schema, cols)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
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
    use super::*;

    use datafusion::arrow::array::{Int64Array, NullBufferBuilder};
    use datafusion::arrow::datatypes::Fields;
    use futures::TryStreamExt;

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
            Field::new("add", add_struct.data_type().clone(), true),
            Field::new("remove", remove_struct.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
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
