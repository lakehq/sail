use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, MapArray, StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::TryStreamExt;
use url::Url;

use crate::datasource::PATH_COLUMN;

const COL_PATH: &str = "path";
const COL_SIZE_BYTES: &str = "size_bytes";
const COL_MODIFICATION_TIME: &str = "modification_time";
const COL_ADD: &str = "add";
const COL_PARTITION_VALUES_CAMEL: &str = "partitionValues";
const COL_PARTITION_VALUES_SNAKE: &str = "partition_values";
const COL_MODIFICATION_TIME_CAMEL: &str = "modificationTime";
const COL_MODIFICATION_TIME_SNAKE: &str = "modification_time";

/// A unary node that transforms raw log actions into a per-file metadata table.
#[derive(Debug, Clone)]
pub struct DeltaLogScanExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    version: i64,
    partition_columns: Vec<String>,
    // purely for observability (EXPLAIN); populated by the planner when available
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    cache: PlanProperties,
}

impl DeltaLogScanExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        version: i64,
        partition_columns: Vec<String>,
        checkpoint_files: Vec<String>,
        commit_files: Vec<String>,
    ) -> Self {
        let schema = Self::output_schema(&partition_columns);
        let output_partitions = input.output_partitioning().partition_count();
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
        Arc::new(Schema::new(fields))
    }

    fn struct_field<'a>(s: &'a StructArray, name: &str) -> Option<&'a ArrayRef> {
        s.column_by_name(name)
    }

    fn get_add_struct(batch: &RecordBatch) -> Result<Arc<StructArray>> {
        let add = batch.column_by_name(COL_ADD).ok_or_else(|| {
            DataFusionError::Plan("DeltaLogScanExec input missing 'add' column".to_string())
        })?;
        let add = add.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            DataFusionError::Plan(
                "DeltaLogScanExec input column 'add' must be a Struct".to_string(),
            )
        })?;
        Ok(Arc::new(add.clone()))
    }

    fn map_lookup_string_values(map: &MapArray, key: &str) -> Result<StringArray> {
        // MapArray entries are StructArray with (key, value)
        let entries = map.entries();
        let entries = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("partitionValues map entries must be Struct".to_string())
            })?;
        let keys = entries
            .column_by_name("keys")
            .or_else(|| entries.column_by_name("key"))
            .ok_or_else(|| DataFusionError::Plan("partitionValues map missing keys".to_string()))?;
        let values = entries
            .column_by_name("values")
            .or_else(|| entries.column_by_name("value"))
            .ok_or_else(|| {
                DataFusionError::Plan("partitionValues map missing values".to_string())
            })?;
        let keys = keys.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Plan("partitionValues keys must be Utf8".to_string())
        })?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("partitionValues values must be Utf8".to_string())
            })?;

        let mut b = StringBuilder::new();
        for row in 0..map.len() {
            if map.is_null(row) {
                b.append_null();
                continue;
            }
            let start = map.value_offsets()[row] as usize;
            let end = map.value_offsets()[row + 1] as usize;
            let mut found: Option<&str> = None;
            for i in start..end {
                if keys.is_null(i) {
                    continue;
                }
                if keys.value(i) == key {
                    if values.is_null(i) {
                        found = None;
                    } else {
                        found = Some(values.value(i));
                    }
                    break;
                }
            }
            if let Some(v) = found {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        Ok(b.finish())
    }

    fn to_file_rows(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let add = Self::get_add_struct(batch)?;

        // Filter only rows where add is present (non-null)
        let mask = BooleanArray::from_iter((0..add.len()).map(|i| Some(!add.is_null(i))));
        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        if filtered.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(self.schema()));
        }

        let add = Self::get_add_struct(&filtered)?;
        let path = Self::struct_field(&add, COL_PATH).ok_or_else(|| {
            DataFusionError::Plan("DeltaLogScanExec add missing 'path'".to_string())
        })?;
        let path = cast(path.as_ref(), &DataType::Utf8)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let size = Self::struct_field(&add, "size").ok_or_else(|| {
            DataFusionError::Plan("DeltaLogScanExec add missing 'size'".to_string())
        })?;
        let size = cast(size.as_ref(), &DataType::Int64)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mod_time = Self::struct_field(&add, COL_MODIFICATION_TIME_CAMEL)
            .or_else(|| Self::struct_field(&add, COL_MODIFICATION_TIME_SNAKE))
            .ok_or_else(|| {
                DataFusionError::Plan("DeltaLogScanExec add missing modification time".to_string())
            })?;
        let mod_time = cast(mod_time.as_ref(), &DataType::Int64)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mut arrays: Vec<ArrayRef> = Vec::new();
        arrays.push(path);
        arrays.push(size);
        arrays.push(mod_time);

        // partition values (map) -> per partition column strings
        let part_values = Self::struct_field(&add, COL_PARTITION_VALUES_CAMEL)
            .or_else(|| Self::struct_field(&add, COL_PARTITION_VALUES_SNAKE));
        let part_values = match part_values {
            Some(a) => {
                let a = a.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    DataFusionError::Plan("partitionValues must be a Map".to_string())
                })?;
                Some(a.clone())
            }
            None => None,
        };

        for col in &self.partition_columns {
            if let Some(map) = &part_values {
                let arr = Self::map_lookup_string_values(map, col)?;
                arrays.push(Arc::new(arr) as ArrayRef);
            } else {
                let mut b = StringBuilder::new();
                for _ in 0..add.len() {
                    b.append_null();
                }
                arrays.push(Arc::new(b.finish()) as ArrayRef);
            }
        }

        let out_schema = Self::output_schema(&self.partition_columns);
        RecordBatch::try_new(out_schema, arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[async_trait]
impl ExecutionPlan for DeltaLogScanExec {
    fn name(&self) -> &'static str {
        "DeltaLogScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaLogScanExec expects exactly one child");
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
        let schema = self.schema();
        let input_stream = self.input.execute(partition, context)?;
        let this = self.clone();
        let s = input_stream.try_filter_map(move |batch| {
            let this = this.clone();
            async move {
                if batch.num_rows() == 0 {
                    return Ok(None);
                }

                let out = this.to_file_rows(&batch)?;
                if out.num_rows() == 0 {
                    return Ok(None);
                }

                Ok(Some(out))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

impl DisplayAs for DeltaLogScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaLogScanExec(table_path={}, version={})",
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
