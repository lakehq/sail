use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{internal_err, Column, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::TryStreamExt;
use url::Url;

use crate::datasource::{collect_physical_columns, PATH_COLUMN};
use crate::kernel::models::Add;
use crate::kernel::statistics::{ColumnCountStat, ColumnValueStat, Stats};

const COL_STATS_JSON: &str = "stats_json";

#[derive(Debug)]
pub struct DeltaDiscoveryExec {
    table_url: Url,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    table_schema: Option<SchemaRef>,
    version: i64,
    input: Arc<dyn ExecutionPlan>,
    input_partition_columns: Vec<String>,
    input_partition_scan: bool,
    cache: PlanProperties,
}

impl DeltaDiscoveryExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: Option<SchemaRef>,
        version: i64,
        partition_columns: Vec<String>,
        partition_scan: bool,
    ) -> Result<Self> {
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            "partition_scan",
            DataType::Boolean,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let output_partitions = input.output_partitioning().partition_count().max(1);
        let cache = Self::compute_properties(schema, output_partitions);
        Ok(Self {
            table_url,
            predicate,
            table_schema,
            version,
            input,
            input_partition_columns: partition_columns,
            input_partition_scan: partition_scan,
            cache,
        })
    }

    pub fn from_log_scan(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        version: i64,
        partition_columns: Vec<String>,
        partition_scan: bool,
    ) -> Result<Self> {
        Self::new(
            input,
            table_url,
            None,
            None,
            version,
            partition_columns,
            partition_scan,
        )
    }

    pub fn with_input(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        table_schema: Option<SchemaRef>,
        version: i64,
        partition_columns: Vec<String>,
        partition_scan: bool,
    ) -> Result<Self> {
        Self::new(
            input,
            table_url,
            predicate,
            table_schema,
            version,
            partition_columns,
            partition_scan,
        )
    }

    fn compute_properties(schema: SchemaRef, output_partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(output_partitions.max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Get the table URL
    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    /// Get the predicate
    pub fn predicate(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.predicate
    }

    /// Get the table schema
    pub fn table_schema(&self) -> &Option<SchemaRef> {
        &self.table_schema
    }

    /// Get the table version
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Get the upstream metadata input plan.
    pub fn input(&self) -> Arc<dyn ExecutionPlan> {
        Arc::clone(&self.input)
    }

    /// Get partition columns carried by the upstream metadata plan.
    pub fn input_partition_columns(&self) -> &[String] {
        &self.input_partition_columns
    }

    /// Whether the upstream metadata plan is already a partition-only scan.
    pub fn input_partition_scan(&self) -> bool {
        self.input_partition_scan
    }

    fn build_adds_from_meta_batch(
        batch: &RecordBatch,
        partition_columns: &[String],
    ) -> Result<Vec<Add>> {
        let path_arr = batch
            .column_by_name(PATH_COLUMN)
            .and_then(|c| {
                c.as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "DeltaDiscoveryExec input must have Utf8 column '{PATH_COLUMN}'"
                ))
            })?;

        let size_arr = batch.column_by_name("size_bytes").and_then(|c| {
            c.as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
        });
        let mod_time_arr = batch.column_by_name("modification_time").and_then(|c| {
            c.as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
        });

        let stats_arr = batch.column_by_name(COL_STATS_JSON).map(|c| {
            datafusion::arrow::compute::cast(c, &DataType::Utf8).unwrap_or_else(|_| c.clone())
        });
        let stats_arr = stats_arr.as_ref().and_then(|c| {
            c.as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
        });

        let part_arrays: Vec<(String, Arc<dyn Array>)> = partition_columns
            .iter()
            .filter_map(|name| {
                batch.column_by_name(name).map(|a| {
                    let a = datafusion::arrow::compute::cast(a, &DataType::Utf8)
                        .unwrap_or_else(|_| a.clone());
                    (name.clone(), a)
                })
            })
            .collect();

        let mut adds = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if path_arr.is_null(row) {
                return Err(DataFusionError::Plan(format!(
                    "DeltaDiscoveryExec input '{PATH_COLUMN}' cannot be null"
                )));
            }
            let path = path_arr.value(row);
            let size = size_arr.map(|a| a.value(row)).unwrap_or_default();
            let modification_time = mod_time_arr.map(|a| a.value(row)).unwrap_or_default();
            let stats = stats_arr.and_then(|a| {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row).to_string())
                }
            });

            let mut partition_values: HashMap<String, Option<String>> =
                HashMap::with_capacity(part_arrays.len());
            for (name, arr) in &part_arrays {
                let v = if arr.is_null(row) {
                    None
                } else if let Some(s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                {
                    Some(s.value(row).to_string())
                } else {
                    datafusion::arrow::util::display::array_value_to_string(arr.as_ref(), row).ok()
                };
                partition_values.insert(name.clone(), v);
            }

            adds.push(Add {
                path: path.to_string(),
                partition_values,
                size,
                modification_time,
                data_change: true,
                stats,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            });
        }
        Ok(adds)
    }

    fn prune_mask_for_meta_batch(
        batch: &RecordBatch,
        predicate: &Arc<dyn PhysicalExpr>,
        table_schema: &SchemaRef,
        partition_columns: &[String],
    ) -> Result<Vec<bool>> {
        let adds = Self::build_adds_from_meta_batch(batch, partition_columns)?;
        if adds.is_empty() {
            return Ok(vec![]);
        }
        let referenced = collect_physical_columns(predicate);
        let stats =
            DeltaAddStatsPruningStatistics::try_new(table_schema.clone(), adds, referenced)?;
        let pruning_predicate = datafusion::physical_optimizer::pruning::PruningPredicate::try_new(
            Arc::clone(predicate),
            table_schema.clone(),
        )?;
        pruning_predicate.prune(&stats)
    }
}

#[derive(Debug)]
struct DeltaAddStatsPruningStatistics {
    table_schema: SchemaRef,
    adds: Vec<Add>,
    stats: Vec<Option<Stats>>,
    referenced_columns: std::collections::HashSet<String>,
}

impl DeltaAddStatsPruningStatistics {
    fn try_new(
        table_schema: SchemaRef,
        adds: Vec<Add>,
        referenced_columns: std::collections::HashSet<String>,
    ) -> Result<Self> {
        let mut stats = Vec::with_capacity(adds.len());
        for a in &adds {
            let parsed = a
                .get_stats()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            stats.push(parsed);
        }
        Ok(Self {
            table_schema,
            adds,
            stats,
            referenced_columns,
        })
    }

    fn field_for(&self, column: &Column) -> Option<Arc<Field>> {
        let name = column.name();
        self.table_schema
            .field_with_name(name)
            .ok()
            .cloned()
            .map(Arc::new)
    }

    fn null_scalar(dt: &DataType) -> ScalarValue {
        ScalarValue::try_from(dt).unwrap_or_else(|_| ScalarValue::Null)
    }

    fn scalar_from_json(dt: &DataType, v: &serde_json::Value) -> Option<ScalarValue> {
        match v {
            serde_json::Value::Null => Some(Self::null_scalar(dt)),
            serde_json::Value::Bool(b) => ScalarValue::try_from_string(b.to_string(), dt).ok(),
            serde_json::Value::Number(n) => ScalarValue::try_from_string(n.to_string(), dt).ok(),
            serde_json::Value::String(s) => ScalarValue::try_from_string(s.clone(), dt).ok(),
            other => ScalarValue::try_from_string(other.to_string(), dt).ok(),
        }
    }

    fn scalar_from_partition_value(dt: &DataType, v: &Option<String>) -> ScalarValue {
        match v {
            None => Self::null_scalar(dt),
            Some(s) => ScalarValue::try_from_string(s.clone(), dt).unwrap_or_else(|_| {
                // If we can't parse the partition value into the target type, treat it as unknown.
                Self::null_scalar(dt)
            }),
        }
    }

    fn lookup_value_stat<'a>(
        map: &'a HashMap<String, ColumnValueStat>,
        name: &str,
    ) -> Option<&'a serde_json::Value> {
        let mut parts = name.split('.');
        let first = parts.next()?;
        let mut cur = map.get(first)?;
        for p in parts {
            cur = cur.as_column()?.get(p)?;
        }
        cur.as_value()
    }

    fn lookup_count_stat<'a>(map: &'a HashMap<String, ColumnCountStat>, name: &str) -> Option<i64> {
        let mut parts = name.split('.');
        let first = parts.next()?;
        let mut cur = map.get(first)?;
        for p in parts {
            cur = cur.as_column()?.get(p)?;
        }
        cur.as_value()
    }

    fn build_array(
        &self,
        column: &Column,
        f: impl Fn(&Add, Option<&Stats>, &DataType) -> ScalarValue,
    ) -> Option<ArrayRef> {
        let field = self.field_for(column)?;
        let dt = field.data_type();

        // Only compute arrays for columns that are actually referenced by the predicate. This
        // reduces repeated stats parsing work in `PruningPredicate`.
        if !self.referenced_columns.contains(field.name()) {
            return None;
        }

        let mut has_value = false;
        let mut scalars = Vec::with_capacity(self.adds.len());
        for (a, s) in self.adds.iter().zip(self.stats.iter()) {
            let sv = f(a, s.as_ref(), dt);
            has_value |= !sv.is_null();
            scalars.push(sv);
        }

        if !has_value {
            return None;
        }
        ScalarValue::iter_to_array(scalars.into_iter()).ok()
    }
}

impl PruningStatistics for DeltaAddStatsPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.build_array(column, |a, s, dt| {
            let name = column.name();
            if let Some(pv) = a.partition_values.get(name) {
                return Self::scalar_from_partition_value(dt, pv);
            }
            if let Some(s) = s {
                if let Some(v) = Self::lookup_value_stat(&s.min_values, name) {
                    return Self::scalar_from_json(dt, v).unwrap_or_else(|| Self::null_scalar(dt));
                }
            }
            Self::null_scalar(dt)
        })
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.build_array(column, |a, s, dt| {
            let name = column.name();
            if let Some(pv) = a.partition_values.get(name) {
                return Self::scalar_from_partition_value(dt, pv);
            }
            if let Some(s) = s {
                if let Some(v) = Self::lookup_value_stat(&s.max_values, name) {
                    return Self::scalar_from_json(dt, v).unwrap_or_else(|| Self::null_scalar(dt));
                }
            }
            Self::null_scalar(dt)
        })
    }

    fn num_containers(&self) -> usize {
        self.adds.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.build_array(column, |a, s, _dt| {
            let name = column.name();
            // Partition columns: all rows in file share same partition value.
            if let Some(pv) = a.partition_values.get(name) {
                let n = s.map(|s| s.num_records).unwrap_or(0);
                let cnt = if pv.is_none() { n } else { 0 };
                return ScalarValue::UInt64(Some(cnt.max(0) as u64));
            }
            if let Some(s) = s {
                if let Some(v) = Self::lookup_count_stat(&s.null_count, name) {
                    return ScalarValue::UInt64(Some(v.max(0) as u64));
                }
            }
            ScalarValue::UInt64(None)
        })
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.build_array(column, |_a, s, _dt| {
            let Some(s) = s else {
                return ScalarValue::UInt64(None);
            };
            ScalarValue::UInt64(Some(s.num_records.max(0) as u64))
        })
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

#[async_trait]
impl ExecutionPlan for DeltaDiscoveryExec {
    fn name(&self) -> &'static str {
        "DeltaDiscoveryExec"
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if _children.len() != 1 {
            return internal_err!(
                "DeltaDiscoveryExec requires exactly one child when used as a unary node"
            );
        }
        let mut cloned = (*self).clone();
        cloned.input = _children[0].clone();
        Ok(Arc::new(cloned))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input_stream = self.input.execute(partition, context)?;
        let schema_for_stream = schema.clone();
        let predicate_for_stream = self.predicate.clone();
        let table_schema_for_stream = self.table_schema.clone();
        let partition_columns_for_stream = self.input_partition_columns.clone();
        let partition_scan = self.input_partition_scan;

        let s = input_stream.try_filter_map(move |batch| {
            let schema = schema_for_stream.clone();
            let predicate = predicate_for_stream.clone();
            let table_schema = table_schema_for_stream.clone();
            let partition_columns = partition_columns_for_stream.clone();
            async move {
                if batch.num_rows() == 0 {
                    return Ok(None);
                }

                let filtered = match (&predicate, &table_schema) {
                    (Some(pred), Some(ts)) => {
                        let mask = DeltaDiscoveryExec::prune_mask_for_meta_batch(
                            &batch,
                            pred,
                            ts,
                            &partition_columns,
                        )?;
                        if mask.is_empty() {
                            batch.clone()
                        } else {
                            let b = BooleanArray::from(mask);
                            filter_record_batch(&batch, &b)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
                        }
                    }
                    _ => batch.clone(),
                };

                let scan_array = Arc::new(BooleanArray::from(vec![
                    partition_scan;
                    filtered.num_rows()
                ]));
                let mut cols = filtered.columns().to_vec();
                cols.push(scan_array);
                let out = RecordBatch::try_new(schema, cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Ok(Some(out))
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

impl DisplayAs for DeltaDiscoveryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaDiscoveryExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

impl Clone for DeltaDiscoveryExec {
    fn clone(&self) -> Self {
        Self {
            table_url: self.table_url.clone(),
            predicate: self.predicate.clone(),
            table_schema: self.table_schema.clone(),
            version: self.version,
            input: Arc::clone(&self.input),
            input_partition_columns: self.input_partition_columns.clone(),
            input_partition_scan: self.input_partition_scan,
            cache: self.cache.clone(),
        }
    }
}
