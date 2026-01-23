use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::{stream, TryStreamExt};

use crate::kernel::statistics::{ColumnValueStat, Stats};

const COL_STATS_JSON: &str = "stats_json";

#[derive(Debug, Clone)]
pub enum MetadataAgg {
    Count,
    Min(String),
    Max(String),
}

#[derive(Debug, Clone)]
struct MetadataAggSpec {
    kind: MetadataAgg,
    name: String,
    data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct DeltaMetadataAggExec {
    input: Arc<dyn ExecutionPlan>,
    table_schema: SchemaRef,
    specs: Vec<MetadataAggSpec>,
    allow_missing_stats: bool,
    output_schema: SchemaRef,
    cache: PlanProperties,
}

impl DeltaMetadataAggExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        table_schema: SchemaRef,
        aggs: Vec<MetadataAgg>,
    ) -> Result<Self> {
        Self::try_new_with_options(input, table_schema, aggs, false)
    }

    pub fn try_new_with_options(
        input: Arc<dyn ExecutionPlan>,
        table_schema: SchemaRef,
        aggs: Vec<MetadataAgg>,
        allow_missing_stats: bool,
    ) -> Result<Self> {
        let mut specs = Vec::with_capacity(aggs.len());
        for agg in aggs {
            let (name, data_type) = match &agg {
                MetadataAgg::Count => ("count".to_string(), DataType::Int64),
                MetadataAgg::Min(col) => {
                    let field = table_schema.field_with_name(col).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "metadata agg column '{col}' not found in table schema"
                        ))
                    })?;
                    (format!("min_{col}"), field.data_type().clone())
                }
                MetadataAgg::Max(col) => {
                    let field = table_schema.field_with_name(col).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "metadata agg column '{col}' not found in table schema"
                        ))
                    })?;
                    (format!("max_{col}"), field.data_type().clone())
                }
            };
            specs.push(MetadataAggSpec {
                kind: agg,
                name,
                data_type,
            });
        }

        let fields = specs
            .iter()
            .map(|spec| Field::new(spec.name.clone(), spec.data_type.clone(), true))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        let output_schema = Arc::clone(&schema);

        Ok(Self {
            input,
            table_schema,
            specs,
            allow_missing_stats,
            output_schema,
            cache,
        })
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

#[async_trait]
impl ExecutionPlan for DeltaMetadataAggExec {
    fn name(&self) -> &'static str {
        "DeltaMetadataAggExec"
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
            return internal_err!("DeltaMetadataAggExec expects exactly one child");
        }
        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            table_schema: Arc::clone(&self.table_schema),
            specs: self.specs.clone(),
            allow_missing_stats: self.allow_missing_stats,
            output_schema: Arc::clone(&self.output_schema),
            cache: self.cache.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaMetadataAggExec only supports partition 0");
        }
        let input_stream = self.input.execute(partition, context)?;
        let schema = self.output_schema();
        let specs = self.specs.clone();
        let allow_missing_stats = self.allow_missing_stats;

        let s = stream::once(async move {
            let mut counts: Vec<i64> = vec![0; specs.len()];
            let mut mins: Vec<Option<ScalarValue>> = vec![None; specs.len()];
            let mut maxs: Vec<Option<ScalarValue>> = vec![None; specs.len()];
            let mut missing_stats = false;

            let mut input = input_stream;
            while let Some(batch) = input.try_next().await? {
                if batch.num_rows() == 0 {
                    continue;
                }
                let stats_col = batch.column_by_name(COL_STATS_JSON).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "metadata agg requires '{COL_STATS_JSON}' column"
                    ))
                })?;
                let stats_col = cast(stats_col.as_ref(), &DataType::Utf8)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                let stats_arr = stats_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "metadata agg '{COL_STATS_JSON}' must be Utf8"
                        ))
                    })?;

                for row in 0..batch.num_rows() {
                    if stats_arr.is_null(row) {
                        missing_stats = true;
                        continue;
                    }
                    let stats = Stats::from_json_str(stats_arr.value(row))
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    for (idx, spec) in specs.iter().enumerate() {
                        match &spec.kind {
                            MetadataAgg::Count => {
                                if stats.num_records > 0 {
                                    counts[idx] = counts[idx].saturating_add(stats.num_records);
                                }
                            }
                            MetadataAgg::Min(col) => {
                                if let Some(value) = lookup_value_stat(&stats.min_values, col)
                                    .and_then(|v| scalar_from_json(&spec.data_type, v))
                                {
                                    mins[idx] = match &mins[idx] {
                                        None => Some(value),
                                        Some(cur) => match value.partial_cmp(cur) {
                                            Some(Ordering::Less) => Some(value),
                                            _ => Some(cur.clone()),
                                        },
                                    };
                                }
                            }
                            MetadataAgg::Max(col) => {
                                if let Some(value) = lookup_value_stat(&stats.max_values, col)
                                    .and_then(|v| scalar_from_json(&spec.data_type, v))
                                {
                                    maxs[idx] = match &maxs[idx] {
                                        None => Some(value),
                                        Some(cur) => match value.partial_cmp(cur) {
                                            Some(Ordering::Greater) => Some(value),
                                            _ => Some(cur.clone()),
                                        },
                                    };
                                }
                            }
                        }
                    }
                }
            }

            if missing_stats && !allow_missing_stats {
                return Err(DataFusionError::Plan(
                    "metadata agg requires stats_json for all files".to_string(),
                ));
            }

            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(specs.len());
            for (idx, spec) in specs.iter().enumerate() {
                let value = match &spec.kind {
                    MetadataAgg::Count => ScalarValue::Int64(Some(counts[idx])),
                    MetadataAgg::Min(_) => mins[idx]
                        .clone()
                        .or_else(|| ScalarValue::try_from(&spec.data_type).ok())
                        .unwrap_or(ScalarValue::Null),
                    MetadataAgg::Max(_) => maxs[idx]
                        .clone()
                        .or_else(|| ScalarValue::try_from(&spec.data_type).ok())
                        .unwrap_or(ScalarValue::Null),
                };
                arrays.push(value.to_array_of_size(1)?);
            }

            Ok(RecordBatch::try_new(schema, arrays)?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema(),
            s,
        )))
    }
}

impl DisplayAs for DeltaMetadataAggExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaMetadataAggExec")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaMetadataAggExec")
            }
        }
    }
}

fn lookup_value_stat<'a>(
    map: &'a std::collections::HashMap<String, ColumnValueStat>,
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

fn scalar_from_json(dt: &DataType, v: &serde_json::Value) -> Option<ScalarValue> {
    match v {
        serde_json::Value::Null => Some(ScalarValue::try_from(dt).unwrap_or(ScalarValue::Null)),
        serde_json::Value::Bool(b) => ScalarValue::try_from_string(b.to_string(), dt).ok(),
        serde_json::Value::Number(n) => ScalarValue::try_from_string(n.to_string(), dt).ok(),
        serde_json::Value::String(s) => ScalarValue::try_from_string(s.clone(), dt).ok(),
        other => ScalarValue::try_from_string(other.to_string(), dt).ok(),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;

    use super::*;
    use crate::kernel::statistics::{ColumnCountStat, ColumnValueStat, Stats};

    fn stats_json(num_records: i64, min: i64, max: i64) -> String {
        let stats = Stats {
            num_records,
            min_values: std::collections::HashMap::from([(
                "id".to_string(),
                ColumnValueStat::Value(serde_json::Value::Number(min.into())),
            )]),
            max_values: std::collections::HashMap::from([(
                "id".to_string(),
                ColumnValueStat::Value(serde_json::Value::Number(max.into())),
            )]),
            null_count: std::collections::HashMap::from([(
                "id".to_string(),
                ColumnCountStat::Value(0),
            )]),
        };
        stats.to_json_string().expect("stats json")
    }

    #[tokio::test]
    async fn metadata_agg_exec_aggregates_stats_json() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            COL_STATS_JSON,
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some(stats_json(3, 1, 10)),
                Some(stats_json(5, 2, 20)),
            ]))],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let input: Arc<dyn ExecutionPlan> = Arc::new(OneBatchExec::new(batch));
        let table_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let exec = DeltaMetadataAggExec::try_new(
            input,
            table_schema,
            vec![
                MetadataAgg::Count,
                MetadataAgg::Min("id".to_string()),
                MetadataAgg::Max("id".to_string()),
            ],
        )?;

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx)?;
        let out = stream.try_next().await?.unwrap();

        let count = out
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>();
        assert_eq!(count.unwrap().value(0), 8);
        Ok(())
    }

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
}
