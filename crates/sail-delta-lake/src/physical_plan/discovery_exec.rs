use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::BooleanArray;
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
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::TryStreamExt;
use url::Url;

use crate::physical_plan::meta_adds;

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

    fn prune_mask_for_meta_batch(
        batch: &RecordBatch,
        predicate: &Arc<dyn PhysicalExpr>,
        table_schema: &SchemaRef,
        partition_columns: &[String],
    ) -> Result<Vec<bool>> {
        let adds = meta_adds::decode_adds_from_meta_batch(batch, Some(partition_columns))?;
        if adds.is_empty() {
            return Ok(vec![]);
        }
        crate::datasource::pruning::prune_adds_by_physical_predicate(
            adds,
            table_schema.clone(),
            Arc::clone(predicate),
        )
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
