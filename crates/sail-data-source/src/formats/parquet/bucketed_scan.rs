use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::{plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// Physical plan node that wraps an inner scan with Hash partitioning metadata.
///
/// When a bucketed Parquet table is read, `ListingTable` creates a `ParquetExec`
/// with N partitions (one per bucket file, in alphabetical order matching bucket IDs).
/// This wrapper overrides the `output_partitioning()` to report
/// `Partitioning::Hash(bucket_col_exprs, num_buckets)`, so that DataFusion's
/// `EnforceDistribution` optimizer can skip shuffle for joins on bucket columns.
#[derive(Debug, Clone)]
pub struct BucketedParquetScanExec {
    inner: Arc<dyn ExecutionPlan>,
    bucket_columns: Vec<String>,
    num_buckets: usize,
    properties: PlanProperties,
}

impl BucketedParquetScanExec {
    /// Create a new `BucketedParquetScanExec` wrapping the given inner plan.
    ///
    /// Builds `Partitioning::Hash(column_exprs, num_buckets)` from the inner
    /// plan's output schema and the bucket column names.
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        bucket_columns: Vec<String>,
        num_buckets: usize,
    ) -> Result<Self> {
        let schema = inner.schema();
        let hash_exprs = build_hash_exprs(&schema, &bucket_columns)?;
        let partitioning = Partitioning::Hash(hash_exprs, num_buckets);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            inner,
            bucket_columns,
            num_buckets,
            properties,
        })
    }

    pub fn bucket_columns(&self) -> &[String] {
        &self.bucket_columns
    }

    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

impl DisplayAs for BucketedParquetScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BucketedParquetScanExec: columns=[{}], num_buckets={}",
            self.bucket_columns.join(", "),
            self.num_buckets,
        )
    }
}

impl ExecutionPlan for BucketedParquetScanExec {
    fn name(&self) -> &str {
        "BucketedParquetScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(new_inner), true) => Ok(Arc::new(Self::new(
                new_inner,
                self.bucket_columns.clone(),
                self.num_buckets,
            )?)),
            _ => plan_err!("BucketedParquetScanExec expects exactly one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

/// Build `Column` physical expressions for Hash partitioning from column names.
fn build_hash_exprs(
    schema: &SchemaRef,
    bucket_columns: &[String],
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    bucket_columns
        .iter()
        .map(|col_name| {
            let idx = schema.index_of(col_name).map_err(|_| {
                datafusion_common::DataFusionError::Plan(format!(
                    "bucket column '{col_name}' not found in schema"
                ))
            })?;
            Ok(Arc::new(Column::new(col_name, idx)) as Arc<dyn PhysicalExpr>)
        })
        .collect()
}
