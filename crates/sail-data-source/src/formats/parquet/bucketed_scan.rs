use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::arrow::compute::SortOptions;
use datafusion::common::{plan_err, Result, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

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
    sort_columns: Vec<(String, bool)>,
    /// When set, only these bucket IDs will be read; other partitions return empty streams.
    target_buckets: Option<HashSet<usize>>,
    properties: PlanProperties,
}

impl BucketedParquetScanExec {
    /// Create a new `BucketedParquetScanExec` wrapping the given inner plan.
    ///
    /// Builds `Partitioning::Hash(column_exprs, num_buckets)` from the inner
    /// plan's output schema and the bucket column names.
    ///
    /// Returns an error if the inner plan's partition count does not match
    /// `num_buckets`, or if any bucket column is missing from the schema.
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        bucket_columns: Vec<String>,
        num_buckets: usize,
        sort_columns: Vec<(String, bool)>,
    ) -> Result<Self> {
        if bucket_columns.is_empty() {
            return plan_err!("BucketedParquetScanExec requires at least one bucket column");
        }
        if num_buckets == 0 {
            return plan_err!("BucketedParquetScanExec requires num_buckets > 0");
        }
        let inner_partitions = inner.output_partitioning().partition_count();
        if inner_partitions != num_buckets {
            return plan_err!(
                "BucketedParquetScanExec: inner plan has {inner_partitions} partitions \
                 but expected {num_buckets} buckets"
            );
        }

        let schema = inner.schema();
        let hash_exprs = build_hash_exprs(&schema, &bucket_columns)?;
        let partitioning = Partitioning::Hash(hash_exprs, num_buckets);

        let mut eq_properties = inner.equivalence_properties().clone();

        // Declare sort orderings if buckets are pre-sorted.
        if !sort_columns.is_empty() {
            let sort_exprs: Vec<PhysicalSortExpr> = sort_columns
                .iter()
                .filter_map(|(name, ascending)| {
                    schema.index_of(name).ok().map(|idx| PhysicalSortExpr {
                        expr: Arc::new(Column::new(name, idx)),
                        options: SortOptions {
                            descending: !ascending,
                            nulls_first: !ascending,
                        },
                    })
                })
                .collect();
            eq_properties.add_orderings(vec![sort_exprs]);
        }

        let properties = PlanProperties::new(
            eq_properties,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            inner,
            bucket_columns,
            num_buckets,
            sort_columns,
            target_buckets: None,
            properties,
        })
    }

    /// Set the target bucket IDs for pruning. Only these partitions will be read;
    /// other partitions return empty streams.
    pub fn with_target_buckets(mut self, target_buckets: Option<HashSet<usize>>) -> Self {
        self.target_buckets = target_buckets;
        self
    }

    pub fn bucket_columns(&self) -> &[String] {
        &self.bucket_columns
    }

    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    pub fn sort_columns(&self) -> &[(String, bool)] {
        &self.sort_columns
    }

    pub fn target_buckets(&self) -> Option<&HashSet<usize>> {
        self.target_buckets.as_ref()
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
        )?;
        if !self.sort_columns.is_empty() {
            let sort_str: Vec<String> = self
                .sort_columns
                .iter()
                .map(|(name, asc)| {
                    if *asc {
                        format!("{name}:asc")
                    } else {
                        format!("{name}:desc")
                    }
                })
                .collect();
            write!(f, ", sort=[{}]", sort_str.join(", "))?;
        }
        if let Some(ref targets) = self.target_buckets {
            let mut ids: Vec<usize> = targets.iter().copied().collect();
            ids.sort_unstable();
            write!(f, ", target_buckets={ids:?}")?;
        }
        Ok(())
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
            (Some(new_inner), true) => Ok(Arc::new(
                Self::new(
                    new_inner,
                    self.bucket_columns.clone(),
                    self.num_buckets,
                    self.sort_columns.clone(),
                )?
                .with_target_buckets(self.target_buckets.clone()),
            )),
            _ => plan_err!("BucketedParquetScanExec expects exactly one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // If target_buckets is set and this partition is not in it, return an empty stream.
        if let Some(ref targets) = self.target_buckets {
            if !targets.contains(&partition) {
                return Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.inner.schema(),
                    futures::stream::empty(),
                )));
            }
        }
        self.inner.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
