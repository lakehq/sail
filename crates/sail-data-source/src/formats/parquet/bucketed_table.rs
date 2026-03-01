use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::datasource::listing::ListingTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;

use super::bucketed_scan::BucketedParquetScanExec;

/// A `TableProvider` wrapper around `ListingTable` that adds Hash partitioning.
///
/// When `scan()` is called, it delegates to the inner `ListingTable` (which produces
/// a `ParquetExec` with one partition per file), then wraps the result in a
/// `BucketedParquetScanExec` to advertise `Partitioning::Hash(bucket_cols, N)`.
///
/// This allows DataFusion's `EnforceDistribution` optimizer to skip shuffle
/// when both sides of a join are bucketed on the same columns with the same
/// number of buckets.
#[derive(Debug)]
pub struct BucketedListingTable {
    inner: ListingTable,
    bucket_columns: Vec<String>,
    num_buckets: usize,
}

impl BucketedListingTable {
    pub fn new(inner: ListingTable, bucket_columns: Vec<String>, num_buckets: usize) -> Self {
        Self {
            inner,
            bucket_columns,
            num_buckets,
        }
    }

    pub fn bucket_columns(&self) -> &[String] {
        &self.bucket_columns
    }

    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }
}

#[async_trait]
impl TableProvider for BucketedListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.inner.constraints()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner_plan = self.inner.scan(state, projection, filters, limit).await?;

        // Determine which bucket columns survive the projection.
        let projected_schema = inner_plan.schema();
        let surviving_columns: Vec<String> = self
            .bucket_columns
            .iter()
            .filter(|col| projected_schema.index_of(col).is_ok())
            .cloned()
            .collect();

        // Only wrap if all bucket columns are present in the projection.
        if surviving_columns.len() != self.bucket_columns.len() {
            log::debug!(
                "BucketedListingTable: projection excludes bucket columns, \
                 falling back to inner scan"
            );
            return Ok(inner_plan);
        }

        match BucketedParquetScanExec::new(
            inner_plan.clone(),
            surviving_columns,
            self.num_buckets,
        ) {
            Ok(bucketed) => {
                log::debug!(
                    "BucketedListingTable: wrapping scan with Hash partitioning, \
                     columns=[{}], num_buckets={}",
                    self.bucket_columns.join(", "),
                    self.num_buckets,
                );
                Ok(Arc::new(bucketed))
            }
            Err(e) => {
                log::warn!(
                    "BucketedListingTable: cannot apply bucketed scan, \
                     falling back to inner scan: {e}"
                );
                Ok(inner_plan)
            }
        }
    }
}
