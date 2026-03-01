use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::datasource::listing::ListingTable;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_common::hash_utils::create_hashes;
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
    sort_columns: Vec<(String, bool)>,
}

impl BucketedListingTable {
    pub fn new(
        inner: ListingTable,
        bucket_columns: Vec<String>,
        num_buckets: usize,
        sort_columns: Vec<(String, bool)>,
    ) -> Self {
        Self {
            inner,
            bucket_columns,
            num_buckets,
            sort_columns,
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

        // Analyze filters to determine if we can prune buckets.
        let target_buckets =
            analyze_bucket_filters(filters, &self.bucket_columns, self.num_buckets);
        if let Some(ref targets) = target_buckets {
            log::debug!(
                "BucketedListingTable: bucket pruning active, target_buckets={:?}",
                targets,
            );
        }

        match BucketedParquetScanExec::new(
            inner_plan.clone(),
            surviving_columns,
            self.num_buckets,
            self.sort_columns.clone(),
        ) {
            Ok(bucketed) => {
                let bucketed = bucketed.with_target_buckets(target_buckets);
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

/// Compute the bucket ID for a scalar value using the same hash as the writer.
///
/// Uses `ahash::RandomState::with_seeds(0,0,0,0)` + `create_hashes`, matching
/// the logic in `bucketed_sink.rs`.
fn compute_bucket_id(value: &ScalarValue, num_buckets: usize) -> Option<usize> {
    let array: ArrayRef = value.to_array_of_size(1).ok()?;
    let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
    let mut hashes = vec![0u64; 1];
    create_hashes(&[array], &random_state, &mut hashes).ok()?;
    Some((hashes[0] as usize) % num_buckets)
}

/// Analyze filter expressions to determine which bucket IDs need to be read.
///
/// Currently handles single-column bucketing only (multi-column deferred).
/// Supports:
/// - `bucket_col = literal`
/// - `bucket_col IN (v1, v2, ...)`
/// - `AND` / `OR` combinations
///
/// Returns `None` if no bucket pruning can be determined (reads all buckets).
fn analyze_bucket_filters(
    filters: &[Expr],
    bucket_columns: &[String],
    num_buckets: usize,
) -> Option<HashSet<usize>> {
    // Only support single-column bucketing for now.
    if bucket_columns.len() != 1 {
        return None;
    }
    let bucket_col = &bucket_columns[0];

    // Intersect results from all filters (they are implicitly ANDed).
    let mut result: Option<HashSet<usize>> = None;
    for filter in filters {
        if let Some(buckets) = extract_bucket_ids(filter, bucket_col, num_buckets) {
            result = Some(match result {
                Some(existing) => existing.intersection(&buckets).copied().collect(),
                None => buckets,
            });
        }
    }
    result
}

/// Extract target bucket IDs from a single expression.
fn extract_bucket_ids(
    expr: &Expr,
    bucket_col: &str,
    num_buckets: usize,
) -> Option<HashSet<usize>> {
    match expr {
        // col = literal  or  literal = col
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(lit, _)) if col.name() == bucket_col => {
                    let id = compute_bucket_id(lit, num_buckets)?;
                    Some(HashSet::from([id]))
                }
                (Expr::Literal(lit, _), Expr::Column(col)) if col.name() == bucket_col => {
                    let id = compute_bucket_id(lit, num_buckets)?;
                    Some(HashSet::from([id]))
                }
                _ => None,
            }
        }
        // col IN (v1, v2, ...)
        Expr::InList(in_list) if !in_list.negated => {
            if let Expr::Column(col) = in_list.expr.as_ref() {
                if col.name() == bucket_col {
                    let mut buckets = HashSet::new();
                    for item in &in_list.list {
                        if let Expr::Literal(lit, _) = item {
                            if let Some(id) = compute_bucket_id(lit, num_buckets) {
                                buckets.insert(id);
                            }
                        }
                    }
                    if buckets.is_empty() {
                        return None;
                    }
                    return Some(buckets);
                }
            }
            None
        }
        // AND: intersect both sides
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = extract_bucket_ids(&binary.left, bucket_col, num_buckets);
            let right = extract_bucket_ids(&binary.right, bucket_col, num_buckets);
            match (left, right) {
                (Some(l), Some(r)) => Some(l.intersection(&r).copied().collect()),
                (Some(s), None) | (None, Some(s)) => Some(s),
                (None, None) => None,
            }
        }
        // OR: union both sides (only if both sides provide bucket info)
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = extract_bucket_ids(&binary.left, bucket_col, num_buckets);
            let right = extract_bucket_ids(&binary.right, bucket_col, num_buckets);
            match (left, right) {
                (Some(l), Some(r)) => Some(l.union(&r).copied().collect()),
                // If one side is unknown, we can't prune at all.
                _ => None,
            }
        }
        _ => None,
    }
}
