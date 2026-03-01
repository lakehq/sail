use std::any::Any;
use std::collections::{HashMap, HashSet};
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

/// Compute the bucket ID for a combination of column values using the same
/// hash as the writer (`ahash::RandomState::with_seeds(0,0,0,0)` + `create_hashes`).
///
/// Values must be in the same order as the bucket columns used during writing.
fn compute_multi_bucket_id(values: &[&ScalarValue], num_buckets: usize) -> Option<usize> {
    let arrays: Vec<ArrayRef> = values
        .iter()
        .map(|v| v.to_array_of_size(1).ok())
        .collect::<Option<Vec<_>>>()?;
    let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
    let mut hashes = vec![0u64; 1];
    create_hashes(&arrays, &random_state, &mut hashes).ok()?;
    Some((hashes[0] as usize) % num_buckets)
}

/// Analyze filter expressions to determine which bucket IDs need to be read.
///
/// Supports single-column and multi-column bucketing.
/// For multi-column, all bucket columns must be constrained in the filters;
/// the cross-product of per-column values is hashed to find target buckets.
///
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
    // For each top-level filter (implicitly ANDed), collect per-column value sets.
    let per_filter: Vec<HashMap<String, HashSet<ScalarValue>>> = filters
        .iter()
        .map(|f| {
            let mut m = HashMap::new();
            collect_column_values(f, bucket_columns, &mut m);
            m
        })
        .collect();

    // For each bucket column, intersect value sets from filters that constrain it.
    // (Top-level filters are ANDed, so same-column constraints must all hold.)
    let mut column_values: HashMap<&str, HashSet<ScalarValue>> = HashMap::new();
    for col in bucket_columns {
        for filter_vals in &per_filter {
            if let Some(vals) = filter_vals.get(col.as_str()) {
                match column_values.get_mut(col.as_str()) {
                    Some(existing) => {
                        *existing = existing.intersection(vals).cloned().collect();
                    }
                    None => {
                        column_values.insert(col.as_str(), vals.clone());
                    }
                }
            }
        }
    }

    // All bucket columns must have constrained values for pruning to work.
    if bucket_columns
        .iter()
        .any(|col| !column_values.contains_key(col.as_str()))
    {
        return None;
    }

    // Contradictory filters on any column → no rows match → no buckets needed.
    if bucket_columns
        .iter()
        .any(|col| column_values[col.as_str()].is_empty())
    {
        return Some(HashSet::new());
    }

    // Build cross-product of per-column values (in bucket_columns order) and
    // hash each combination to find target bucket IDs.
    let ordered_sets: Vec<Vec<&ScalarValue>> = bucket_columns
        .iter()
        .map(|col| column_values[col.as_str()].iter().collect())
        .collect();

    let mut bucket_ids = HashSet::new();
    for combo in cross_product(&ordered_sets) {
        if let Some(id) = compute_multi_bucket_id(&combo, num_buckets) {
            bucket_ids.insert(id);
        }
    }

    if bucket_ids.is_empty() {
        None
    } else {
        Some(bucket_ids)
    }
}

/// Collect possible literal values per bucket column from a filter expression.
///
/// For `AND`, collects from both sides (values for the same column will be
/// intersected at the caller level across top-level filters).
/// For `OR`, collects from both sides (over-approximation: may read extra
/// buckets, but never misses data).
fn collect_column_values(
    expr: &Expr,
    bucket_columns: &[String],
    values: &mut HashMap<String, HashSet<ScalarValue>>,
) {
    match expr {
        // col = literal  or  literal = col
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let pair = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(lit, _)) => Some((col.name(), lit)),
                (Expr::Literal(lit, _), Expr::Column(col)) => Some((col.name(), lit)),
                _ => None,
            };
            if let Some((col_name, lit)) = pair {
                if bucket_columns.iter().any(|bc| bc == col_name) {
                    values
                        .entry(col_name.to_string())
                        .or_default()
                        .insert(lit.clone());
                }
            }
        }
        // col IN (v1, v2, ...)
        Expr::InList(in_list) if !in_list.negated => {
            if let Expr::Column(col) = in_list.expr.as_ref() {
                if bucket_columns.iter().any(|bc| bc == col.name()) {
                    let entry = values.entry(col.name().to_string()).or_default();
                    for item in &in_list.list {
                        if let Expr::Literal(lit, _) = item {
                            entry.insert(lit.clone());
                        }
                    }
                }
            }
        }
        // AND: collect from both sides
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_column_values(&binary.left, bucket_columns, values);
            collect_column_values(&binary.right, bucket_columns, values);
        }
        // OR: collect from both sides (over-approximation but correct)
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            collect_column_values(&binary.left, bucket_columns, values);
            collect_column_values(&binary.right, bucket_columns, values);
        }
        _ => {}
    }
}

/// Compute the Cartesian product of value sets.
fn cross_product<'a>(sets: &[Vec<&'a ScalarValue>]) -> Vec<Vec<&'a ScalarValue>> {
    let mut result = vec![vec![]];
    for set in sets {
        let mut new_result = Vec::with_capacity(result.len() * set.len());
        for existing in &result {
            for &value in set {
                let mut combo = existing.clone();
                combo.push(value);
                new_result.push(combo);
            }
        }
        result = new_result;
    }
    result
}
