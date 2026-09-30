//! Resolves `min`/`max` aggregates over a partition column of a listing table from
//! directory listings alone.
//!
//! Finding the latest partition with `SELECT max(dt) FROM t` makes the scan open
//! every file in the table. When the aggregate only reads a partition column, the
//! answer is fully determined by the names of the directories at that level of the
//! tree, so a single `list_with_delimiter` request is enough.
//!
//! Reaching a level below the first one requires every partition column above it to
//! be pinned by an equality filter, so that exactly one directory has to be listed.
//! A table partitioned by `year`, `month` and `day` therefore yields its latest
//! partition in three requests, one per level:
//!
//! ```sql
//! SELECT max(year) FROM t;
//! SELECT max(month) FROM t WHERE year = '2025';
//! SELECT max(day) FROM t WHERE year = '2025' AND month = '10';
//! ```
//!
//! Note that `SELECT max(year), max(month), max(day) FROM t` is a different question
//! and is left alone: its answer is the largest value seen in each column separately,
//! which need not be a partition that exists.
//!
//! # Relation to Spark
//!
//! Spark answers the same class of query with `OptimizeMetadataOnlyQuery`, behind
//! `spark.sql.optimizer.metadataOnly`. That setting is also off by default, and its
//! own documentation gives the same reason: it may return incorrect results when the
//! files are empty. The two differ in shape, deliberately:
//!
//! - Spark rewrites the *relation* into a `LocalRelation` of partition values, so it
//!   must require that every scanned column be a partition column. This rule rewrites
//!   the *aggregate*, whose result depends only on the aggregated column, so whatever
//!   else the scan would have read cannot change the answer and no such guard is needed.
//! - Spark covers `GROUP BY partition_col` and any aggregate insensitive to duplicates.
//!   Those need a node producing one row per partition rather than a single row, so
//!   this rule covers only `min` and `max` for now.
//! - Both accept `DISTINCT`, since it cannot change `min` or `max`.
//!
//! This is opt-in because it changes the result for partitions whose files contain
//! no rows: such a partition is invisible to a real scan, but its directory is
//! listed here. Partitions with no non-empty file at all are still excluded, which
//! matches how [`pruned_partition_list`] builds the file list for a normal scan.
//!
//! [`pruned_partition_list`]: datafusion::datasource::listing::helpers::pruned_partition_list

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::catalog::Session;
use datafusion::logical_expr::expr::{AggregateFunction, Alias, BinaryExpr};
use datafusion::logical_expr::{
    Aggregate, Expr, Extension, LogicalPlan, Operator, TableScan, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchemaRef, Result, ScalarValue, internal_err};
use datafusion_datasource::ListingTableUrl;
use educe::Educe;
use futures::StreamExt;
use log::warn;
use object_store::ObjectStore;
use object_store::path::{Path, PathPart};
use percent_encoding::percent_decode_str;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::listing::table::ListingTableSource;
use crate::listing::utils::has_hidden_path_component;

/// The marker Hive uses for a partition whose value is `NULL`.
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PartitionBound {
    Min,
    Max,
}

/// A leaf node that produces one row holding the bounds of a partition column.
///
/// The node carries the table location rather than the [`ListingTableSource`] itself
/// so that plan equality and hashing stay meaningful.
#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct PartitionBoundsNode {
    #[educe(PartialOrd(ignore))]
    table_url: ListingTableUrl,
    column: String,
    #[educe(PartialOrd(ignore))]
    data_type: DataType,
    /// The `col=value` segments, taken from equality filters, that lead to the one
    /// directory whose children hold the values of `column`. Empty when `column` is
    /// the leading partition column.
    prefix: Vec<String>,
    bounds: Vec<PartitionBound>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl PartitionBoundsNode {
    pub(crate) fn bounds(&self) -> &[PartitionBound] {
        &self.bounds
    }
}

impl UserDefinedLogicalNodeCore for PartitionBoundsNode {
    fn name(&self) -> &str {
        "PartitionBounds"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "PartitionBounds: column={}, bounds={:?}, path={}",
            self.column, self.bounds, self.table_url
        )?;
        if !self.prefix.is_empty() {
            write!(f, ", prefix={}", self.prefix.join("/"))?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}

/// Rewrites `min`/`max` over a partition column into a [`PartitionBoundsNode`],
/// leaving anything it does not fully understand to be planned the usual way.
///
/// Registered only when `execution.partition_bounds_from_listing` is on, so that a
/// disabled rule leaves no trace in `EXPLAIN` output.
#[derive(Debug, Default)]
pub struct ResolvePartitionBounds;

impl OptimizerRule for ResolvePartitionBounds {
    fn name(&self) -> &str {
        "resolve_partition_bounds"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(ref aggregate) = plan else {
            return Ok(Transformed::no(plan));
        };
        match rewrite_aggregate(aggregate) {
            Some(node) => {
                // Spark's `OptimizeMetadataOnlyQuery` warns on every rewrite too, so
                // that a surprising result has a trail to follow.
                warn!(
                    "Answering {:?} over partition column `{}` from directory names \
                     because `execution.partition_bounds_from_listing` is enabled. \
                     This can differ from a scan when a partition holds files with no rows.",
                    node.bounds, node.column
                );
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(node) as Arc<dyn UserDefinedLogicalNode>,
                })))
            }
            None => Ok(Transformed::no(plan)),
        }
    }
}

fn rewrite_aggregate(aggregate: &Aggregate) -> Option<PartitionBoundsNode> {
    if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.is_empty() {
        return None;
    }

    let mut bounds = Vec::with_capacity(aggregate.aggr_expr.len());
    let mut aggregated_index = None;
    for expr in &aggregate.aggr_expr {
        let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
            return None;
        };
        let bound = match func.name() {
            "min" => PartitionBound::Min,
            "max" => PartitionBound::Max,
            _ => return None,
        };
        // `DISTINCT` is accepted: `min` and `max` give the same answer with or
        // without it, which is how Spark's `OptimizeMetadataOnlyQuery` treats them.
        if params.filter.is_some() || !params.order_by.is_empty() || params.null_treatment.is_some()
        {
            return None;
        }
        let [Expr::Column(column)] = params.args.as_slice() else {
            return None;
        };
        let index = aggregate.input.schema().index_of_column(column).ok()?;
        match aggregated_index {
            Some(previous) if previous != index => return None,
            _ => aggregated_index = Some(index),
        }
        bounds.push(bound);
    }

    let (scan, index) = resolve_scan_column(aggregate.input.as_ref(), aggregated_index?)?;
    // A limit changes which rows take part in the aggregate, so the directory
    // listing alone is no longer the answer.
    if scan.fetch.is_some() {
        return None;
    }

    let source = scan.source.downcast_ref::<ListingTableSource>()?;
    let config = source.config();
    // A glob filter can exclude files, and therefore whole partitions.
    if config.path_glob_filter.is_some() {
        return None;
    }
    let [table_path] = config.table_paths.as_slice() else {
        return None;
    };

    let table_index = match &scan.projection {
        Some(projection) => *projection.get(index)?,
        None => index,
    };
    let field = config.schema.table_schema().fields().get(table_index)?;

    // Only string columns for now: a directory name is text, so comparing it as text
    // cannot disagree with how the scan would have ordered the values. Numeric and
    // date columns can follow once their parsing and ordering are covered by tests.
    if !matches!(
        field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return None;
    }

    // Without every column above it pinned, the aggregate spans every branch of the
    // tree at that level, which a single listing does not see.
    let partition_columns = config.schema.table_partition_cols();
    let depth = partition_columns
        .iter()
        .position(|column| column.name() == field.name())?;
    let prefix = partition_prefix(&scan.filters, partition_columns, depth)?;

    Some(PartitionBoundsNode {
        table_url: table_path.clone(),
        column: field.name().clone(),
        data_type: field.data_type().clone(),
        prefix,
        bounds,
        schema: Arc::clone(&aggregate.schema),
    })
}

/// Turns the scan filters into the `col=value` path segments leading to the single
/// directory that has to be listed.
///
/// Every partition column above `depth` must be pinned by an equality filter, and no
/// other filter may be present: anything else would restrict the rows taking part in
/// the aggregate in a way the directory names cannot express.
fn partition_prefix(
    filters: &[Expr],
    partition_columns: &[FieldRef],
    depth: usize,
) -> Option<Vec<String>> {
    if filters.len() != depth {
        return None;
    }
    let mut values: Vec<Option<String>> = vec![None; depth];
    for filter in filters {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else {
            return None;
        };
        if *op != Operator::Eq {
            return None;
        }
        let (column, literal) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(column), Expr::Literal(literal, _)) => (column, literal),
            (Expr::Literal(literal, _), Expr::Column(column)) => (column, literal),
            _ => return None,
        };
        // The value goes straight into a path segment, so it has to be text.
        let Some(Some(value)) = literal.try_as_str() else {
            return None;
        };
        let position = partition_columns
            .iter()
            .position(|candidate| candidate.name() == &column.name)?;
        // A filter on the aggregated column itself, or below it, does not narrow the
        // listing; a repeated column would make the prefix ambiguous.
        if position >= depth || values[position].is_some() {
            return None;
        }
        values[position] = Some(value.to_string());
    }

    values
        .into_iter()
        .zip(partition_columns)
        .map(|(value, column)| value.map(|value| format!("{}={}", column.name(), value)))
        .collect()
}

/// Follows a column of `plan` down to the table scan it originates from,
/// returning the scan and the column index within the scan output.
fn resolve_scan_column(plan: &LogicalPlan, index: usize) -> Option<(&TableScan, usize)> {
    match plan {
        LogicalPlan::Projection(projection) => {
            let expr = match projection.expr.get(index)? {
                Expr::Alias(Alias { expr, .. }) => expr.as_ref(),
                expr => expr,
            };
            let Expr::Column(column) = expr else {
                return None;
            };
            let input = projection.input.as_ref();
            let index = input.schema().index_of_column(column).ok()?;
            resolve_scan_column(input, index)
        }
        LogicalPlan::SubqueryAlias(alias) => resolve_scan_column(alias.input.as_ref(), index),
        LogicalPlan::TableScan(scan) => Some((scan, index)),
        _ => None,
    }
}

/// A partition directory at the level being aggregated.
struct PartitionCandidate {
    value: ScalarValue,
    path: Path,
    /// The `col=value` directory name, as it appears in the path.
    name: String,
}

/// How many partitions are inspected one request at a time before the search
/// switches to a single listing that settles every remaining candidate at once.
///
/// Object stores have no empty directories, so a partition that is listed almost
/// always holds data and the first probe succeeds. Filesystems do keep them, and a
/// retention policy that empties the oldest partitions would otherwise make `min`
/// cost one request per emptied partition.
const MAX_INDIVIDUAL_PROBES: usize = 8;

/// Reads the directory names and returns one value per bound the node asks for, in
/// the order the node lists them.
pub(crate) async fn resolve_partition_bounds(
    ctx: &dyn Session,
    node: &PartitionBoundsNode,
) -> Result<Vec<ScalarValue>> {
    let store = ctx.runtime_env().object_store(&node.table_url)?;
    let prefix = search_prefix(node);

    let mut candidates = list_partition_candidates(store.as_ref(), node, &prefix).await?;
    candidates.sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap_or(Ordering::Equal));

    // Each bound is resolved at most once, walking the sorted candidates from the
    // end that bound cares about and stopping at the first partition holding data.
    let null = ScalarValue::try_from(&node.data_type)?;
    let min = if node.bounds.contains(&PartitionBound::Min) {
        first_non_empty(store.as_ref(), node, &prefix, &candidates, false).await?
    } else {
        None
    };
    let max = if node.bounds.contains(&PartitionBound::Max) {
        first_non_empty(store.as_ref(), node, &prefix, &candidates, true).await?
    } else {
        None
    };
    // A table with no partition holding data has no bounds, which is `NULL`.
    let min = min.unwrap_or_else(|| null.clone());
    let max = max.unwrap_or_else(|| null.clone());

    Ok(node
        .bounds
        .iter()
        .map(|bound| match bound {
            PartitionBound::Min => min.clone(),
            PartitionBound::Max => max.clone(),
        })
        .collect())
}

pub(crate) async fn plan_partition_bounds(
    session: &dyn Session,
    node: &PartitionBoundsNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let values = resolve_partition_bounds(session, node).await?;

    let mut expressions = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let Some(field) = node.schema.fields().get(index) else {
            return internal_err!("partition bounds output has no field at index {index}");
        };
        expressions.push((
            Arc::new(Literal::new(value)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));
    }

    let input = Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));
    Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
}

/// The single directory whose children hold the values of the aggregated column.
fn search_prefix(node: &PartitionBoundsNode) -> Path {
    Path::from_iter(
        node.table_url
            .prefix()
            .parts()
            .chain(node.prefix.iter().map(|part| PathPart::from(part.as_str()))),
    )
}

/// Lists the directories below `prefix` and turns each one into the partition value
/// it encodes. Only that one level is listed, so this does not grow with the depth
/// of the table nor with the number of files it holds.
async fn list_partition_candidates(
    store: &dyn ObjectStore,
    node: &PartitionBoundsNode,
    prefix: &Path,
) -> Result<Vec<PartitionCandidate>> {
    let listing = store
        .list_with_delimiter(Some(prefix).filter(|p| !p.as_ref().is_empty()))
        .await?;

    let mut candidates = Vec::with_capacity(listing.common_prefixes.len());
    for path in listing.common_prefixes {
        let Some(name) = path.filename() else {
            continue;
        };
        let name = name.to_string();
        let Some(raw) = name.strip_prefix(&format!("{}=", node.column)) else {
            continue;
        };
        let Ok(decoded) = percent_decode_str(raw).decode_utf8() else {
            continue;
        };
        // A `NULL` partition never takes part in `min`/`max`.
        if decoded == HIVE_DEFAULT_PARTITION {
            continue;
        }
        let Ok(value) = ScalarValue::try_from_string(decoded.into_owned(), &node.data_type) else {
            continue;
        };
        // Unreachable for the string types the rule accepts, where the conversion is
        // a cast between string types. It guards the numeric and date types that are
        // still to come, whose directory names can fail to parse.
        if value.is_null() {
            continue;
        }
        candidates.push(PartitionCandidate { value, path, name });
    }
    Ok(candidates)
}

/// Returns the value of the first candidate holding at least one file a scan would
/// read, so that a partition left behind by a deleted or empty write does not win
/// the comparison. `from_largest` picks the end of the sorted candidates to start from.
///
/// Returns `None` when no candidate holds data, which is the empty table case and
/// yields a `NULL` bound.
async fn first_non_empty(
    store: &dyn ObjectStore,
    node: &PartitionBoundsNode,
    prefix: &Path,
    candidates: &[PartitionCandidate],
    from_largest: bool,
) -> Result<Option<ScalarValue>> {
    let ordered: Vec<&PartitionCandidate> = if from_largest {
        candidates.iter().rev().collect()
    } else {
        candidates.iter().collect()
    };

    for candidate in ordered.iter().take(MAX_INDIVIDUAL_PROBES) {
        if has_readable_file(store, &node.table_url, &candidate.path).await? {
            return Ok(Some(candidate.value.clone()));
        }
    }
    if ordered.len() <= MAX_INDIVIDUAL_PROBES {
        return Ok(None);
    }

    // Enough partitions turned out to be empty that probing them one at a time is no
    // longer the cheaper option. One listing of the whole prefix settles all of them,
    // and costs the same order of requests as the candidate listing already did.
    let with_data = list_partitions_holding_data(store, node, prefix).await?;
    Ok(ordered
        .iter()
        .skip(MAX_INDIVIDUAL_PROBES)
        .find(|candidate| with_data.contains(&candidate.name))
        .map(|candidate| candidate.value.clone()))
}

/// The names of the directories below `prefix` that hold at least one readable file,
/// collected with a single listing.
async fn list_partitions_holding_data(
    store: &dyn ObjectStore,
    node: &PartitionBoundsNode,
    prefix: &Path,
) -> Result<HashSet<String>> {
    let depth = prefix.parts().count();
    let mut names = HashSet::new();
    let mut objects = store.list(Some(prefix));
    while let Some(object) = objects.next().await {
        let object = object?;
        if object.size == 0 || has_hidden_path_component(&node.table_url, &object.location) {
            continue;
        }
        if let Some(part) = object.location.parts().nth(depth) {
            names.insert(part.as_ref().to_string());
        }
    }
    Ok(names)
}

async fn has_readable_file(
    store: &dyn ObjectStore,
    url: &ListingTableUrl,
    path: &Path,
) -> Result<bool> {
    let mut objects = store.list(Some(path));
    while let Some(object) = objects.next().await {
        let object = object?;
        if object.size > 0 && !has_hidden_path_component(url, &object.location) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::{col, lit};

    use super::*;

    fn partition_columns() -> Vec<FieldRef> {
        ["year", "month", "day"]
            .into_iter()
            .map(|name| Arc::new(Field::new(name, DataType::Utf8, true)))
            .collect()
    }

    #[test]
    fn leading_column_needs_no_filter() {
        let columns = partition_columns();
        assert_eq!(partition_prefix(&[], &columns, 0), Some(vec![]));
    }

    #[test]
    fn leading_column_rejects_any_filter() {
        let columns = partition_columns();
        let filters = [col("year").eq(lit("2025"))];
        // Nothing above the leading column can be pinned, so a filter here only
        // narrows the rows and the directory names no longer give the answer.
        assert_eq!(partition_prefix(&filters, &columns, 0), None);
    }

    #[test]
    fn deeper_column_uses_the_pinned_ancestors() {
        let columns = partition_columns();
        let filters = [col("year").eq(lit("2025")), col("month").eq(lit("10"))];
        assert_eq!(
            partition_prefix(&filters, &columns, 2),
            Some(vec!["year=2025".to_string(), "month=10".to_string()])
        );
    }

    #[test]
    fn deeper_column_accepts_reversed_operands() {
        let columns = partition_columns();
        let filters = [lit("2025").eq(col("year"))];
        assert_eq!(
            partition_prefix(&filters, &columns, 1),
            Some(vec!["year=2025".to_string()])
        );
    }

    #[test]
    fn deeper_column_rejects_a_partially_pinned_prefix() {
        let columns = partition_columns();
        let filters = [col("year").eq(lit("2025"))];
        // `month` is unpinned, so every month of 2025 would have to be listed.
        assert_eq!(partition_prefix(&filters, &columns, 2), None);
    }

    #[test]
    fn deeper_column_rejects_a_gap_in_the_prefix() {
        let columns = partition_columns();
        let filters = [col("year").eq(lit("2025")), col("day").eq(lit("04"))];
        // `day` is at or below the aggregated column, so it does not narrow the listing.
        assert_eq!(partition_prefix(&filters, &columns, 2), None);
    }

    #[test]
    fn deeper_column_rejects_a_repeated_column() {
        let columns = partition_columns();
        let filters = [col("year").eq(lit("2025")), col("year").eq(lit("2024"))];
        assert_eq!(partition_prefix(&filters, &columns, 2), None);
    }

    #[test]
    fn deeper_column_rejects_a_range_filter() {
        let columns = partition_columns();
        let filters = [col("year").gt(lit("2024"))];
        assert_eq!(partition_prefix(&filters, &columns, 1), None);
    }

    #[test]
    fn deeper_column_rejects_a_non_literal_filter() {
        let columns = partition_columns();
        let filters = [col("year").eq(col("month"))];
        assert_eq!(partition_prefix(&filters, &columns, 1), None);
    }

    #[test]
    fn deeper_column_rejects_a_non_string_literal() {
        let columns = partition_columns();
        let filters = [col("year").eq(lit(2025_i64))];
        // The value is interpolated into a path segment, so it has to be text.
        assert_eq!(partition_prefix(&filters, &columns, 1), None);
    }

    #[test]
    fn unknown_column_is_rejected() {
        let columns = partition_columns();
        let filters = [col("region").eq(lit("eu"))];
        assert_eq!(partition_prefix(&filters, &columns, 1), None);
    }
}
