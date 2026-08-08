//! Resolves `min`/`max` aggregates over a partition column of a listing table from
//! directory listings alone.
//!
//! The usual way to find the latest partition of a table (`SELECT max(dt) FROM t`,
//! or a window function over `dt`) makes the scan open every file in the table.
//! When the aggregate only reads a partition column, the answer is fully determined
//! by the names of the directories at that level of the tree, so a single
//! `list_with_delimiter` request is enough and no data file is ever opened.
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
//! This is opt-in because it changes the result for partitions whose files contain
//! no rows: such a partition is invisible to a real scan, but its directory is
//! listed here. Partitions with no non-empty file at all are still excluded, which
//! matches how [`pruned_partition_list`] builds the file list for a normal scan.
//!
//! [`pruned_partition_list`]: datafusion::datasource::listing::helpers::pruned_partition_list

use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::execution::SessionState;
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
    table_url: String,
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

/// Rewrites `min`/`max` over the leading partition column into a [`PartitionBoundsNode`].
///
/// The rule is deliberately conservative: anything it does not fully understand is
/// left alone and planned the usual way.
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
            Some(node) => Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(node) as Arc<dyn UserDefinedLogicalNode>,
            }))),
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

    // Only string partition columns for now. A directory name is text, so comparing it
    // as text needs no conversion and cannot disagree with how the scan would have
    // ordered the values. Numeric and date columns can follow once the parsing and
    // ordering of their directory names is covered by tests.
    if !matches!(
        field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return None;
    }

    // The directory names at one level of the tree only determine the aggregate when
    // every partition column above that level is pinned to a single value, so that
    // exactly one directory has to be listed. Without those filters, the maximum over
    // the table spans every branch of the tree and a single listing does not see it.
    let partition_columns = config.schema.table_partition_cols();
    let depth = partition_columns
        .iter()
        .position(|column| column.name() == field.name())?;
    let prefix = partition_prefix(&scan.filters, partition_columns, depth)?;

    Some(PartitionBoundsNode {
        table_url: table_path.as_str().to_string(),
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

/// A partition directory directly below the table root.
struct PartitionCandidate {
    value: ScalarValue,
    path: Path,
}

pub(crate) async fn plan_partition_bounds(
    session_state: &SessionState,
    node: &PartitionBoundsNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let url = ListingTableUrl::parse(&node.table_url)?;
    let store = session_state.runtime_env().object_store(&url)?;

    let mut candidates = list_partition_candidates(store.as_ref(), &url, node).await?;
    candidates.sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap_or(Ordering::Equal));

    // Each bound is resolved at most once, walking the sorted candidates from the
    // end that bound cares about and stopping at the first partition holding data.
    let null = ScalarValue::try_from(&node.data_type)?;
    let min = if node.bounds.contains(&PartitionBound::Min) {
        first_non_empty(store.as_ref(), &url, candidates.iter(), &null).await?
    } else {
        null.clone()
    };
    let max = if node.bounds.contains(&PartitionBound::Max) {
        first_non_empty(store.as_ref(), &url, candidates.iter().rev(), &null).await?
    } else {
        null.clone()
    };

    let mut expressions = Vec::with_capacity(node.bounds.len());
    for (index, bound) in node.bounds.iter().enumerate() {
        let value = match bound {
            PartitionBound::Min => min.clone(),
            PartitionBound::Max => max.clone(),
        };
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

/// Lists the directories directly below the table root and turns each one into the
/// partition value it encodes. Only the top level is listed, so this is a single
/// request regardless of how many partitions the table has.
async fn list_partition_candidates(
    store: &dyn ObjectStore,
    url: &ListingTableUrl,
    node: &PartitionBoundsNode,
) -> Result<Vec<PartitionCandidate>> {
    let prefix = Path::from_iter(
        url.prefix()
            .parts()
            .chain(node.prefix.iter().map(|part| PathPart::from(part.as_str()))),
    );
    let listing = store
        .list_with_delimiter(Some(&prefix).filter(|p| !p.as_ref().is_empty()))
        .await?;

    let mut candidates = Vec::with_capacity(listing.common_prefixes.len());
    for path in listing.common_prefixes {
        let Some(name) = path.filename() else {
            continue;
        };
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
        if value.is_null() {
            continue;
        }
        candidates.push(PartitionCandidate { value, path });
    }
    Ok(candidates)
}

/// Returns the value of the first candidate that holds at least one file a scan
/// would read, so that partitions left behind by a deleted or empty write do not
/// win the comparison.
async fn first_non_empty<'a, I>(
    store: &dyn ObjectStore,
    url: &ListingTableUrl,
    candidates: I,
    null: &ScalarValue,
) -> Result<ScalarValue>
where
    I: Iterator<Item = &'a PartitionCandidate>,
{
    for candidate in candidates {
        if has_readable_file(store, url, &candidate.path).await? {
            return Ok(candidate.value.clone());
        }
    }
    Ok(null.clone())
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
