use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::compute::take_record_batch;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::logical_plan::{
    Aggregate, EmptyRelation, Extension, FetchType, Limit, Projection, SkipType, TableScan, Union,
};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableScanBuilder,
    TableSource, when,
};
use log::debug;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_logical_plan::range::RangeNode;

use crate::datasource::get_pushdown_filters;
use crate::datasource::pruning::partition_filter_mask;
use crate::logical::table_source::{
    DeltaFileSelection, DeltaMetadataAggregateSource, DeltaTableSource,
};
use crate::snapshot::{GroupedCountMetadata, GroupedCountMetadataRow, SnapshotPruningStats};

const MAX_METADATA_GROUPS: usize = 100_000;
const LARGE_METADATA_SAVINGS_BYTES: u64 = 64 * 1024 * 1024;
const LARGE_METADATA_SAVINGS_ROWS: u64 = 8192;
const COUNT_WEIGHT_COLUMN: &str = "__sail_delta_count_weight";
const COUNT_SUM_COLUMN: &str = "__sail_delta_count_sum";

pub struct DeltaMetadataAggregateRewriter<'a> {
    session: &'a dyn Session,
}

impl<'a> DeltaMetadataAggregateRewriter<'a> {
    pub fn new(session: &'a dyn Session) -> Self {
        Self { session }
    }
}

impl LogicalRewriter for DeltaMetadataAggregateRewriter<'_> {
    fn name(&self) -> &str {
        "delta_metadata_aggregate"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| {
            let rewritten = match &plan {
                LogicalPlan::Aggregate(aggregate) => {
                    match rewrite_exact_ungrouped_aggregate(aggregate, self.session)? {
                        Some(rewritten) => Some(rewritten),
                        None => rewrite_metadata_grouping(aggregate, self.session)?,
                    }
                }
                LogicalPlan::Limit(limit) => rewrite_empty_projection_limit(limit, self.session)?,
                _ => None,
            };
            match rewritten {
                Some(rewritten) => Ok(Transformed::yes(rewritten)),
                None => Ok(Transformed::no(plan)),
            }
        })
    }
}

fn rewrite_empty_projection_limit(
    limit: &Limit,
    session: &dyn Session,
) -> Result<Option<LogicalPlan>> {
    if !limit.input.schema().fields().is_empty() {
        return Ok(None);
    }
    let (SkipType::Literal(skip), FetchType::Literal(Some(fetch))) =
        (limit.get_skip_type()?, limit.get_fetch_type()?)
    else {
        return Ok(None);
    };
    let Some(required) = skip.checked_add(fetch) else {
        return Ok(None);
    };
    let Some(input) = DeltaAggregateInput::try_new(limit.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    let Some(indices) = input.metadata_file_indices(source, session)? else {
        return Ok(None);
    };
    let mut known_rows = 0usize;
    let mut complete = true;
    for index in indices {
        match source.snapshot().adds()[index].num_logical_records() {
            Some(rows) => known_rows = known_rows.saturating_add(rows),
            None => complete = false,
        }
        if known_rows >= required {
            break;
        }
    }
    let known_rows = known_rows.min(scan.fetch.unwrap_or(usize::MAX));
    if !complete && known_rows < required {
        return Ok(None);
    }
    let output_rows = known_rows.saturating_sub(skip).min(fetch);
    if output_rows <= 1 {
        return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: output_rows == 1,
            schema: Arc::clone(limit.input.schema()),
        })));
    }
    let Ok(end) = i64::try_from(output_rows) else {
        return Ok(None);
    };
    // A range carries the cardinality without allocating a row per log record during planning.
    let rows = LogicalPlan::Extension(Extension {
        node: Arc::new(RangeNode::try_new("__sail_delta_row".into(), 0, end, 1, 1)?),
    });
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(vec![], Arc::new(rows), Arc::clone(limit.input.schema()))?,
    )))
}

struct DeltaAggregateInput<'a> {
    plan: &'a LogicalPlan,
    scan: &'a TableScan,
}

impl<'a> DeltaAggregateInput<'a> {
    fn try_new(plan: &'a LogicalPlan) -> Option<Self> {
        let mut input = plan;
        loop {
            match input {
                LogicalPlan::TableScan(scan) => return Some(Self { plan, scan }),
                LogicalPlan::Projection(projection) => input = projection.input.as_ref(),
                LogicalPlan::SubqueryAlias(alias) => input = alias.input.as_ref(),
                _ => return None,
            }
        }
    }

    fn scan(&self) -> &'a TableScan {
        self.scan
    }

    fn has_partition_filters(&self, source: &DeltaTableSource) -> bool {
        if self
            .scan
            .filters
            .iter()
            .flat_map(Expr::column_refs)
            .any(|column| {
                source
                    .schema()
                    .field_with_name(&column.name)
                    .ok()
                    .map(|field| field.data_type().clone())
                    != source
                        .snapshot()
                        .schema()
                        .field_with_name(&column.name)
                        .ok()
                        .map(|field| field.data_type().clone())
            })
        {
            return false;
        }
        get_pushdown_filters(
            &self.scan.filters.iter().collect::<Vec<_>>(),
            source.snapshot().metadata().partition_columns(),
        )
        .iter()
        .all(|pushdown| *pushdown == TableProviderFilterPushDown::Exact)
    }

    fn metadata_file_indices(
        &self,
        source: &DeltaTableSource,
        session: &dyn Session,
    ) -> Result<Option<Vec<usize>>> {
        let snapshot = source.snapshot();
        if !snapshot.load_config().require_files || !self.has_partition_filters(source) {
            return Ok(None);
        }
        let indices = match source.file_selection() {
            DeltaFileSelection::Snapshot => (0..snapshot.adds().len()).collect::<Vec<_>>(),
            DeltaFileSelection::Selected(indices) => indices.to_vec(),
        };
        let Some(predicate) = conjunction(unnormalize_cols(self.scan.filters.clone())) else {
            return Ok(Some(indices));
        };
        if indices.is_empty() {
            return Ok(Some(indices));
        }
        let values = partition_filter_mask(
            session,
            snapshot,
            snapshot.schema(),
            snapshot.adds(),
            predicate,
        )?;
        Ok(Some(
            indices
                .into_iter()
                .filter(|&index| values.is_valid(index) && values.value(index))
                .collect(),
        ))
    }

    fn source_column(&self, aggregate_column: &Column) -> Option<String> {
        let source = self.scan.source.downcast_ref::<DeltaTableSource>()?;
        let expression = self.source_expression(&Expr::Column(aggregate_column.clone()), source)?;
        match expression {
            DeltaSourceExpression::Column { logical_path, .. } if logical_path.len() == 1 => {
                logical_path.into_iter().next()
            }
            _ => None,
        }
    }

    fn replace_source(&self, source: Arc<dyn TableSource>) -> Result<LogicalPlan> {
        let scan = self.scan();
        let replacement = LogicalPlan::TableScan(
            TableScanBuilder::new(scan.table_name.clone(), source)
                .with_projection(scan.projection.clone())
                .with_filters(scan.filters.clone())
                .with_fetch(scan.fetch)
                .with_statistics_requests(scan.statistics_requests.clone())
                .build()?,
        );
        self.plan
            .clone()
            .transform_up(|plan| match plan {
                LogicalPlan::TableScan(_) => Ok(Transformed::yes(replacement.clone())),
                _ => Ok(Transformed::no(plan)),
            })
            .map(|transformed| transformed.data)
    }

    fn source_expression(
        &self,
        expression: &Expr,
        source: &DeltaTableSource,
    ) -> Option<DeltaSourceExpression> {
        resolve_input_expression(self.plan, expression, source)
    }
}

#[derive(Debug, Clone)]
enum DeltaSourceExpression {
    Literal(ScalarValue),
    Column {
        logical_path: Vec<String>,
        data_type: DataType,
    },
    Cast {
        expression: Box<Self>,
        data_type: DataType,
    },
}

#[derive(Debug, Clone)]
struct DeltaValueStatistics {
    data_type: DataType,
    null_count: Precision<usize>,
    min_value: Precision<ScalarValue>,
    max_value: Precision<ScalarValue>,
}

fn resolve_input_expression(
    plan: &LogicalPlan,
    expression: &Expr,
    source: &DeltaTableSource,
) -> Option<DeltaSourceExpression> {
    match plan {
        LogicalPlan::TableScan(scan) => resolve_scan_expression(scan, expression, source),
        LogicalPlan::Projection(projection) => resolve_expression(expression, &|column| {
            let index = projection.schema.index_of_column(column).ok()?;
            resolve_input_expression(
                projection.input.as_ref(),
                projection.expr.get(index)?,
                source,
            )
        }),
        LogicalPlan::SubqueryAlias(alias) => resolve_expression(expression, &|column| {
            let index = alias.schema.index_of_column(column).ok()?;
            let column = alias.input.schema().columns().get(index)?.clone();
            resolve_input_expression(alias.input.as_ref(), &Expr::Column(column), source)
        }),
        _ => None,
    }
}

fn resolve_scan_expression(
    scan: &TableScan,
    expression: &Expr,
    source: &DeltaTableSource,
) -> Option<DeltaSourceExpression> {
    resolve_expression(expression, &|column| {
        let projected_index = scan.projected_schema.index_of_column(column).ok()?;
        let source_index = match &scan.projection {
            Some(projection) => *projection.get(projected_index)?,
            None => projected_index,
        };
        let scan_field = scan.source.schema().fields().get(source_index)?.clone();
        let snapshot_field = source
            .snapshot()
            .schema()
            .fields()
            .iter()
            .find(|field| field.name() == scan_field.name())?;
        if scan_field.data_type() != snapshot_field.data_type() {
            return None;
        }
        Some(DeltaSourceExpression::Column {
            logical_path: vec![snapshot_field.name().clone()],
            data_type: snapshot_field.data_type().clone(),
        })
    })
}

fn resolve_expression(
    expression: &Expr,
    resolve_column: &impl Fn(&Column) -> Option<DeltaSourceExpression>,
) -> Option<DeltaSourceExpression> {
    match expression {
        Expr::Alias(alias) => resolve_expression(alias.expr.as_ref(), resolve_column),
        Expr::Literal(value, _) => Some(DeltaSourceExpression::Literal(value.clone())),
        Expr::Column(column) => resolve_column(column),
        Expr::Cast(cast) => Some(DeltaSourceExpression::Cast {
            expression: Box::new(resolve_expression(cast.expr.as_ref(), resolve_column)?),
            data_type: cast.field.data_type().clone(),
        }),
        Expr::TryCast(cast) => Some(DeltaSourceExpression::Cast {
            expression: Box::new(resolve_expression(cast.expr.as_ref(), resolve_column)?),
            data_type: cast.field.data_type().clone(),
        }),
        Expr::ScalarFunction(function) if function.func.inner().is::<GetFieldFunc>() => {
            let [base, field] = function.args.as_slice() else {
                return None;
            };
            let field_name = match field {
                Expr::Literal(ScalarValue::Utf8(Some(value)), _)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
                | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => value,
                _ => return None,
            };
            let DeltaSourceExpression::Column {
                mut logical_path,
                data_type,
            } = resolve_expression(base, resolve_column)?
            else {
                return None;
            };
            let DataType::Struct(fields) = data_type else {
                return None;
            };
            let field = fields.iter().find(|field| field.name() == field_name)?;
            logical_path.push(field.name().clone());
            Some(DeltaSourceExpression::Column {
                logical_path,
                data_type: field.data_type().clone(),
            })
        }
        _ => None,
    }
}

fn rewrite_exact_ungrouped_aggregate(
    aggregate: &Aggregate,
    session: &dyn Session,
) -> Result<Option<LogicalPlan>> {
    if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.is_empty() {
        return Ok(None);
    }
    let Some(input) = DeltaAggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    if scan.fetch.is_some() {
        return Ok(None);
    }
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    let Some(indices) = input.metadata_file_indices(source, session)? else {
        return Ok(None);
    };
    let Ok(files) = source.snapshot().files_batch() else {
        return Ok(None);
    };
    let selected = if indices.len() == files.num_rows() {
        files.clone()
    } else {
        let indices = UInt64Array::from_iter_values(indices.into_iter().map(|index| index as u64));
        take_record_batch(files, &indices)?
    };
    let snapshot_stats = SnapshotPruningStats::try_new(&selected, source.snapshot())?;
    let Some(row_count) = snapshot_stats.exact_num_records() else {
        return Ok(None);
    };

    let values = aggregate
        .aggr_expr
        .iter()
        .zip(aggregate.schema.fields())
        .map(|(expression, field)| {
            exact_aggregate_value(
                expression,
                field.data_type(),
                &input,
                source,
                &snapshot_stats,
                row_count,
            )
        })
        .collect::<Vec<_>>();
    let resolved_count = values.iter().filter(|value| value.is_some()).count();
    if resolved_count == 0 {
        return Ok(None);
    }

    if resolved_count != values.len() {
        return build_residual_aggregate(aggregate, values).map(Some);
    }
    let Some(values) = values
        .into_iter()
        .zip(aggregate.schema.iter())
        .map(|(value, (qualifier, field))| {
            value.map(|value| {
                Expr::Literal(value, None).alias_qualified(qualifier.cloned(), field.name())
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    debug!(
        "resolved {} Delta aggregate expressions from exact snapshot statistics",
        values.len()
    );
    // Preserve the Aggregate schema by ordinal, including duplicate internal expression names and
    // typed NULLs, while representing the result as literals over one placeholder row.
    let row = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: Arc::new(DFSchema::empty()),
    });
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(values, Arc::new(row), Arc::clone(&aggregate.schema))?,
    )))
}

fn build_residual_aggregate(
    aggregate: &Aggregate,
    values: Vec<Option<ScalarValue>>,
) -> Result<LogicalPlan> {
    let residual_expr = aggregate
        .aggr_expr
        .iter()
        .zip(&values)
        .enumerate()
        .filter(|(_, (_, value))| value.is_none())
        .map(|(index, (expression, _))| {
            let expression = match expression {
                Expr::Alias(alias) => alias.expr.as_ref().clone(),
                expression => expression.clone(),
            };
            expression.alias(format!("__sail_delta_residual_aggregate_{index}"))
        })
        .collect::<Vec<_>>();
    let residual_input = project_residual_input(Arc::clone(&aggregate.input), &residual_expr)?;
    let residual =
        LogicalPlan::Aggregate(Aggregate::try_new(residual_input, vec![], residual_expr)?);
    let residual_columns = residual.schema().columns();
    let mut residual_index = 0;
    let output_expr = values
        .into_iter()
        .zip(aggregate.schema.fields())
        .map(|(value, field)| {
            let expression = match value {
                Some(value) => Expr::Literal(value, None),
                None => {
                    let column = Expr::Column(residual_columns[residual_index].clone());
                    residual_index += 1;
                    column
                }
            };
            expression.alias(field.name())
        })
        .collect::<Vec<_>>();
    debug!(
        "resolved {} Delta aggregate expressions and retained {} residual expressions",
        output_expr.len() - residual_columns.len(),
        residual_columns.len()
    );
    // A global residual aggregate always emits one row, so it is also the row carrier for the
    // metadata literals; no join or custom distributed operator is required.
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        output_expr,
        Arc::new(residual),
        Arc::clone(&aggregate.schema),
    )?))
}

fn project_residual_input(
    input: Arc<LogicalPlan>,
    residual_expr: &[Expr],
) -> Result<Arc<LogicalPlan>> {
    for expression in residual_expr {
        if expression.exists(|expression| {
            Ok(matches!(
                expression,
                Expr::ScalarSubquery(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::SetComparison(_)
            ))
        })? {
            // Subquery outer references are not included in `Expr::column_refs()`.
            return Ok(input);
        }
    }

    let schema = input.schema();
    let mut required_indices = HashSet::new();
    for column in residual_expr.iter().flat_map(Expr::column_refs) {
        let Some(index) = schema.maybe_index_of_column(column) else {
            return Ok(input);
        };
        required_indices.insert(index);
    }
    if required_indices.len() == schema.fields().len() {
        return Ok(input);
    }

    let projection = schema
        .columns()
        .into_iter()
        .enumerate()
        .filter_map(|(index, column)| {
            required_indices
                .contains(&index)
                .then_some(Expr::Column(column))
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
        projection, input,
    )?)))
}

fn exact_aggregate_value(
    expression: &Expr,
    output_type: &DataType,
    input: &DeltaAggregateInput<'_>,
    source: &DeltaTableSource,
    snapshot_stats: &SnapshotPruningStats<'_>,
    row_count: usize,
) -> Option<ScalarValue> {
    let expression = match expression {
        Expr::Alias(alias) => alias.expr.as_ref(),
        expression => expression,
    };
    let Expr::AggregateFunction(function) = expression else {
        return None;
    };
    if function.params.filter.is_some()
        || !function.params.order_by.is_empty()
        || function.params.null_treatment.is_some()
    {
        return None;
    }

    if function.func.name().eq_ignore_ascii_case("count") {
        return exact_count_value(
            &function.params.args,
            function.params.distinct,
            input,
            source,
            snapshot_stats,
            row_count,
        );
    }
    if function.func.name().eq_ignore_ascii_case("min") {
        return exact_extreme_value(
            function.params.args.as_slice(),
            output_type,
            input,
            source,
            snapshot_stats,
            row_count,
            true,
        );
    }
    if function.func.name().eq_ignore_ascii_case("max") {
        return exact_extreme_value(
            function.params.args.as_slice(),
            output_type,
            input,
            source,
            snapshot_stats,
            row_count,
            false,
        );
    }
    None
}

fn exact_count_value(
    arguments: &[Expr],
    distinct: bool,
    input: &DeltaAggregateInput<'_>,
    source: &DeltaTableSource,
    snapshot_stats: &SnapshotPruningStats<'_>,
    row_count: usize,
) -> Option<ScalarValue> {
    let as_count = |value: usize| {
        i64::try_from(value)
            .ok()
            .map(|value| ScalarValue::Int64(Some(value)))
    };
    if row_count == 0 {
        return as_count(0);
    }
    if arguments
        .iter()
        .any(|argument| constant_scalar(argument).is_some_and(|value| value.is_null()))
    {
        return as_count(0);
    }

    if distinct {
        let [argument] = arguments else {
            return None;
        };
        return constant_scalar(argument).and_then(|value| as_count(usize::from(!value.is_null())));
    }

    let mut nullable_count = None;
    for argument in arguments {
        let expression = input.source_expression(argument, source)?;
        let statistics = exact_value_statistics(expression, snapshot_stats, row_count)?;
        let Precision::Exact(null_count) = statistics.null_count else {
            return None;
        };
        if null_count == row_count {
            return as_count(0);
        }
        if null_count == 0 {
            continue;
        }
        if nullable_count.replace(null_count).is_some() {
            // Independent null counts do not describe the overlap between two nullable values.
            return None;
        }
    }
    as_count(row_count.checked_sub(nullable_count.unwrap_or(0))?)
}

fn exact_extreme_value(
    arguments: &[Expr],
    output_type: &DataType,
    input: &DeltaAggregateInput<'_>,
    source: &DeltaTableSource,
    snapshot_stats: &SnapshotPruningStats<'_>,
    row_count: usize,
    minimum: bool,
) -> Option<ScalarValue> {
    let [argument] = arguments else {
        return None;
    };
    if row_count == 0 {
        return ScalarValue::try_new_null(output_type).ok();
    }
    let expression = input.source_expression(argument, source)?;
    let statistics = exact_value_statistics(expression, snapshot_stats, row_count)?;
    if matches!(statistics.null_count, Precision::Exact(nulls) if nulls == row_count) {
        return ScalarValue::try_new_null(output_type).ok();
    }
    let bound = if minimum {
        statistics.min_value
    } else {
        statistics.max_value
    };
    let Precision::Exact(value) = bound else {
        return None;
    };
    value.cast_to(output_type).ok()
}

fn exact_value_statistics(
    expression: DeltaSourceExpression,
    snapshot_stats: &SnapshotPruningStats<'_>,
    row_count: usize,
) -> Option<DeltaValueStatistics> {
    match expression {
        DeltaSourceExpression::Literal(value) => Some(literal_statistics(value, row_count)),
        DeltaSourceExpression::Column {
            logical_path,
            data_type,
        } => {
            let column = snapshot_stats.exact_column_stats(&logical_path)?;
            if column.data_type != data_type {
                return None;
            }
            Some(DeltaValueStatistics {
                data_type,
                null_count: column.statistics.null_count,
                min_value: column.statistics.min_value,
                max_value: column.statistics.max_value,
            })
        }
        DeltaSourceExpression::Cast {
            expression,
            data_type,
        } => {
            let statistics = exact_value_statistics(*expression, snapshot_stats, row_count)?;
            cast_value_statistics(statistics, data_type, row_count)
        }
    }
}

fn literal_statistics(value: ScalarValue, row_count: usize) -> DeltaValueStatistics {
    let data_type = value.data_type();
    let is_null = value.is_null();
    let bound = if row_count > 0 && !is_null {
        Precision::Exact(value)
    } else {
        Precision::Absent
    };
    DeltaValueStatistics {
        data_type,
        null_count: Precision::Exact(if is_null { row_count } else { 0 }),
        min_value: bound.clone(),
        max_value: bound,
    }
}

fn cast_value_statistics(
    statistics: DeltaValueStatistics,
    target_type: DataType,
    row_count: usize,
) -> Option<DeltaValueStatistics> {
    if statistics.data_type == target_type {
        return Some(statistics);
    }
    if matches!(statistics.null_count, Precision::Exact(nulls) if nulls == row_count) {
        return Some(DeltaValueStatistics {
            data_type: target_type,
            null_count: statistics.null_count,
            min_value: Precision::Absent,
            max_value: Precision::Absent,
        });
    }

    let singleton = match (&statistics.min_value, &statistics.max_value) {
        (Precision::Exact(min), Precision::Exact(max)) if min == max => Some(min),
        _ => None,
    };
    if let Some(value) = singleton {
        let value = value.cast_to(&target_type).ok()?;
        return Some(DeltaValueStatistics {
            data_type: target_type,
            null_count: statistics.null_count,
            min_value: Precision::Exact(value.clone()),
            max_value: Precision::Exact(value),
        });
    }
    if !safe_monotonic_cast(&statistics.data_type, &target_type) {
        return None;
    }
    Some(DeltaValueStatistics {
        data_type: target_type.clone(),
        null_count: statistics.null_count,
        min_value: cast_exact_bound(statistics.min_value, &target_type),
        max_value: cast_exact_bound(statistics.max_value, &target_type),
    })
}

fn cast_exact_bound(
    bound: Precision<ScalarValue>,
    target_type: &DataType,
) -> Precision<ScalarValue> {
    match bound {
        Precision::Exact(value) => value
            .cast_to(target_type)
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        Precision::Inexact(_) | Precision::Absent => Precision::Absent,
    }
}

fn safe_monotonic_cast(source: &DataType, target: &DataType) -> bool {
    fn signed_width(data_type: &DataType) -> Option<u8> {
        Some(match data_type {
            DataType::Int8 => 8,
            DataType::Int16 => 16,
            DataType::Int32 => 32,
            DataType::Int64 => 64,
            _ => return None,
        })
    }
    fn unsigned_width(data_type: &DataType) -> Option<u8> {
        Some(match data_type {
            DataType::UInt8 => 8,
            DataType::UInt16 => 16,
            DataType::UInt32 => 32,
            DataType::UInt64 => 64,
            _ => return None,
        })
    }

    if source == target {
        return true;
    }
    if let (Some(source), Some(target)) = (signed_width(source), signed_width(target)) {
        return source <= target;
    }
    if let (Some(source), Some(target)) = (unsigned_width(source), unsigned_width(target)) {
        return source <= target;
    }
    matches!(
        (source, target),
        (DataType::Decimal32(source_precision, source_scale), DataType::Decimal32(target_precision, target_scale))
            | (DataType::Decimal64(source_precision, source_scale), DataType::Decimal64(target_precision, target_scale))
            | (DataType::Decimal128(source_precision, source_scale), DataType::Decimal128(target_precision, target_scale))
            | (DataType::Decimal256(source_precision, source_scale), DataType::Decimal256(target_precision, target_scale))
            if source_scale == target_scale && source_precision <= target_precision
    )
}

fn constant_scalar(expression: &Expr) -> Option<ScalarValue> {
    match expression {
        Expr::Alias(alias) => constant_scalar(alias.expr.as_ref()),
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => constant_scalar(cast.expr.as_ref())
            .and_then(|value| value.cast_to(cast.field.data_type()).ok()),
        Expr::TryCast(cast) => constant_scalar(cast.expr.as_ref())
            .and_then(|value| value.cast_to(cast.field.data_type()).ok()),
        _ => None,
    }
}

fn rewrite_metadata_grouping(
    aggregate: &Aggregate,
    session: &dyn Session,
) -> Result<Option<LogicalPlan>> {
    let distinct = !aggregate.group_expr.is_empty() && aggregate.aggr_expr.is_empty();
    let count = aggregate.aggr_expr.len() == 1 && is_row_count(&aggregate.aggr_expr[0]);
    if !distinct && !count {
        return Ok(None);
    }
    let Some(input) = DeltaAggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    if scan.fetch.is_some() {
        return Ok(None);
    }
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    if !input.has_partition_filters(source) {
        return Ok(None);
    }

    let group_columns = aggregate
        .group_expr
        .iter()
        .map(|expression| {
            let Expr::Column(column) = expression else {
                return None;
            };
            input.source_column(column)
        })
        .collect::<Option<Vec<_>>>();
    let Some(group_columns) = group_columns else {
        return Ok(None);
    };
    if group_columns.iter().collect::<HashSet<_>>().len() != group_columns.len() {
        return Ok(None);
    }
    let partition_columns = source.snapshot().metadata().partition_columns();
    if distinct
        && group_columns
            .iter()
            .any(|name| !partition_columns.contains(name))
    {
        return Ok(None);
    }

    let weighted_projection = weighted_projection_expressions(&aggregate.group_expr);
    let weighted_schema =
        Projection::try_new(weighted_projection.clone(), Arc::clone(&aggregate.input))?.schema;
    if weighted_schema
        .fields()
        .iter()
        .map(|field| field.name())
        .collect::<HashSet<_>>()
        .len()
        != weighted_schema.fields().len()
    {
        return Ok(None);
    }

    let weighted_input = if source.snapshot().load_config().require_files {
        let Some(indices) = input.metadata_file_indices(source, session)? else {
            return Ok(None);
        };
        let metadata = if indices.len() == source.snapshot().adds().len() {
            source
                .snapshot()
                .grouped_count_metadata(&group_columns, MAX_METADATA_GROUPS)
        } else {
            let adds = indices
                .iter()
                .map(|&index| source.snapshot().adds()[index].clone())
                .collect::<Vec<_>>();
            source
                .snapshot()
                .summarize_metadata_files(&adds, &group_columns, MAX_METADATA_GROUPS)
        };
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        if !worth_rewriting(&metadata) {
            return Ok(None);
        }
        let metadata_branch = build_metadata_branch(metadata.rows, &weighted_schema)?;
        let residual_branch = if metadata.residual_file_indices.is_empty() {
            None
        } else {
            let selected_source = source
                .try_select_files(
                    metadata
                        .residual_file_indices
                        .into_iter()
                        .map(|index| indices[index])
                        .collect(),
                )
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let residual_input = input.replace_source(Arc::new(selected_source))?;
            Some(LogicalPlan::Projection(Projection::try_new(
                weighted_projection,
                Arc::new(residual_input),
            )?))
        };
        match (metadata_branch, residual_branch) {
            (Some(metadata), Some(residual)) => LogicalPlan::Union(Union::try_new(vec![
                Arc::new(metadata),
                Arc::new(residual),
            ])?),
            (Some(metadata), None) => metadata,
            (None, Some(residual)) => residual,
            (None, None) => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(&weighted_schema),
            }),
        }
    } else {
        let metadata_source = DeltaMetadataAggregateSource {
            table: source.clone(),
            filters: unnormalize_cols(scan.filters.clone()),
            group_columns,
            schema: Arc::new(weighted_schema.as_arrow().clone()),
        };
        LogicalPlan::TableScan(
            TableScanBuilder::new(scan.table_name.clone(), Arc::new(metadata_source)).build()?,
        )
    };

    // Metadata rows carry a file's logical count; residual rows carry weight one.
    let weighted_columns = weighted_input.schema().columns();
    let group_count = aggregate.group_expr.len();
    let group_expr = weighted_columns[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let aggregate_expr = if count {
        let count_weight = Expr::Column(weighted_columns[group_count].clone());
        vec![sum(count_weight).alias(COUNT_SUM_COLUMN)]
    } else {
        vec![]
    };
    let weighted_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(weighted_input),
        group_expr,
        aggregate_expr,
    )?);

    let mut output_expr = weighted_aggregate.schema().columns()[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    if count {
        let count_sum = Expr::Column(weighted_aggregate.schema().columns()[group_count].clone());
        let count_sum = if group_count == 0 {
            when(
                count_sum.clone().is_null(),
                Expr::Literal(ScalarValue::Int64(Some(0)), None),
            )
            .otherwise(count_sum)?
        } else {
            count_sum
        };
        output_expr.push(count_sum.alias(aggregate.schema.field(group_count).name()));
    }
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(
            output_expr,
            Arc::new(weighted_aggregate),
            Arc::clone(&aggregate.schema),
        )?,
    )))
}

fn is_row_count(expression: &Expr) -> bool {
    let expression = match expression {
        Expr::Alias(alias) => alias.expr.as_ref(),
        expression => expression,
    };
    let Expr::AggregateFunction(function) = expression else {
        return false;
    };
    function.func.name().eq_ignore_ascii_case("count")
        && !function.params.distinct
        && function.params.filter.is_none()
        && function.params.order_by.is_empty()
        && function.params.null_treatment.is_none()
        && matches!(
            function.params.args.as_slice(),
            [Expr::Literal(value, _)] if !value.is_null()
        )
}

fn worth_rewriting(metadata: &GroupedCountMetadata) -> bool {
    if metadata.metadata_file_count == 0 {
        return false;
    }
    if metadata.residual_file_indices.is_empty()
        || metadata.metadata_row_count >= LARGE_METADATA_SAVINGS_ROWS
    {
        return true;
    }
    let residual_files = metadata.residual_file_indices.len();
    let substantial_file_reduction =
        metadata.metadata_file_count > residual_files.saturating_mul(2);
    let substantial_byte_reduction = metadata.metadata_bytes >= LARGE_METADATA_SAVINGS_BYTES;
    (metadata.metadata_file_count >= 2 || substantial_byte_reduction)
        && (substantial_file_reduction || substantial_byte_reduction)
}

fn weighted_projection_expressions(group_expr: &[Expr]) -> Vec<Expr> {
    let mut expressions = group_expr.to_vec();
    expressions.push(Expr::Literal(ScalarValue::Int64(Some(1)), None).alias(COUNT_WEIGHT_COLUMN));
    expressions
}

fn build_metadata_branch(
    rows: Vec<GroupedCountMetadataRow>,
    weighted_schema: &datafusion::common::DFSchemaRef,
) -> Result<Option<LogicalPlan>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let values = rows
        .into_iter()
        .map(|row| {
            let mut values = row
                .group_values
                .into_iter()
                .map(|value| Expr::Literal(value, None))
                .collect::<Vec<_>>();
            values.push(Expr::Literal(ScalarValue::Int64(Some(row.count)), None));
            values
        })
        .collect::<Vec<_>>();
    let values = LogicalPlanBuilder::values(values)?.build()?;
    let projection = values
        .schema()
        .columns()
        .into_iter()
        .zip(weighted_schema.fields())
        .map(|(column, field)| Expr::Column(column).alias(field.name()))
        .collect::<Vec<_>>();
    Ok(Some(LogicalPlan::Projection(Projection::try_new(
        projection,
        Arc::new(values),
    )?)))
}
