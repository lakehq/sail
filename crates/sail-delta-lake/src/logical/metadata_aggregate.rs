use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DataFusionError, Result, ScalarValue};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::logical_plan::{
    Aggregate, EmptyRelation, Projection, TableScan, Union, Values,
};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableSource};
use log::debug;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;

use crate::logical::table_source::{DeltaFileSelection, DeltaTableSource};
use crate::snapshot::{GroupedCountMetadata, GroupedCountMetadataRow, SnapshotPruningStats};

const MAX_METADATA_GROUPS: usize = 100_000;
const LARGE_METADATA_SAVINGS_BYTES: u64 = 64 * 1024 * 1024;
const COUNT_WEIGHT_COLUMN: &str = "__sail_delta_count_weight";
const COUNT_SUM_COLUMN: &str = "__sail_delta_count_sum";

#[derive(Debug, Default)]
pub struct DeltaMetadataAggregateRewriter;

impl LogicalRewriter for DeltaMetadataAggregateRewriter {
    fn name(&self) -> &str {
        "delta_metadata_aggregate"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Aggregate(aggregate) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let rewritten = match rewrite_exact_ungrouped_aggregate(aggregate)? {
                Some(rewritten) => Some(rewritten),
                None => rewrite_grouped_count(aggregate)?,
            };
            match rewritten {
                Some(rewritten) => Ok(Transformed::yes(rewritten)),
                None => Ok(Transformed::no(plan)),
            }
        })
    }
}

enum DeltaAggregateInput<'a> {
    Scan(&'a TableScan),
    Projection {
        projection: &'a Projection,
        scan: &'a TableScan,
    },
}

impl<'a> DeltaAggregateInput<'a> {
    fn try_new(plan: &'a LogicalPlan) -> Option<Self> {
        match plan {
            LogicalPlan::TableScan(scan) => Some(Self::Scan(scan)),
            LogicalPlan::Projection(projection) => match projection.input.as_ref() {
                LogicalPlan::TableScan(scan) => Some(Self::Projection { projection, scan }),
                _ => None,
            },
            _ => None,
        }
    }

    fn scan(&self) -> &'a TableScan {
        match self {
            Self::Scan(scan) | Self::Projection { scan, .. } => scan,
        }
    }

    fn source_column(&self, aggregate_column: &Column) -> Option<String> {
        let scan_column = match self {
            Self::Scan(_) => aggregate_column,
            Self::Projection { projection, .. } => {
                let index = projection.schema.index_of_column(aggregate_column).ok()?;
                projected_column(projection.expr.get(index)?)?
            }
        };
        let scan = self.scan();
        let projected_index = scan.projected_schema.index_of_column(scan_column).ok()?;
        let source_index = match &scan.projection {
            Some(projection) => *projection.get(projected_index)?,
            None => projected_index,
        };
        scan.source
            .schema()
            .fields()
            .get(source_index)
            .map(|field| field.name().clone())
    }

    fn replace_source(&self, source: Arc<dyn TableSource>) -> Result<LogicalPlan> {
        let scan = self.scan();
        let scan = LogicalPlan::TableScan(TableScan::try_new(
            scan.table_name.clone(),
            source,
            scan.projection.clone(),
            scan.filters.clone(),
            scan.fetch,
        )?);
        match self {
            Self::Scan(_) => Ok(scan),
            Self::Projection { projection, .. } => {
                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    projection.expr.clone(),
                    Arc::new(scan),
                    Arc::clone(&projection.schema),
                )?))
            }
        }
    }

    fn source_expression(
        &self,
        expression: &Expr,
        source: &DeltaTableSource,
    ) -> Option<DeltaSourceExpression> {
        match self {
            Self::Scan(scan) => resolve_scan_expression(scan, expression, source),
            Self::Projection { projection, scan } => {
                resolve_projection_expression(projection, scan, expression, source)
            }
        }
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

fn resolve_projection_expression(
    projection: &Projection,
    scan: &TableScan,
    expression: &Expr,
    source: &DeltaTableSource,
) -> Option<DeltaSourceExpression> {
    resolve_expression(expression, &|column| {
        let index = projection.schema.index_of_column(column).ok()?;
        resolve_scan_expression(scan, projection.expr.get(index)?, source)
    })
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

fn rewrite_exact_ungrouped_aggregate(aggregate: &Aggregate) -> Result<Option<LogicalPlan>> {
    if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.is_empty() {
        return Ok(None);
    }
    let Some(input) = DeltaAggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return Ok(None);
    }
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    if !source.snapshot().load_config().require_files
        || !matches!(source.file_selection(), DeltaFileSelection::Snapshot)
    {
        return Ok(None);
    }
    let Ok(snapshot_stats) = source.snapshot().pruning_stats() else {
        return Ok(None);
    };
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
        .collect::<Option<Vec<_>>>();
    let Some(values) = values else {
        return Ok(None);
    };

    let values = values
        .into_iter()
        .map(|value| Expr::Literal(value, None))
        .collect::<Vec<_>>();
    debug!(
        "resolved {} Delta aggregate expressions from exact snapshot statistics",
        values.len()
    );
    // Values preserves the Aggregate schema by ordinal, including duplicate internal expression
    // names introduced by leaf extraction, without requiring an executable aggregate or scan.
    Ok(Some(LogicalPlan::Values(Values {
        schema: Arc::clone(&aggregate.schema),
        values: vec![values],
    })))
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

fn rewrite_grouped_count(aggregate: &Aggregate) -> Result<Option<LogicalPlan>> {
    if aggregate.group_expr.is_empty()
        || aggregate.aggr_expr.len() != 1
        || !is_row_count(&aggregate.aggr_expr[0])
    {
        return Ok(None);
    }
    let Some(input) = DeltaAggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let scan = input.scan();
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return Ok(None);
    }
    let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() else {
        return Ok(None);
    };
    if !matches!(source.file_selection(), DeltaFileSelection::Snapshot) {
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

    let Some(metadata) = source
        .snapshot()
        .grouped_count_metadata(&group_columns, MAX_METADATA_GROUPS)
    else {
        return Ok(None);
    };
    if !worth_rewriting(&metadata) {
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

    let metadata_file_count = metadata.metadata_file_count;
    let metadata_group_count = metadata.rows.len();
    let residual_file_count = metadata.residual_file_indices.len();
    let metadata_bytes = metadata.metadata_bytes;
    let residual_bytes = metadata.residual_bytes;
    let metadata_branch = build_metadata_branch(metadata.rows, &weighted_schema)?;
    let residual_branch = if metadata.residual_file_indices.is_empty() {
        None
    } else {
        let selected_source = source
            .try_select_files(metadata.residual_file_indices)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let residual_input = input.replace_source(Arc::new(selected_source))?;
        Some(LogicalPlan::Projection(Projection::try_new(
            weighted_projection,
            Arc::new(residual_input),
        )?))
    };

    let weighted_input = match (metadata_branch, residual_branch) {
        (Some(metadata), Some(residual)) => LogicalPlan::Union(Union::try_new(vec![
            Arc::new(metadata),
            Arc::new(residual),
        ])?),
        (Some(metadata), None) => metadata,
        (None, Some(residual)) => residual,
        (None, None) => {
            return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(&aggregate.schema),
            })));
        }
    };

    // Metadata rows carry a whole file's logical row count while residual rows carry one. A
    // single SUM above the union therefore reuses DataFusion's normal partial/final aggregation.
    let weighted_columns = weighted_input.schema().columns();
    let group_count = aggregate.group_expr.len();
    let group_expr = weighted_columns[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let count_weight = Expr::Column(weighted_columns[group_count].clone());
    let weighted_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(weighted_input),
        group_expr,
        vec![sum(count_weight).alias(COUNT_SUM_COLUMN)],
    )?);

    let mut output_expr = weighted_aggregate.schema().columns()[..group_count]
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let count_sum = Expr::Column(weighted_aggregate.schema().columns()[group_count].clone());
    output_expr.push(count_sum.alias(aggregate.schema.field(group_count).name()));

    debug!(
        "rewrote Delta grouped count using {metadata_file_count} metadata files ({metadata_bytes} bytes, {metadata_group_count} groups) and {residual_file_count} residual files ({residual_bytes} bytes)"
    );
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(
            output_expr,
            Arc::new(weighted_aggregate),
            Arc::clone(&aggregate.schema),
        )?,
    )))
}

fn projected_column(expression: &Expr) -> Option<&Column> {
    match expression {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => projected_column(alias.expr.as_ref()),
        _ => None,
    }
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
