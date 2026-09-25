//! Exact aggregate evaluation over a format-owned, fixed set of logical rows.
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result, ScalarValue};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::min_max::{Max, Min};
use datafusion::logical_expr::logical_plan::{Aggregate, EmptyRelation, Projection, TableScan};
use datafusion::logical_expr::{Expr, LogicalPlan, TableScanBuilder, TableSource};

/// The format validates row selection, deletion effects, and metric precision before
/// exposing statistics. These are source facts, not propagated physical estimates.
pub trait ExactAggregateStatistics {
    fn schema(&self) -> SchemaRef;
    fn row_count(&self) -> Option<usize>;
    fn column_statistics(&self, logical_path: &[String]) -> Option<ColumnStatistics>;
}

/// Replace exact global aggregate expressions while retaining unresolved expressions.
pub fn rewrite_aggregate(
    aggregate: &Aggregate,
    source: &dyn ExactAggregateStatistics,
) -> Result<Option<LogicalPlan>> {
    if !aggregate.group_expr.is_empty() || aggregate.aggr_expr.is_empty() {
        return Ok(None);
    }
    let Some(input) = AggregateInput::try_new(aggregate.input.as_ref()) else {
        return Ok(None);
    };
    let Some(row_count) = source.row_count() else {
        return Ok(None);
    };
    let values = aggregate
        .aggr_expr
        .iter()
        .zip(aggregate.schema.fields())
        .map(|(expression, field)| {
            exact_aggregate_value(expression, field.data_type(), &input, source, row_count)
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

pub struct AggregateInput<'a> {
    plan: &'a LogicalPlan,
    scan: &'a TableScan,
}

impl<'a> AggregateInput<'a> {
    pub fn try_new(plan: &'a LogicalPlan) -> Option<Self> {
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

    pub fn scan(&self) -> &'a TableScan {
        self.scan
    }

    pub fn source_column(
        &self,
        aggregate_column: &Column,
        source_schema: &Schema,
    ) -> Option<String> {
        let expression = resolve_input_expression(
            self.plan,
            &Expr::Column(aggregate_column.clone()),
            source_schema,
        )?;
        match expression {
            SourceExpression::Column { logical_path, .. } if logical_path.len() == 1 => {
                logical_path.into_iter().next()
            }
            _ => None,
        }
    }

    pub fn replace_source(&self, source: Arc<dyn TableSource>) -> Result<LogicalPlan> {
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
        source: &dyn ExactAggregateStatistics,
    ) -> Option<SourceExpression> {
        resolve_input_expression(self.plan, expression, source.schema().as_ref())
    }
}

#[derive(Debug, Clone)]
enum SourceExpression {
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
struct ValueStatistics {
    data_type: DataType,
    null_count: Precision<usize>,
    min_value: Precision<ScalarValue>,
    max_value: Precision<ScalarValue>,
}

fn resolve_input_expression(
    plan: &LogicalPlan,
    expression: &Expr,
    source_schema: &Schema,
) -> Option<SourceExpression> {
    match plan {
        LogicalPlan::TableScan(scan) => resolve_scan_expression(scan, expression, source_schema),
        LogicalPlan::Projection(projection) => resolve_expression(expression, &|column| {
            let index = projection.schema.index_of_column(column).ok()?;
            resolve_input_expression(
                projection.input.as_ref(),
                projection.expr.get(index)?,
                source_schema,
            )
        }),
        LogicalPlan::SubqueryAlias(alias) => resolve_expression(expression, &|column| {
            let index = alias.schema.index_of_column(column).ok()?;
            let column = alias.input.schema().columns().get(index)?.clone();
            resolve_input_expression(alias.input.as_ref(), &Expr::Column(column), source_schema)
        }),
        _ => None,
    }
}

fn resolve_scan_expression(
    scan: &TableScan,
    expression: &Expr,
    source_schema: &Schema,
) -> Option<SourceExpression> {
    resolve_expression(expression, &|column| {
        let projected_index = scan.projected_schema.index_of_column(column).ok()?;
        let source_index = match &scan.projection {
            Some(projection) => *projection.get(projected_index)?,
            None => projected_index,
        };
        let scan_field = scan.source.schema().fields().get(source_index)?.clone();
        let snapshot_field = source_schema
            .fields()
            .iter()
            .find(|field| field.name() == scan_field.name())?;
        if scan_field.data_type() != snapshot_field.data_type() {
            return None;
        }
        Some(SourceExpression::Column {
            logical_path: vec![snapshot_field.name().clone()],
            data_type: snapshot_field.data_type().clone(),
        })
    })
}

fn resolve_expression(
    expression: &Expr,
    resolve_column: &impl Fn(&Column) -> Option<SourceExpression>,
) -> Option<SourceExpression> {
    match expression {
        Expr::Alias(alias) => resolve_expression(alias.expr.as_ref(), resolve_column),
        Expr::Literal(value, _) => Some(SourceExpression::Literal(value.clone())),
        Expr::Column(column) => resolve_column(column),
        Expr::Cast(cast) => Some(SourceExpression::Cast {
            expression: Box::new(resolve_expression(cast.expr.as_ref(), resolve_column)?),
            data_type: cast.field.data_type().clone(),
        }),
        Expr::TryCast(cast) => Some(SourceExpression::Cast {
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
            let SourceExpression::Column {
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
            Some(SourceExpression::Column {
                logical_path,
                data_type: field.data_type().clone(),
            })
        }
        _ => None,
    }
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
            expression.alias(format!("__sail_residual_aggregate_{index}"))
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
    input: &AggregateInput<'_>,
    source: &dyn ExactAggregateStatistics,
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

    if function.func.inner().is::<Count>() {
        return exact_count_value(
            &function.params.args,
            function.params.distinct,
            input,
            source,
            row_count,
        );
    }
    if function.func.inner().is::<Min>() {
        return exact_extreme_value(
            function.params.args.as_slice(),
            output_type,
            input,
            source,
            row_count,
            true,
        );
    }
    if function.func.inner().is::<Max>() {
        return exact_extreme_value(
            function.params.args.as_slice(),
            output_type,
            input,
            source,
            row_count,
            false,
        );
    }
    None
}

fn exact_count_value(
    arguments: &[Expr],
    distinct: bool,
    input: &AggregateInput<'_>,
    source: &dyn ExactAggregateStatistics,
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
        let statistics = exact_value_statistics(expression, source, row_count)?;
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
    input: &AggregateInput<'_>,
    source: &dyn ExactAggregateStatistics,
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
    let statistics = exact_value_statistics(expression, source, row_count)?;
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
    expression: SourceExpression,
    source: &dyn ExactAggregateStatistics,
    row_count: usize,
) -> Option<ValueStatistics> {
    match expression {
        SourceExpression::Literal(value) => Some(literal_statistics(value, row_count)),
        SourceExpression::Column {
            logical_path,
            data_type,
        } => {
            let column = source.column_statistics(&logical_path)?;
            Some(ValueStatistics {
                data_type,
                null_count: column.null_count,
                min_value: column.min_value,
                max_value: column.max_value,
            })
        }
        SourceExpression::Cast {
            expression,
            data_type,
        } => {
            let statistics = exact_value_statistics(*expression, source, row_count)?;
            cast_value_statistics(statistics, data_type, row_count)
        }
    }
}

fn literal_statistics(value: ScalarValue, row_count: usize) -> ValueStatistics {
    let data_type = value.data_type();
    let is_null = value.is_null();
    let bound = if row_count > 0 && !is_null {
        Precision::Exact(value)
    } else {
        Precision::Absent
    };
    ValueStatistics {
        data_type,
        null_count: Precision::Exact(if is_null { row_count } else { 0 }),
        min_value: bound.clone(),
        max_value: bound,
    }
}

fn cast_value_statistics(
    statistics: ValueStatistics,
    target_type: DataType,
    row_count: usize,
) -> Option<ValueStatistics> {
    if statistics.data_type == target_type {
        return Some(statistics);
    }
    if matches!(statistics.null_count, Precision::Exact(nulls) if nulls == row_count) {
        return Some(ValueStatistics {
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
        if value.is_null() {
            return None;
        }
        return Some(ValueStatistics {
            data_type: target_type,
            null_count: statistics.null_count,
            min_value: Precision::Exact(value.clone()),
            max_value: Precision::Exact(value),
        });
    }
    if !safe_monotonic_cast(&statistics.data_type, &target_type) {
        return None;
    }
    Some(ValueStatistics {
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
