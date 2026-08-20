use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalUnit};
use datafusion_common::{DFSchemaRef, Result as DataFusionResult};
use datafusion_expr::expr::{BinaryExpr, InList};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr};
use datafusion_expr_common::operator::Operator;
use sail_common::spec;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

/// Applies Spark timestamp/string coercion to one predicate before DataFusion's generic
/// coercion, preserving session-zone parsing and Spark TimestampType's microsecond precision.
pub(super) fn coerce_timestamp_string_predicate(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
) -> PlanResult<expr::Expr> {
    Ok(match expression {
        expr::Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            let (left, right) =
                coerce_timestamp_pair(*left, *right, schema, session_timezone, ansi_mode)?;
            expr::Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
        }
        expr::Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let has_timestamp = std::iter::once(expr.as_ref())
                .chain(list.iter())
                .any(|value| timestamp_parse_timezone(value, schema, session_timezone).is_some());
            let has_string = std::iter::once(expr.as_ref())
                .chain(list.iter())
                .any(|value| is_string_expression(value, schema));
            if !has_timestamp || !has_string {
                return Ok(expr::Expr::InList(InList::new(expr, list, negated)));
            }

            if !ansi_mode {
                let all_string_promotable =
                    std::iter::once(expr.as_ref())
                        .chain(list.iter())
                        .all(|value| {
                            value
                                .get_type(schema.as_ref())
                                .is_ok_and(|data_type| non_ansi_in_string_promotable(&data_type))
                        });
                if !all_string_promotable {
                    return Ok(expr::Expr::InList(InList::new(expr, list, negated)));
                }
                let expr = stringify_non_ansi_in_expression(*expr, schema, session_timezone)?;
                let list = list
                    .into_iter()
                    .map(|value| stringify_non_ansi_in_expression(value, schema, session_timezone))
                    .collect::<DataFusionResult<Vec<_>>>()?;
                return Ok(expr::Expr::InList(InList::new(
                    Box::new(expr),
                    list,
                    negated,
                )));
            }

            let Some(timezone) =
                std::iter::once(expr.as_ref())
                    .chain(list.iter())
                    .find_map(|value| {
                        timestamp_parse_timezone(value, schema, session_timezone.as_ref())
                    })
            else {
                return Ok(expr::Expr::InList(InList::new(expr, list, negated)));
            };
            let expr = coerce_string_to_timestamp(*expr, schema, timezone.clone(), ansi_mode)?;
            let list = list
                .into_iter()
                .map(|value| coerce_string_to_timestamp(value, schema, timezone.clone(), ansi_mode))
                .collect::<DataFusionResult<Vec<_>>>()?;
            expr::Expr::InList(InList::new(Box::new(expr), list, negated))
        }
        expression => expression,
    })
}

fn coerce_timestamp_pair(
    left: expr::Expr,
    right: expr::Expr,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
) -> DataFusionResult<(expr::Expr, expr::Expr)> {
    if let Some(timezone) = timestamp_parse_timezone(&left, schema, session_timezone.as_ref()) {
        let right = coerce_string_to_timestamp(right, schema, timezone, ansi_mode)?;
        return Ok((left, right));
    }
    if let Some(timezone) = timestamp_parse_timezone(&right, schema, session_timezone.as_ref()) {
        let left = coerce_string_to_timestamp(left, schema, timezone, ansi_mode)?;
        return Ok((left, right));
    }
    Ok((left, right))
}

fn coerce_string_to_timestamp(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    timezone: Option<Arc<str>>,
    ansi_mode: bool,
) -> DataFusionResult<expr::Expr> {
    if !expression
        .get_type(schema.as_ref())
        .is_ok_and(|data_type| data_type.is_string())
    {
        return Ok(expression);
    }
    let function = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, false)?);
    Ok(function.call(vec![expression]))
}

fn stringify_non_ansi_in_expression(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
) -> DataFusionResult<expr::Expr> {
    let data_type = expression.get_type(schema.as_ref())?;
    if is_string_type(&data_type) || data_type == DataType::Null {
        return Ok(expression);
    }
    let expression = localize_timestamp_for_string(expression, &data_type, session_timezone);
    Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![expression]))
}

fn localize_timestamp_for_string(
    expression: expr::Expr,
    data_type: &DataType,
    session_timezone: &Arc<str>,
) -> expr::Expr {
    match data_type {
        DataType::Timestamp(unit, Some(_)) => cast(
            expression,
            DataType::Timestamp(*unit, Some(Arc::clone(session_timezone))),
        ),
        _ => expression,
    }
}

fn non_ansi_in_string_promotable(data_type: &DataType) -> bool {
    data_type.is_null()
        || data_type.is_numeric()
        || data_type.is_string()
        || (data_type.is_temporal()
            && !matches!(data_type, DataType::Interval(IntervalUnit::MonthDayNano)))
}

fn is_string_expression(expression: &expr::Expr, schema: &DFSchemaRef) -> bool {
    expression
        .get_type(schema.as_ref())
        .is_ok_and(|data_type| is_string_type(&data_type))
}

fn is_string_type(data_type: &DataType) -> bool {
    data_type.is_string()
}

/// Coerces an IN-subquery pair whose right-hand expression belongs to a separate logical plan.
pub(super) fn coerce_timestamp_in_subquery_pair(
    left: expr::Expr,
    left_type: &DataType,
    right: expr::Expr,
    right_type: &DataType,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
) -> DataFusionResult<(expr::Expr, expr::Expr, bool, bool)> {
    if let Some(timezone) = timestamp_parse_timezone_for_type(left_type, session_timezone)
        && is_string_type(right_type)
    {
        if ansi_mode {
            let function = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, false)?);
            return Ok((left, function.call(vec![right]), false, true));
        }
        let left = localize_timestamp_for_string(left, left_type, session_timezone);
        let left = ScalarUDF::from(SparkToUtf8::new()).call(vec![left]);
        return Ok((left, right, true, false));
    }
    if let Some(timezone) = timestamp_parse_timezone_for_type(right_type, session_timezone)
        && is_string_type(left_type)
    {
        if ansi_mode {
            let function = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, false)?);
            return Ok((function.call(vec![left]), right, true, false));
        }
        let right = localize_timestamp_for_string(right, right_type, session_timezone);
        let right = ScalarUDF::from(SparkToUtf8::new()).call(vec![right]);
        return Ok((left, right, false, true));
    }
    Ok((left, right, false, false))
}

fn timestamp_parse_timezone_for_type(
    data_type: &DataType,
    session_timezone: &Arc<str>,
) -> Option<Option<Arc<str>>> {
    match data_type {
        DataType::Timestamp(_, Some(_)) => Some(Some(Arc::clone(session_timezone))),
        DataType::Timestamp(_, None) => Some(None),
        _ => None,
    }
}

fn timestamp_parse_timezone(
    expression: &expr::Expr,
    schema: &DFSchemaRef,
    session_timezone: &str,
) -> Option<Option<Arc<str>>> {
    match expression.get_type(schema.as_ref()).ok()? {
        DataType::Timestamp(_, Some(_)) => Some(Some(Arc::from(session_timezone))),
        DataType::Timestamp(_, None) => Some(None),
        _ => None,
    }
}

fn is_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

impl PlanResolver<'_> {
    // TODO: Construct better names for the expression (e.g. a IN (b, c)) for all functions below.

    pub(super) async fn resolve_expression_in_list(
        &self,
        expr: spec::Expr,
        list: Vec<spec::Expr>,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = Box::new(self.resolve_expression(expr, schema, state).await?);
        let list = self.resolve_expressions(list, schema, state).await?;
        let expression = coerce_timestamp_string_predicate(
            expr::Expr::InList(expr::InList::new(expr, list, negated)),
            schema,
            &self.config.session_timezone,
            self.config.ansi_mode,
        )?;
        Ok(NamedExpr::new(vec!["in_list".to_string()], expression))
    }

    pub(super) async fn resolve_expression_is_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_false".to_string()],
            expr::Expr::IsFalse(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_false".to_string()],
            expr::Expr::IsNotFalse(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_true".to_string()],
            expr::Expr::IsTrue(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_true".to_string()],
            expr::Expr::IsNotTrue(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_null".to_string()],
            expr::Expr::IsNull(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_null".to_string()],
            expr::Expr::IsNotNull(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_unknown".to_string()],
            expr::Expr::IsUnknown(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_unknown".to_string()],
            expr::Expr::IsNotUnknown(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_between(
        &self,
        expr: spec::Expr,
        negated: bool,
        low: spec::Expr,
        high: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let low = self.resolve_expression(low, schema, state).await?;
        let high = self.resolve_expression(high, schema, state).await?;

        // DataFusion's BETWEEN operator has a bug, so we construct the expression manually.
        let greater_eq = coerce_timestamp_string_predicate(
            expr::Expr::BinaryExpr(BinaryExpr::new(
                Box::new(expr.clone()),
                Operator::GtEq,
                Box::new(low),
            )),
            schema,
            &self.config.session_timezone,
            self.config.ansi_mode,
        )?;
        let less_eq = coerce_timestamp_string_predicate(
            expr::Expr::BinaryExpr(BinaryExpr::new(
                Box::new(expr),
                Operator::LtEq,
                Box::new(high),
            )),
            schema,
            &self.config.session_timezone,
            self.config.ansi_mode,
        )?;
        let between_expr = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(greater_eq),
            Operator::And,
            Box::new(less_eq),
        ));
        let between_expr = if negated {
            expr::Expr::Not(Box::new(between_expr))
        } else {
            between_expr
        };
        Ok(NamedExpr::new(vec!["between".to_string()], between_expr))
    }

    pub(super) async fn resolve_expression_is_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        let expression = coerce_timestamp_string_predicate(
            expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::IsDistinctFrom,
                right: Box::new(right),
            }),
            schema,
            &self.config.session_timezone,
            self.config.ansi_mode,
        )?;
        Ok(NamedExpr::new(
            vec!["is_distinct_from".to_string()],
            expression,
        ))
    }

    pub(super) async fn resolve_expression_is_not_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        let expression = coerce_timestamp_string_predicate(
            expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::IsNotDistinctFrom,
                right: Box::new(right),
            }),
            schema,
            &self.config.session_timezone,
            self.config.ansi_mode,
        )?;
        Ok(NamedExpr::new(
            vec!["is_not_distinct_from".to_string()],
            expression,
        ))
    }

    pub(super) async fn resolve_expression_similar_to(
        &self,
        expr: spec::Expr,
        pattern: spec::Expr,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let pattern = self.resolve_expression(pattern, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["similar_to".to_string()],
            expr::Expr::SimilarTo(expr::Like::new(
                negated,
                Box::new(expr),
                Box::new(pattern),
                escape_char,
                case_insensitive,
            )),
        ))
    }
}
