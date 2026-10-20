use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, Result as DataFusionResult};
use datafusion_expr::expr::{BinaryExpr, HigherOrderFunction, InList, Lambda, LambdaVariable};
use datafusion_expr::{ExprSchemable, HigherOrderUDF, ScalarUDF, cast, expr, lit};
use datafusion_expr_common::operator::Operator;
use sail_common::spec;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_struct_cast::SparkStructCast;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::function::datetime_utils::{cast_temporal, common_temporal_type};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

static SPARK_IN_ARRAY_TRANSFORM_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new())));

enum TimestampStringInCoercion {
    ToString,
    ToTimestamp(Option<Arc<str>>),
    List {
        null_input_type: DataType,
        element: Box<TimestampStringInCoercion>,
    },
    Struct {
        null_input_type: DataType,
        field_names: Vec<String>,
        fields: Vec<Option<TimestampStringInCoercion>>,
    },
}

fn find_timestamp_string_in_coercion(
    data_types: &[DataType],
    session_timezone: &Arc<str>,
    ansi_mode: bool,
    case_sensitive: bool,
) -> Option<TimestampStringInCoercion> {
    let non_null_types = data_types
        .iter()
        .filter(|data_type| !data_type.is_null())
        .cloned()
        .collect::<Vec<_>>();
    let first = non_null_types.first()?;

    match first {
        DataType::List(_) | DataType::LargeList(_) => {
            let fields = non_null_types
                .iter()
                .map(|data_type| match data_type {
                    DataType::List(field) | DataType::LargeList(field) => Some(field.as_ref()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let element_types = fields
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>();
            let element = find_timestamp_string_in_coercion(
                &element_types,
                session_timezone,
                ansi_mode,
                case_sensitive,
            )?;
            return Some(TimestampStringInCoercion::List {
                null_input_type: first.clone(),
                element: Box::new(element),
            });
        }
        DataType::Struct(first_fields) => {
            let structs = non_null_types
                .iter()
                .map(|data_type| match data_type {
                    DataType::Struct(fields) => Some(fields),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            if structs.iter().any(|fields| {
                fields.len() != first_fields.len()
                    || fields
                        .iter()
                        .zip(first_fields.iter())
                        .any(|(field, first)| {
                            !struct_field_names_equal(field.name(), first.name(), case_sensitive)
                        })
            }) {
                return None;
            }

            let fields = first_fields
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let field_types = structs
                        .iter()
                        .map(|fields| fields[index].data_type().clone())
                        .collect::<Vec<_>>();
                    find_timestamp_string_in_coercion(
                        &field_types,
                        session_timezone,
                        ansi_mode,
                        case_sensitive,
                    )
                })
                .collect::<Vec<_>>();
            if fields.iter().all(Option::is_none) {
                return None;
            }
            return Some(TimestampStringInCoercion::Struct {
                null_input_type: first.clone(),
                field_names: first_fields
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
                fields,
            });
        }
        _ => {}
    }

    let has_timestamp = non_null_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, _)));
    let has_string = non_null_types.iter().any(is_string_type);
    if !has_timestamp {
        return None;
    }

    if has_string && !ansi_mode {
        return data_types
            .iter()
            .all(non_ansi_in_string_promotable)
            .then_some(TimestampStringInCoercion::ToString);
    }

    if !data_types.iter().all(|data_type| {
        data_type.is_null()
            || data_type.is_string()
            || matches!(
                data_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            )
    }) {
        return None;
    }

    let timezone = non_null_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
        .then(|| Arc::clone(session_timezone));
    Some(TimestampStringInCoercion::ToTimestamp(timezone))
}

fn apply_timestamp_string_in_coercion(
    expression: expr::Expr,
    data_type: &DataType,
    coercion: &TimestampStringInCoercion,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
    list_depth: usize,
) -> DataFusionResult<expr::Expr> {
    if !timestamp_string_coercion_requires_rewrite(data_type, coercion) {
        return Ok(expression);
    }

    match coercion {
        TimestampStringInCoercion::ToString => {
            if data_type.is_null() {
                Ok(cast(expression, DataType::Utf8))
            } else {
                stringify_non_ansi_expression(expression, data_type, schema, session_timezone)
            }
        }
        TimestampStringInCoercion::ToTimestamp(timezone) => {
            let target_type = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
            Ok(match data_type {
                data_type if data_type.is_string() => {
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), ansi_mode, false)?)
                        .call(vec![expression])
                }
                DataType::Null => cast(expression, target_type),
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
                    cast_temporal(expression, data_type, target_type, session_timezone, false)?
                }
                _ => expression,
            })
        }
        TimestampStringInCoercion::List {
            null_input_type,
            element,
        } => {
            let mut expression = expression;
            let data_type = if data_type.is_null() {
                expression = cast(expression, null_input_type.clone());
                null_input_type
            } else {
                data_type
            };
            let field = match data_type {
                DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
                _ => return Ok(expression),
            };
            let parameter = format!("__sail_in_element_{list_depth}");
            let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                parameter.clone(),
                Some(Arc::clone(&field)),
            ));
            let body = apply_timestamp_string_in_coercion(
                variable,
                field.data_type(),
                element,
                schema,
                session_timezone,
                ansi_mode,
                list_depth + 1,
            )?;
            Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                Arc::clone(&SPARK_IN_ARRAY_TRANSFORM_UDF),
                vec![
                    expression,
                    expr::Expr::Lambda(Lambda::new(vec![parameter], body)),
                ],
            )))
        }
        TimestampStringInCoercion::Struct {
            null_input_type,
            field_names,
            fields: field_coercions,
        } => {
            let mut expression = expression;
            let data_type = if data_type.is_null() {
                expression = cast(expression, null_input_type.clone());
                null_input_type
            } else {
                data_type
            };
            let source_fields = match data_type {
                DataType::Struct(fields) => fields,
                _ => return Ok(expression),
            };

            // Bind one evaluated struct so volatile inputs cannot supply mismatched fields.
            let mut arguments = vec![expression];
            let mut output_fields = Vec::with_capacity(source_fields.len());
            for ((field, field_coercion), name) in
                source_fields.iter().zip(field_coercions).zip(field_names)
            {
                let parameter = "field".to_owned();
                let value = expr::Expr::LambdaVariable(LambdaVariable::new(
                    parameter.clone(),
                    Some(Arc::clone(field)),
                ));
                let value = if let Some(field_coercion) = field_coercion {
                    apply_timestamp_string_in_coercion(
                        value,
                        field.data_type(),
                        field_coercion,
                        schema,
                        session_timezone,
                        ansi_mode,
                        list_depth,
                    )?
                } else {
                    value
                };
                output_fields.push(Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_name(name)
                        .with_data_type(value.get_type(schema.as_ref())?)
                        .with_nullable(value.nullable(schema.as_ref())?),
                ));
                arguments.push(expr::Expr::Lambda(Lambda::new(vec![parameter], value)));
            }
            Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                Arc::new(HigherOrderUDF::new_from_impl(SparkStructCast::new(
                    output_fields.into(),
                ))),
                arguments,
            )))
        }
    }
}

fn timestamp_string_coercion_requires_rewrite(
    data_type: &DataType,
    coercion: &TimestampStringInCoercion,
) -> bool {
    match coercion {
        TimestampStringInCoercion::ToString => !is_string_type(data_type),
        TimestampStringInCoercion::ToTimestamp(timezone) => {
            data_type != &DataType::Timestamp(TimeUnit::Microsecond, timezone.clone())
        }
        TimestampStringInCoercion::List { element, .. } => {
            data_type.is_null()
                || match data_type {
                    DataType::List(field) | DataType::LargeList(field) => {
                        timestamp_string_coercion_requires_rewrite(field.data_type(), element)
                    }
                    _ => false,
                }
        }
        TimestampStringInCoercion::Struct {
            field_names,
            fields: field_coercions,
            ..
        } => {
            data_type.is_null()
                || match data_type {
                    DataType::Struct(fields) => fields.iter().enumerate().any(|(index, field)| {
                        field.name() != &field_names[index]
                            || field_coercions[index].as_ref().is_some_and(|coercion| {
                                timestamp_string_coercion_requires_rewrite(
                                    field.data_type(),
                                    coercion,
                                )
                            })
                    }),
                    _ => false,
                }
        }
    }
}

/// Applies Spark timestamp coercion to one predicate before DataFusion's generic coercion,
/// preserving session-zone conversion and Spark TimestampType's microsecond precision.
///
/// Compare the converted values, without reversing their casts against a literal bound.
/// Spark's `UnwrapCastInBinaryComparison` can change results because timezone conversions
/// are not one-to-one; a successful round trip of the bound does not make the rewrite valid:
/// - In Pacific/Apia, DATE '2011-12-30' and DATE '2011-12-31' both convert to
///   TIMESTAMP '2011-12-31 00:00:00'. Spark reports TRUE for the literal skipped date's
///   equality, but FALSE for the same date computed with date_add(..., id) at id = 0.
/// - In America/Los_Angeles, NTZ '2020-03-08 02:30:00' converts across the gap to
///   LTZ '2020-03-08 03:30:00'. Spark's rewrite instead compares the original 02:30
///   with 03:30, returning FALSE for equality (and incorrectly TRUE for < LTZ 03:00).
/// - Both LTZ instants '2020-11-01 08:30:00Z' and '2020-11-01 09:30:00Z' convert
///   to NTZ '2020-11-01 01:30:00' in Los Angeles. Reversing that equality selects
///   the first overlap instant and incorrectly rejects the second.
///
/// Sail deliberately preserves the cast results for literal and computed inputs alike,
/// including IN comparisons, instead of reproducing these Spark optimizer bugs.
pub(super) fn coerce_timestamp_string_predicate(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> PlanResult<expr::Expr> {
    Ok(match expression {
        expr::Expr::BinaryExpr(BinaryExpr {
            left,
            op:
                op @ (Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom),
            right,
        }) => {
            let (left, right) = coerce_timestamp_pair(*left, *right, op, schema, config)?;
            expr::Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
        }
        expr::Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr_type = expr.get_type(schema.as_ref())?;
            let list_types = list
                .iter()
                .map(|value| value.get_type(schema.as_ref()))
                .collect::<DataFusionResult<Vec<_>>>()?;
            let mut data_types = Vec::with_capacity(list_types.len() + 1);
            data_types.push(expr_type.clone());
            data_types.extend(list_types.iter().cloned());

            let Some(coercion) = find_timestamp_string_in_coercion(
                &data_types,
                &config.session_timezone,
                config.ansi_mode,
                config.case_sensitive,
            ) else {
                return Ok(expr::Expr::InList(InList::new(expr, list, negated)));
            };
            let has_null_list_value = list.iter().any(is_null_literal_expression);
            let recursive_list_coercion =
                matches!(&coercion, TimestampStringInCoercion::List { .. });
            let coerce = |value, data_type: &DataType| -> DataFusionResult<expr::Expr> {
                apply_timestamp_string_in_coercion(
                    value,
                    data_type,
                    &coercion,
                    schema,
                    &config.session_timezone,
                    config.ansi_mode,
                    0,
                )
            };

            let expr = coerce(*expr, &expr_type)?;
            let list = list
                .into_iter()
                .zip(list_types)
                .map(|(value, data_type)| coerce(value, &data_type))
                .collect::<DataFusionResult<Vec<_>>>()?;
            if recursive_list_coercion && has_null_list_value && !expr.is_volatile() {
                // A static nested IN set can miss an equal value when the set also contains NULL.
                // Spark defines IN as a Kleene equality disjunction for non-volatile operands.
                recursive_list_in_as_comparisons(expr, list, negated)
            } else {
                expr::Expr::InList(InList::new(Box::new(expr), list, negated))
            }
        }
        expression => expression,
    })
}

fn is_null_literal_expression(expression: &expr::Expr) -> bool {
    match expression {
        expr::Expr::Literal(value, _) => value.is_null(),
        expr::Expr::Alias(alias) => is_null_literal_expression(&alias.expr),
        expr::Expr::Cast(datafusion_expr::expr::Cast { expr, .. })
        | expr::Expr::TryCast(datafusion_expr::expr::TryCast { expr, .. }) => {
            is_null_literal_expression(expr)
        }
        _ => false,
    }
}

fn recursive_list_in_as_comparisons(
    expression: expr::Expr,
    list: Vec<expr::Expr>,
    negated: bool,
) -> expr::Expr {
    let mut comparisons = list.into_iter().map(|value| {
        expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expression.clone()),
            Operator::Eq,
            Box::new(value),
        ))
    });
    let Some(first) = comparisons.next() else {
        return expr::Expr::InList(InList::new(Box::new(expression), vec![], negated));
    };
    let combined = comparisons.fold(first, |left, right| {
        expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Or,
            Box::new(right),
        ))
    });
    if negated {
        expr::Expr::Not(Box::new(combined))
    } else {
        combined
    }
}

pub(in crate::resolver) fn coerce_timestamp_pair(
    left: expr::Expr,
    right: expr::Expr,
    operator: Operator,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> DataFusionResult<(expr::Expr, expr::Expr)> {
    let left_type = left.get_type(schema.as_ref())?;
    let right_type = right.get_type(schema.as_ref())?;

    if let Some(target) = common_temporal_type(
        &[left_type.clone(), right_type.clone()],
        &config.session_timezone,
    ) {
        return Ok((
            cast_temporal(
                left,
                &left_type,
                target.clone(),
                &config.session_timezone,
                false,
            )?,
            cast_temporal(right, &right_type, target, &config.session_timezone, false)?,
        ));
    }

    if !config.ansi_mode
        && config.legacy_type_coercion_datetime_to_string
        && is_ordering_comparison(operator)
    {
        if is_date_or_timestamp_ltz(&left_type) && is_string_type(&right_type) {
            let left =
                stringify_non_ansi_expression(left, &left_type, schema, &config.session_timezone)?;
            return Ok((left, right));
        }
        if is_string_type(&left_type) && is_date_or_timestamp_ltz(&right_type) {
            let right = stringify_non_ansi_expression(
                right,
                &right_type,
                schema,
                &config.session_timezone,
            )?;
            return Ok((left, right));
        }
    }

    if let Some(timezone) = timestamp_parse_timezone_for_type(&left_type, &config.session_timezone)
        && is_string_type(&right_type)
    {
        let right = coerce_string_to_timestamp(right, timezone, config.ansi_mode)?;
        return Ok((left, right));
    }
    if let Some(timezone) = timestamp_parse_timezone_for_type(&right_type, &config.session_timezone)
        && is_string_type(&left_type)
    {
        let left = coerce_string_to_timestamp(left, timezone, config.ansi_mode)?;
        return Ok((left, right));
    }
    Ok((left, right))
}

fn coerce_string_to_timestamp(
    expression: expr::Expr,
    timezone: Option<Arc<str>>,
    ansi_mode: bool,
) -> DataFusionResult<expr::Expr> {
    let function = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, false)?);
    Ok(function.call(vec![expression]))
}

fn stringify_non_ansi_expression(
    expression: expr::Expr,
    data_type: &DataType,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
) -> DataFusionResult<expr::Expr> {
    if is_string_type(data_type) || data_type == &DataType::Null {
        return Ok(expression);
    }
    if let Some(interval) = spark_interval_metadata_for_expression(&expression, schema)? {
        let metadata = interval
            .to_json()
            .map_err(|error| datafusion_common::DataFusionError::Plan(error.to_string()))?;
        return Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![expression, lit(metadata)]));
    }
    let expression = localize_timestamp_for_string(expression, data_type, session_timezone);
    Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![expression]))
}

pub(in crate::resolver) fn spark_interval_metadata_for_expression(
    expression: &expr::Expr,
    schema: &DFSchemaRef,
) -> DataFusionResult<Option<spec::SparkIntervalMetadata>> {
    let field = expression.to_field(schema.as_ref())?.1;
    if !matches!(
        field.data_type(),
        DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(IntervalUnit::YearMonth)
    ) {
        return Ok(None);
    }

    let interval_type = field.data_type().clone();
    let mut combined = None::<spec::SparkIntervalMetadata>;
    expression.apply(|candidate| {
        let field = candidate.to_field(schema.as_ref())?.1;
        if field.data_type() != &interval_type {
            return Ok(TreeNodeRecursion::Jump);
        }
        let Some(value) = field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let candidate = spec::SparkIntervalMetadata::from_json(value)
            .map_err(|error| datafusion_common::DataFusionError::Plan(error.to_string()))?;
        combined = Some(match combined {
            None => candidate,
            Some(current) => current.wider(candidate).ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(
                    "incompatible Spark interval metadata in expression".to_string(),
                )
            })?,
        });
        Ok(TreeNodeRecursion::Jump)
    })?;
    Ok(combined)
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

fn is_string_type(data_type: &DataType) -> bool {
    data_type.is_string()
}

/// Coerces an IN-subquery pair whose right-hand expression belongs to a separate logical plan.
pub(super) fn coerce_timestamp_in_subquery_pair(
    left: expr::Expr,
    left_type: &DataType,
    left_schema: &DFSchemaRef,
    right: expr::Expr,
    right_type: &DataType,
    right_schema: &DFSchemaRef,
    config: &PlanConfig,
) -> DataFusionResult<(expr::Expr, expr::Expr, bool, bool)> {
    let Some(coercion) = find_timestamp_string_in_coercion(
        &[left_type.clone(), right_type.clone()],
        &config.session_timezone,
        config.ansi_mode,
        config.case_sensitive,
    ) else {
        return Ok((left, right, false, false));
    };

    let left_changed = timestamp_string_coercion_requires_rewrite(left_type, &coercion);
    let right_changed = timestamp_string_coercion_requires_rewrite(right_type, &coercion);
    let left = apply_timestamp_string_in_coercion(
        left,
        left_type,
        &coercion,
        left_schema,
        &config.session_timezone,
        config.ansi_mode,
        0,
    )?;
    let right = apply_timestamp_string_in_coercion(
        right,
        right_type,
        &coercion,
        right_schema,
        &config.session_timezone,
        config.ansi_mode,
        0,
    )?;
    Ok((left, right, left_changed, right_changed))
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

fn struct_field_names_equal(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

fn is_date_or_timestamp_ltz(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, Some(_))
    )
}

fn is_ordering_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
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
            &self.config,
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
            &self.config,
        )?;
        let less_eq = coerce_timestamp_string_predicate(
            expr::Expr::BinaryExpr(BinaryExpr::new(
                Box::new(expr),
                Operator::LtEq,
                Box::new(high),
            )),
            schema,
            &self.config,
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
            &self.config,
        )?;
        let expression = expr::Expr::IsTrue(Box::new(expression));
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
            &self.config,
        )?;
        let expression = expr::Expr::IsTrue(Box::new(expression));
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
