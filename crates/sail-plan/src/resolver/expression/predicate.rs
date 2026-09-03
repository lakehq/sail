use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion_common::{DFSchemaRef, Result as DataFusionResult};
use datafusion_expr::expr::{BinaryExpr, HigherOrderFunction, InList, Lambda, LambdaVariable};
use datafusion_expr::{ExprSchemable, HigherOrderUDF, ScalarUDF, cast, expr, lit};
use datafusion_expr_common::operator::Operator;
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkIntervalToUtf8, SparkToUtf8};
use sail_function::scalar::update_struct_field::UpdateStructField;

use crate::config::PlanConfig;
use crate::error::PlanResult;
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
    if !has_timestamp || !has_string {
        return None;
    }

    if !ansi_mode {
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
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
                    if timezone.is_some() =>
                {
                    let string = ScalarUDF::from(SparkToUtf8::new()).call(vec![expression]);
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), ansi_mode, false)?)
                        .call(vec![string])
                }
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
                    cast(expression, target_type)
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

            let unique_names = (0..source_fields.len())
                .map(|index| format!("__sail_in_field_{index}"))
                .collect::<Vec<_>>();
            let unique_fields = source_fields
                .iter()
                .zip(&unique_names)
                .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
                .collect::<Vec<Arc<Field>>>();
            expression = ScalarUDF::from(SparkStructRename::new(DataType::Struct(Fields::from(
                unique_fields,
            ))))
            .call(vec![expression]);

            for ((field, field_coercion), unique_name) in
                source_fields.iter().zip(field_coercions).zip(&unique_names)
            {
                let Some(field_coercion) = field_coercion else {
                    continue;
                };
                let value = get_field().call(vec![expression.clone(), lit(unique_name.clone())]);
                let value = apply_timestamp_string_in_coercion(
                    value,
                    field.data_type(),
                    field_coercion,
                    schema,
                    session_timezone,
                    ansi_mode,
                    list_depth,
                )?;
                expression = ScalarUDF::from(UpdateStructField::new(vec![unique_name.clone()]))
                    .call(vec![expression, value]);
            }

            let DataType::Struct(updated_fields) = expression.get_type(schema.as_ref())? else {
                return Ok(expression);
            };
            let output_fields = updated_fields
                .iter()
                .zip(field_names)
                .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
                .collect::<Vec<Arc<Field>>>();
            Ok(
                ScalarUDF::from(SparkStructRename::new(DataType::Struct(Fields::from(
                    output_fields,
                ))))
                .call(vec![expression]),
            )
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

/// Applies Spark timestamp/string coercion to one predicate before DataFusion's generic
/// coercion, preserving session-zone parsing and Spark TimestampType's microsecond precision.
pub(super) fn coerce_timestamp_string_predicate(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> PlanResult<expr::Expr> {
    Ok(match expression {
        expr::Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
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

            let expr = apply_timestamp_string_in_coercion(
                *expr,
                &expr_type,
                &coercion,
                schema,
                &config.session_timezone,
                config.ansi_mode,
                0,
            )?;
            let list = list
                .into_iter()
                .zip(list_types)
                .map(|(value, data_type)| {
                    apply_timestamp_string_in_coercion(
                        value,
                        &data_type,
                        &coercion,
                        schema,
                        &config.session_timezone,
                        config.ansi_mode,
                        0,
                    )
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            expr::Expr::InList(InList::new(Box::new(expr), list, negated))
        }
        expression => expression,
    })
}

fn coerce_timestamp_pair(
    left: expr::Expr,
    right: expr::Expr,
    operator: Operator,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> DataFusionResult<(expr::Expr, expr::Expr)> {
    let left_type = left.get_type(schema.as_ref())?;
    let right_type = right.get_type(schema.as_ref())?;

    if !config.ansi_mode
        && config.legacy_type_coercion_datetime_to_string
        && is_ordering_comparison(operator)
    {
        if is_datetime_type(&left_type) && is_string_type(&right_type) {
            let left =
                stringify_non_ansi_expression(left, &left_type, schema, &config.session_timezone)?;
            return Ok((left, right));
        }
        if is_string_type(&left_type) && is_datetime_type(&right_type) {
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
        let metadata = serde_json::to_string(&interval).map_err(|error| {
            datafusion_common::DataFusionError::Plan(format!(
                "failed to serialize Spark interval metadata: {error}"
            ))
        })?;
        return Ok(
            ScalarUDF::from(SparkIntervalToUtf8::new()).call(vec![expression, lit(metadata)])
        );
    }
    let expression = localize_timestamp_for_string(expression, data_type, session_timezone);
    Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![expression]))
}

fn spark_interval_metadata_for_expression(
    expression: &expr::Expr,
    schema: &DFSchemaRef,
) -> DataFusionResult<Option<spec::SparkIntervalMetadata>> {
    let field = expression.to_field(schema.as_ref())?.1;
    if let Some(value) = field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) {
        return serde_json::from_str(value).map(Some).map_err(|error| {
            datafusion_common::DataFusionError::Plan(format!(
                "invalid Spark interval metadata {value:?}: {error}"
            ))
        });
    }
    if !matches!(
        field.data_type(),
        DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(IntervalUnit::YearMonth)
    ) {
        return Ok(None);
    }

    let candidates = match expression {
        expr::Expr::Alias(alias) => vec![alias.expr.as_ref()],
        expr::Expr::Negative(value)
        | expr::Expr::Cast(datafusion_expr::expr::Cast { expr: value, .. })
        | expr::Expr::TryCast(datafusion_expr::expr::TryCast { expr: value, .. }) => {
            vec![value.as_ref()]
        }
        expr::Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            vec![left.as_ref(), right.as_ref()]
        }
        expr::Expr::ScalarFunction(function) => function.args.iter().collect(),
        _ => vec![],
    };
    candidates.into_iter().try_fold(
        None::<spec::SparkIntervalMetadata>,
        |combined, candidate| {
            let Some(candidate) = spark_interval_metadata_for_expression(candidate, schema)? else {
                return Ok(combined);
            };
            Ok(match combined {
                None => Some(candidate),
                Some(current) if current.interval_unit == candidate.interval_unit => {
                    Some(spec::SparkIntervalMetadata {
                        interval_unit: current.interval_unit,
                        start_field: current.start_field.min(candidate.start_field),
                        end_field: current.end_field.max(candidate.end_field),
                    })
                }
                Some(_) => None,
            })
        },
    )
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

fn is_datetime_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

fn is_ordering_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
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
