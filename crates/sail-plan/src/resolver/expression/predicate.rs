use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion::functions::core;
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, Operator, ScalarUDF};
use sail_common::spec;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::update_struct_field::UpdateStructField;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::function::common::FunctionContextInput;
use crate::resolver::{NamedExpr, PlanResolver};

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
    context: &mut FunctionContextInput<'_>,
    mut value: Expr,
    policy: &TimestampStringInCoercion,
) -> PlanResult<Expr> {
    let data_type = value.result_type.resolved("IN coercion")?.data_type.clone();
    if !timestamp_string_coercion_requires_rewrite(&data_type, policy) {
        return Ok(value);
    }
    match policy {
        TimestampStringInCoercion::ToString => {
            if data_type.is_null() {
                context.cast(value, DataType::Utf8)
            } else {
                stringify_non_ansi_expression(context, value)
            }
        }
        TimestampStringInCoercion::ToTimestamp(timezone) => {
            if data_type.is_string() {
                return coerce_string_to_timestamp(context, value, timezone.clone());
            }
            if timezone.is_some()
                && matches!(
                    data_type,
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
                )
            {
                let value = stringify_non_ansi_expression(context, value)?;
                return coerce_string_to_timestamp(context, value, timezone.clone());
            }
            context.cast(
                value,
                DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
            )
        }
        TimestampStringInCoercion::List {
            null_input_type,
            element,
        } => {
            if data_type.is_null() {
                value = context.cast(value, null_input_type.clone())?;
            }
            let field = match &value.result_type.resolved("IN array")?.data_type {
                DataType::List(field) | DataType::LargeList(field) => field.clone(),
                _ => return Ok(value),
            };
            let parameter = context.attribute("element".into(), ExprType::from_field(&field));
            let variable = Expr {
                id: context.ids.expression(),
                result_type: parameter.result_type.clone(),
                kind: Deferred::Resolved(ExprKind::LambdaVariable(parameter.id)),
            };
            let body = apply_timestamp_string_in_coercion(context, variable, element)?;
            let function = Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new()));
            let fields = vec![
                datafusion_expr::ValueOrLambda::Value(
                    value.result_type.resolved("IN array")?.to_field(""),
                ),
                datafusion_expr::ValueOrLambda::Lambda(
                    body.result_type
                        .resolved("IN transformed element")?
                        .to_field(""),
                ),
            ];
            let result = crate::analyzer::typing::higher_order::resolve(
                function.as_ref(),
                &fields,
                &[None, None],
            )?;
            Ok(Expr {
                id: context.ids.expression(),
                result_type: Deferred::Resolved(ExprType::from_field(&result.result)),
                kind: Deferred::Resolved(ExprKind::HigherOrderFunction {
                    function,
                    arguments: vec![
                        FunctionArg::Value(Box::new(value)),
                        FunctionArg::Lambda {
                            id: context.ids.expression(),
                            parameters: QuerySchema {
                                attributes: vec![parameter],
                            },
                            body: Box::new(body),
                        },
                    ],
                }),
            })
        }
        TimestampStringInCoercion::Struct {
            null_input_type,
            field_names,
            fields: policies,
        } => {
            if data_type.is_null() {
                value = context.cast(value, null_input_type.clone())?;
            }
            let DataType::Struct(fields) = &value.result_type.resolved("IN struct")?.data_type
            else {
                return Ok(value);
            };
            let names = (0..fields.len())
                .map(|i| format!("__in_field_{i}"))
                .collect::<Vec<_>>();
            let fields = fields
                .iter()
                .zip(&names)
                .map(|(f, n)| Arc::new(f.as_ref().clone().with_name(n)))
                .collect::<Vec<_>>();
            value = context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkStructRename::new(DataType::Struct(
                    fields.into(),
                )))),
                vec![value],
            )?;
            for (name, policy) in names.iter().zip(policies) {
                let Some(policy) = policy else {
                    continue;
                };
                let copied = context.copy_expression(&value)?;
                let key = context.literal(ScalarValue::Utf8(Some(name.clone())));
                let field = context.scalar_udf(core::get_field(), vec![copied, key])?;
                let field = apply_timestamp_string_in_coercion(context, field, policy)?;
                value = context.scalar_udf(
                    Arc::new(ScalarUDF::from(UpdateStructField::new(vec![name.clone()]))),
                    vec![value, field],
                )?;
            }
            let DataType::Struct(fields) =
                &value.result_type.resolved("IN updated struct")?.data_type
            else {
                return Err(PlanError::internal(
                    "IN struct update did not return struct",
                ));
            };
            let fields = fields
                .iter()
                .zip(field_names)
                .map(|(f, n)| Arc::new(f.as_ref().clone().with_name(n)))
                .collect::<Vec<_>>();
            context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkStructRename::new(DataType::Struct(
                    fields.into(),
                )))),
                vec![value],
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

fn is_null_literal_expression(value: &Expr) -> bool {
    match &value.kind {
        Deferred::Resolved(ExprKind::Literal(value)) => value.is_null(),
        Deferred::Resolved(
            ExprKind::Cast { expression, .. } | ExprKind::WithMetadata(expression),
        ) => is_null_literal_expression(expression),
        _ => false,
    }
}

pub(crate) fn coerce_timestamp_pair(
    context: &mut FunctionContextInput<'_>,
    mut left: Expr,
    operator: Operator,
    mut right: Expr,
) -> PlanResult<(Expr, Expr)> {
    if !is_comparison(operator) {
        return Ok((left, right));
    }
    let l = left
        .result_type
        .resolved("predicate left")?
        .data_type
        .clone();
    let r = right
        .result_type
        .resolved("predicate right")?
        .data_type
        .clone();
    let config = context.config.clone();
    if !config.ansi_mode
        && config.legacy_type_coercion_datetime_to_string
        && is_ordering_comparison(operator)
    {
        if is_date_or_timestamp_ltz(&l) && r.is_string() {
            left = stringify_non_ansi_expression(context, left)?;
            return Ok((left, right));
        }
        if l.is_string() && is_date_or_timestamp_ltz(&r) {
            right = stringify_non_ansi_expression(context, right)?;
            return Ok((left, right));
        }
    }
    if let Some(timezone) = timestamp_parse_timezone_for_type(&l, &config.session_timezone)
        && r.is_string()
    {
        right = coerce_string_to_timestamp(context, right, timezone)?;
    } else if let Some(timezone) = timestamp_parse_timezone_for_type(&r, &config.session_timezone)
        && l.is_string()
    {
        left = coerce_string_to_timestamp(context, left, timezone)?;
    }
    Ok((left, right))
}

fn coerce_string_to_timestamp(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    timezone: Option<Arc<str>>,
) -> PlanResult<Expr> {
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
            timezone,
            context.config.ansi_mode,
            false,
        )?)),
        vec![value],
    )
}

fn stringify_non_ansi_expression(
    context: &mut FunctionContextInput<'_>,
    mut value: Expr,
) -> PlanResult<Expr> {
    let field = value
        .result_type
        .resolved("predicate string input")?
        .clone();
    if field.data_type.is_string() {
        return Ok(value);
    }
    if let DataType::Timestamp(unit, Some(_)) = field.data_type {
        value = context.cast(
            value,
            DataType::Timestamp(unit, Some(context.config.session_timezone.clone())),
        )?;
    }
    let metadata = spark_interval_metadata_for_expression(&value, &field.data_type)?;
    let mut args = vec![value];
    if let Some(metadata) = metadata {
        args.push(
            context.literal(ScalarValue::Utf8(Some(
                metadata
                    .to_json()
                    .map_err(|e| PlanError::analysis(e.to_string()))?,
            ))),
        );
    }
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), args)
}

fn spark_interval_metadata_for_expression(
    value: &Expr,
    data_type: &DataType,
) -> PlanResult<Option<sail_common::spec::SparkIntervalMetadata>> {
    use sail_common::spec::{SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata};
    let field = value.result_type.resolved("interval metadata")?;
    if &field.data_type != data_type {
        return Ok(None);
    }
    if let Some(metadata) = field.metadata.get(SAIL_SPARK_INTERVAL_METADATA_KEY) {
        return SparkIntervalMetadata::from_json(metadata)
            .map(Some)
            .map_err(|e| PlanError::analysis(e.to_string()));
    }
    let mut combined: Option<SparkIntervalMetadata> = None;
    value.clone().map_children::<PlanError>(&mut |child| {
        if let Some(metadata) = spark_interval_metadata_for_expression(&child, data_type)? {
            combined = Some(match combined {
                Some(current) => current
                    .wider(metadata)
                    .ok_or_else(|| PlanError::analysis("incompatible interval metadata"))?,
                None => metadata,
            });
        }
        Ok(child)
    })?;
    Ok(combined)
}

/// Applies Spark timestamp/string coercion to one predicate before DataFusion's generic
/// coercion, preserving session-zone parsing and Spark TimestampType's microsecond precision.
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

pub(crate) fn in_arguments(
    context: &mut FunctionContextInput<'_>,
    values: Vec<Expr>,
) -> PlanResult<Vec<Expr>> {
    let types = values
        .iter()
        .map(|v| Ok(v.result_type.resolved("IN input")?.data_type.clone()))
        .collect::<PlanResult<Vec<_>>>()?;
    let config = context.config.clone();
    let Some(policy) = find_timestamp_string_in_coercion(
        &types,
        &config.session_timezone,
        config.ansi_mode,
        config.case_sensitive,
    ) else {
        return Ok(values);
    };
    values
        .into_iter()
        .map(|value| apply_timestamp_string_in_coercion(context, value, &policy))
        .collect()
}

pub(crate) fn in_list(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    list: Vec<Expr>,
    negated: bool,
) -> PlanResult<Expr> {
    let compare = matches!(value.result_type.resolved("IN input")?.data_type, DataType::List(_) | DataType::LargeList(_))
        && list.iter().any(is_null_literal_expression)
        && !value.any(&|node| matches!(node, ExprKind::ScalarFunction { function, .. } if function.signature().volatility == datafusion_expr::Volatility::Volatile))?;
    let mut values = vec![value];
    values.extend(list);
    let mut values = in_arguments(context, values)?.into_iter();
    let value = values
        .next()
        .ok_or_else(|| PlanError::internal("missing IN input"))?;
    let list = values.collect::<Vec<_>>();
    if compare && !list.is_empty() {
        let mut combined = None;
        for item in list {
            let copied = context.copy_expression(&value)?;
            let equal = context.binary(copied, Operator::Eq, item)?;
            combined = Some(match combined {
                Some(left) => context.binary(left, Operator::Or, equal)?,
                None => equal,
            });
        }
        let result = combined.ok_or_else(|| PlanError::internal("missing IN comparison"))?;
        return if negated {
            context.not(result)
        } else {
            Ok(result)
        };
    }
    crate::analyzer::typing::resolve_in_list(
        context.ids.expression(),
        value,
        list,
        negated,
        context.ids,
    )
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn truth_test(
        &mut self,
        id: ExprId,
        expression: spec::Expr,
        value: Option<bool>,
        negated: bool,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpr> {
        let inner = self.resolve_named_expression(expression, schema).await?;
        let expression = typing::coerce(inner.expression, &DataType::Boolean, &mut self.state.ids)?;
        Ok(NamedExpr {
            name: format!(
                "({} IS {}{})",
                inner.name,
                if negated { "NOT " } else { "" },
                match value {
                    Some(true) => "TRUE",
                    Some(false) => "FALSE",
                    None => "UNKNOWN",
                }
            ),
            expression: Expr {
                id,
                kind: Deferred::Resolved(ExprKind::TruthTest {
                    expression: Box::new(expression),
                    value,
                    negated,
                }),
                result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
            },
        })
    }
}
