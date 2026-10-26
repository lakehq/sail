use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::predicate::rewrite_like_pattern::RewriteLikePatternFunc;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::update_struct_field::UpdateStructField;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind, ExprType, FunctionArg, QuerySchema};
use crate::analyzer::typing::predicate::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn resolve_pattern(
    mut input: ScalarFunctionInput<'_>,
    name: &str,
    regex: bool,
) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, mut pattern, escape) = match values.len() {
        2 => {
            let (a, b) = values.two()?;
            (a, b, None)
        }
        3 if !regex => {
            let (a, b, c) = values.three()?;
            (a, b, Some(c))
        }
        n => {
            return Err(PlanError::invalid(format!(
                "{name} has an invalid argument count: {n}"
            )));
        }
    };
    let mut escape_char = None;
    if let Some(escape) = escape {
        let escape = crate::analyzer::lowering::evaluate_constant(&escape)?;
        let value = match escape {
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)) => value,
            _ => {
                return Err(PlanError::invalid(
                    "escape character must be a string literal",
                ));
            }
        };
        let mut chars = value.chars();
        let (Some(character), None) = (chars.next(), chars.next()) else {
            return Err(PlanError::invalid(
                "escape character must be a single character",
            ));
        };
        escape_char = Some(character);
        if character != '\\' {
            let character = context.literal(ScalarValue::Utf8(Some(character.to_string())));
            pattern = context.scalar_udf(
                Arc::new(ScalarUDF::from(RewriteLikePatternFunc::new())),
                vec![pattern, character],
            )?;
            escape_char = Some('\\');
        }
    }
    crate::function::scalar::predicate::pattern(
        context,
        value,
        pattern,
        regex,
        name == "ilike",
        escape_char,
    )
}

pub(crate) fn pattern(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    pattern: Expr,
    regex: bool,
    case_insensitive: bool,
    escape_char: Option<char>,
) -> PlanResult<Expr> {
    let value = context.cast(value, DataType::Utf8)?;
    let pattern = context.cast(pattern, DataType::Utf8)?;
    let nullable = value.result_type.resolved("pattern value")?.nullable
        || pattern.result_type.resolved("pattern")?.nullable;
    let node = if regex {
        ExprKind::SimilarTo {
            expression: Box::new(value),
            pattern: Box::new(pattern),
            negated: false,
            escape_char,
            case_insensitive,
        }
    } else {
        ExprKind::Like {
            expression: Box::new(value),
            pattern: Box::new(pattern),
            negated: false,
            escape_char,
            case_insensitive,
        }
    };
    Ok(Expr {
        id: context.ids.expression(),
        kind: Deferred::Resolved(node),
        result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, nullable)),
    })
}

pub(crate) fn predicate_pair(
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
            left = predicate_string(context, left)?;
            return Ok((left, right));
        }
        if l.is_string() && is_date_or_timestamp_ltz(&r) {
            right = predicate_string(context, right)?;
            return Ok((left, right));
        }
    }
    if let Some(timezone) = timestamp_parse_timezone_for_type(&l, &config.session_timezone)
        && r.is_string()
    {
        right = predicate_timestamp(context, right, timezone)?;
    } else if let Some(timezone) = timestamp_parse_timezone_for_type(&r, &config.session_timezone)
        && l.is_string()
    {
        left = predicate_timestamp(context, left, timezone)?;
    }
    Ok((left, right))
}

pub(crate) fn predicate_timestamp(
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

pub(crate) fn predicate_string(
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
    let metadata = interval_metadata(&value, &field.data_type)?;
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
        .map(|value| in_coercion(context, value, &policy))
        .collect()
}

pub(crate) fn in_coercion(
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
                predicate_string(context, value)
            }
        }
        TimestampStringInCoercion::ToTimestamp(timezone) => {
            if data_type.is_string() {
                return predicate_timestamp(context, value, timezone.clone());
            }
            if timezone.is_some()
                && matches!(
                    data_type,
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
                )
            {
                let value = predicate_string(context, value)?;
                return predicate_timestamp(context, value, timezone.clone());
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
            let body = in_coercion(context, variable, element)?;
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
                let field = context.scalar("get_field", vec![copied, key])?;
                let field = in_coercion(context, field, policy)?;
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

pub(crate) fn in_list(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    list: Vec<Expr>,
    negated: bool,
) -> PlanResult<Expr> {
    let compare = matches!(value.result_type.resolved("IN input")?.data_type, DataType::List(_) | DataType::LargeList(_))
        && list.iter().any(null_literal)
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

fn null_literal(value: &Expr) -> bool {
    match &value.kind {
        Deferred::Resolved(ExprKind::Literal(value)) => value.is_null(),
        Deferred::Resolved(
            ExprKind::Cast { expression, .. } | ExprKind::WithMetadata(expression),
        ) => null_literal(expression),
        _ => false,
    }
}

fn interval_metadata(
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
        if let Some(metadata) = interval_metadata(&child, data_type)? {
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

fn resolve_not(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let value = input.arguments.one()?;
    let nullable = value.result_type.resolved("predicate input")?.nullable;
    let expression = input.context.cast(value, DataType::Boolean)?;
    Ok(Expr {
        id: input.context.ids.expression(),
        kind: Deferred::Resolved(ExprKind::Not(Box::new(expression))),
        result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, nullable)),
    })
}

pub(super) fn list_built_in_predicate_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("!", F::custom(resolve_not)),
        ("!=", F::binary_op(Operator::NotEq)),
        ("<", F::binary_op(Operator::Lt)),
        ("<=", F::binary_op(Operator::LtEq)),
        ("<=>", F::binary_op(Operator::IsNotDistinctFrom)),
        ("=", F::binary_op(Operator::Eq)),
        ("==", F::binary_op(Operator::Eq)),
        (">", F::binary_op(Operator::Gt)),
        (">=", F::binary_op(Operator::GtEq)),
        ("and", F::binary_op(Operator::And)),
        (
            "ilike",
            F::custom(|input| resolve_pattern(input, "ilike", false)),
        ),
        (
            "in",
            F::custom(|mut input| {
                let (value, list) = input.arguments.at_least_one()?;
                in_list(&mut input.context, value, list, false)
            }),
        ),
        ("isnan", F::kernel("isnan")),
        (
            "isnotnull",
            F::custom(|mut input| Ok(input.context.is_not_null(input.arguments.one()?))),
        ),
        (
            "isnull",
            F::custom(|mut input| Ok(input.context.is_null(input.arguments.one()?))),
        ),
        (
            "like",
            F::custom(|input| resolve_pattern(input, "like", false)),
        ),
        ("not", F::custom(resolve_not)),
        ("or", F::binary_op(Operator::Or)),
        (
            "regexp",
            F::custom(|input| resolve_pattern(input, "regexp", true)),
        ),
        (
            "regexp_like",
            F::custom(|input| resolve_pattern(input, "regexp_like", true)),
        ),
        (
            "rlike",
            F::custom(|input| resolve_pattern(input, "rlike", true)),
        ),
        ("<>", F::binary_op(Operator::NotEq)),
        ("between", F::kernel("between")),
    ]
}
