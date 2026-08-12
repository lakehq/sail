use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{ExprSchemable, Operator, ScalarUDF, cast, expr, lit, not};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::predicate::rewrite_like_pattern::RewriteLikePatternFunc;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use super::datetime::timezone_cast;
use super::string::stringify_ltz;
use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

pub(super) fn is_temporal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

pub(super) fn common_temporal_type(data_types: &[DataType]) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("UTC")),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(|data_type| matches!(data_type, DataType::Date32 | DataType::Date64))
            .then_some(DataType::Date32)
    }
}

fn comparison_temporal_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if !(is_temporal_type(left) || is_temporal_type(right)) {
        return None;
    }
    if !matches!(left, DataType::Null) && !is_temporal_type(left) && !left.is_string() {
        return None;
    }
    if !matches!(right, DataType::Null) && !is_temporal_type(right) && !right.is_string() {
        return None;
    }

    common_temporal_type(&[left.clone(), right.clone()])
}

pub(crate) fn coerce_temporal_expr(
    expression: expr::Expr,
    source_type: &DataType,
    target_type: &DataType,
    config: &PlanConfig,
) -> PlanResult<expr::Expr> {
    if source_type == target_type {
        return Ok(expression);
    }
    if source_type.is_string() {
        return match target_type {
            DataType::Date32 => {
                Ok(ScalarUDF::from(SparkDate::new(!config.ansi_mode)).call(vec![expression]))
            }
            DataType::Timestamp(_, timezone) => Ok(ScalarUDF::from(SparkTimestamp::try_new(
                timezone
                    .as_ref()
                    .map(|_| Arc::clone(&config.session_timezone)),
                config.ansi_mode,
                false,
            )?)
            .call(vec![expression])),
            _ => Ok(cast(expression, target_type.clone())),
        };
    }
    if matches!(
        (source_type, target_type),
        (
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
            DataType::Timestamp(_, Some(_)),
        ) | (
            DataType::Timestamp(_, Some(_)),
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
        )
    ) {
        return Ok(timezone_cast(
            expression,
            target_type.clone(),
            &config.session_timezone,
            false,
        ));
    }
    Ok(cast(expression, target_type.clone()))
}

pub(crate) fn coerce_temporal_comparison(
    left: expr::Expr,
    right: expr::Expr,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    let left_type = left.get_type(schema)?;
    let right_type = right.get_type(schema)?;
    let Some(target_type) = comparison_temporal_type(&left_type, &right_type) else {
        return Ok((left, right));
    };
    Ok((
        coerce_temporal_expr(left, &left_type, &target_type, config)?,
        coerce_temporal_expr(right, &right_type, &target_type, config)?,
    ))
}

pub(crate) fn coerce_temporal_in_list(
    value: expr::Expr,
    list: Vec<expr::Expr>,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> PlanResult<(expr::Expr, Vec<expr::Expr>)> {
    let mut expressions = Vec::with_capacity(list.len() + 1);
    expressions.push(value);
    expressions.extend(list);
    let data_types = expressions
        .iter()
        .map(|expression| expression.get_type(schema))
        .collect::<Result<Vec<_>, _>>()?;
    let has_temporal = data_types.iter().any(is_temporal_type);
    if !has_temporal {
        let value = expressions.remove(0);
        return Ok((value, expressions));
    }

    if data_types.iter().any(DataType::is_string)
        && !config.ansi_mode
        && data_types.iter().all(|data_type| !data_type.is_nested())
    {
        let expressions = expressions
            .into_iter()
            .zip(data_types)
            .map(|(expression, data_type)| {
                if data_type.is_string() {
                    expression
                } else {
                    ScalarUDF::from(SparkToUtf8::new(Arc::clone(&config.session_timezone)))
                        .call(vec![expression])
                }
            })
            .collect::<Vec<_>>();
        let mut expressions = expressions.into_iter();
        let value = expressions
            .next()
            .ok_or_else(|| PlanError::invalid("IN requires a value expression"))?;
        return Ok((value, expressions.collect()));
    }

    if data_types.iter().all(|data_type| {
        is_temporal_type(data_type)
            || (config.ansi_mode && data_type.is_string())
            || matches!(data_type, DataType::Null)
    }) {
        let target_type = if data_types
            .iter()
            .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
        {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        } else if data_types
            .iter()
            .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
        {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        } else {
            DataType::Date32
        };
        let expressions = expressions
            .into_iter()
            .zip(data_types)
            .map(|(expression, data_type)| {
                coerce_temporal_expr(expression, &data_type, &target_type, config)
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let mut expressions = expressions.into_iter();
        let value = expressions
            .next()
            .ok_or_else(|| PlanError::invalid("IN requires a value expression"))?;
        Ok((value, expressions.collect()))
    } else {
        let value = expressions.remove(0);
        Ok((value, expressions))
    }
}

fn binary_comparison(input: ScalarFunctionInput, op: Operator) -> PlanResult<expr::Expr> {
    let (left, right) = input.arguments.two()?;
    let (left, right) = coerce_temporal_comparison(
        left,
        right,
        input.function_context.schema,
        input.function_context.plan_config,
    )?;
    Ok(expr::Expr::BinaryExpr(expr::BinaryExpr::new(
        Box::new(left),
        op,
        Box::new(right),
    )))
}

fn extract_escape_char(escape_expr: expr::Expr) -> PlanResult<Option<char>> {
    match escape_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(ref s)), _)
        | expr::Expr::Literal(ScalarValue::Utf8View(Some(ref s)), _)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(Some(ref s)), _) => {
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => Ok(Some(c)),
                _ => Err(PlanError::invalid(
                    "escape character must be a single character",
                )),
            }
        }
        _ => Err(PlanError::invalid(
            "escape character must be a string literal",
        )),
    }
}

fn build_like_expr(input: ScalarFunctionInput, case_insensitive: bool) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let n = arguments.len();
    match n {
        2 => {
            let (value, pattern) = arguments.two()?;
            let value = stringify_ltz(
                value,
                function_context.schema,
                &function_context.plan_config.session_timezone,
            )?;
            let pattern = stringify_ltz(
                pattern,
                function_context.schema,
                &function_context.plan_config.session_timezone,
            )?;
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr: Box::new(value),
                pattern: Box::new(pattern),
                case_insensitive,
                escape_char: None,
            }))
        }
        3 => {
            let (value, pattern, escape) = arguments.three()?;
            let value = stringify_ltz(
                value,
                function_context.schema,
                &function_context.plan_config.session_timezone,
            )?;
            let pattern = stringify_ltz(
                pattern,
                function_context.schema,
                &function_context.plan_config.session_timezone,
            )?;
            let escape_char = extract_escape_char(escape)?;
            // Arrow's LIKE kernel only supports `\` as the escape character.
            // For any other escape, wrap the pattern in a UDF that rewrites
            // it so Arrow sees `\` as the effective escape; then build an
            // `Expr::Like` with `escape_char: Some('\\')` to be explicit
            // about the escape that the rewritten pattern actually uses.
            let (pattern, escape_char) = match escape_char {
                Some(c) if c != '\\' => {
                    let rewritten = expr::Expr::ScalarFunction(expr::ScalarFunction {
                        func: Arc::new(ScalarUDF::from(RewriteLikePatternFunc::new())),
                        args: vec![pattern, lit(c.to_string())],
                    });
                    (rewritten, Some('\\'))
                }
                _ => (pattern, escape_char),
            };
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr: Box::new(value),
                pattern: Box::new(pattern),
                case_insensitive,
                escape_char,
            }))
        }
        _ => Err(PlanError::invalid(format!(
            "like/ilike expects 2 or 3 arguments, got {n}"
        ))),
    }
}

fn rlike(expr: expr::Expr, pattern: expr::Expr) -> expr::Expr {
    expr::Expr::SimilarTo(expr::Like {
        negated: false,
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        case_insensitive: false,
        escape_char: None,
    })
}

fn build_rlike(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, pattern) = arguments.two()?;
    let value = stringify_ltz(
        value,
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;
    let pattern = stringify_ltz(
        pattern,
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;
    Ok(rlike(value, pattern))
}

fn is_in_list(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, list) = arguments.at_least_one()?;
    let (value, list) = coerce_temporal_in_list(
        value,
        list,
        function_context.schema,
        function_context.plan_config,
    )?;
    Ok(expr::Expr::InList(expr::InList {
        expr: Box::new(value),
        list,
        negated: false,
    }))
}

pub(super) fn list_built_in_predicate_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("!", F::unary(not)),
        (
            "!=",
            F::custom(|input| binary_comparison(input, Operator::NotEq)),
        ),
        (
            "<",
            F::custom(|input| binary_comparison(input, Operator::Lt)),
        ),
        (
            "<=",
            F::custom(|input| binary_comparison(input, Operator::LtEq)),
        ),
        (
            "<=>",
            F::custom(|input| binary_comparison(input, Operator::IsNotDistinctFrom)),
        ),
        (
            "=",
            F::custom(|input| binary_comparison(input, Operator::Eq)),
        ),
        (
            "==",
            F::custom(|input| binary_comparison(input, Operator::Eq)),
        ),
        (
            ">",
            F::custom(|input| binary_comparison(input, Operator::Gt)),
        ),
        (
            ">=",
            F::custom(|input| binary_comparison(input, Operator::GtEq)),
        ),
        ("and", F::binary_op(Operator::And)),
        ("ilike", F::custom(|input| build_like_expr(input, true))),
        // TODO:
        //  If we want to prevent `IN` as a function in SQL,
        //  we can remove that from the built-in functions,
        //  and instead resolve it to spec::Expr::InList in the proto converter.
        ("in", F::custom(is_in_list)), // Spark passes isin as in
        ("isnan", F::unary(expr_fn::isnan)),
        (
            "isnotnull",
            F::unary(|x| expr::Expr::IsNotNull(Box::new(x))),
        ),
        ("isnull", F::unary(|x| expr::Expr::IsNull(Box::new(x)))),
        ("like", F::custom(|input| build_like_expr(input, false))),
        ("not", F::unary(not)),
        ("or", F::binary_op(Operator::Or)),
        ("regexp", F::custom(build_rlike)),
        ("regexp_like", F::custom(build_rlike)),
        ("rlike", F::custom(build_rlike)),
    ]
}
