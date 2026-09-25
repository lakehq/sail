use std::sync::Arc;

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::error::PlanResult;
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let mut conditions = Vec::new();
    let mut branch_values = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                conditions.push(condition);
                branch_values.push(result);
            }
            _ => {
                conditions.push(lit(true));
                branch_values.push(condition);
                break;
            }
        }
    }
    let branch_values = coerce_branch_values(branch_values, &function_context)?;
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr: None,
    }))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let (then_expr, else_expr) =
        coerce_branch_values(vec![then_expr, else_expr], &function_context)?.two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn nvl2(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (tested, if_non_null, if_null) = arguments.three()?;
    let branches = coerce_branch_values(vec![if_non_null, if_null], &function_context)?;
    // Preserve NVL2's common result type before exposing a CASE to its callers.
    let common_type =
        get_coerce_type_for_case_expression(&argument_types(&branches, &function_context)?, None);
    let (mut if_non_null, mut if_null) = branches.two()?;
    if let Some(common_type) = common_type {
        if_non_null = if_non_null.cast_to(&common_type, function_context.schema)?;
        if_null = if_null.cast_to(&common_type, function_context.schema)?;
    }
    // A simple CASE preserves Spark's branch-based nullability for NVL2.
    Ok(expr::Expr::Case(expr::Case {
        expr: Some(Box::new(tested.is_not_null())),
        when_then_expr: vec![(Box::new(lit(true)), Box::new(if_non_null))],
        else_expr: Some(Box::new(if_null)),
    }))
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

/// Coerces the CASE/IF result values to a common type before building the expression,
/// since DataFusion types a CASE expression by its first non-null result value.
fn coerce_branch_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let arguments = coerce_string_temporal_values(arguments, function_context)?;
    coerce_numeric_values(arguments, function_context)
}

fn argument_types(
    arguments: &[expr::Expr],
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<DataType>> {
    Ok(arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?)
}

/// Casts numeric values to Spark's wider common type (`findWiderCommonType`).
/// Preserves DataFusion's existing nested and string/numeric coercion.
// TODO: Coerce mixed strings in ANSI mode and nested types to Spark's wider
//  common type as well.
fn coerce_numeric_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = argument_types(&arguments, function_context)?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let common_type = data_types.iter().try_fold(DataType::Null, |left, right| {
        wider_numeric_type(&left, right, ansi_mode)
    });
    let common_type = common_type.or_else(|| {
        if (data_types.iter().any(is_string_type)
            && data_types.iter().any(is_numeric_type)
            && data_types
                .iter()
                .all(|t| t.is_null() || is_numeric_type(t) || is_string_type(t)))
            || data_types.iter().all(|t| t.is_null() || t.is_nested())
        {
            // Preserve DataFusion's existing coercion for nested types and
            // string/numeric branches before an enclosing numeric CASE/IF uses their type.
            get_coerce_type_for_case_expression(&data_types, None)
        } else {
            None
        }
    });
    let Some(common_type) = common_type else {
        return Ok(arguments);
    };
    arguments
        .into_iter()
        .zip(data_types)
        .map(|(arg, data_type)| {
            if data_type.is_null() {
                // NULL values are coerced to the common type by DataFusion.
                Ok(arg)
            } else {
                // Like DataFusion's type coercion, this keeps values of the common type unchanged
                // and casts a scalar subquery inside the subquery.
                Ok(arg.cast_to(&common_type, function_context.schema)?)
            }
        })
        .collect()
}

/// Returns Spark's wider type of two numeric (or NULL) types,
/// following `findWiderTypeForTwo` in `TypeCoercion` and `AnsiTypeCoercion`.
fn wider_numeric_type(left: &DataType, right: &DataType, ansi_mode: bool) -> Option<DataType> {
    match (left, right) {
        (DataType::Null, other) | (other, DataType::Null) => {
            (other.is_null() || is_numeric_type(other)).then(|| other.clone())
        }
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            Some(wider_decimal_type((*p1, *s1), (*p2, *s2)))
        }
        (DataType::Decimal128(precision, scale), other)
        | (other, DataType::Decimal128(precision, scale)) => match other {
            DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
            _ => Some(wider_decimal_type(
                (integral_decimal_precision(other)?, 0),
                (*precision, *scale),
            )),
        },
        _ => {
            let wider = if numeric_precedence(left)? >= numeric_precedence(right)? {
                left
            } else {
                right
            };
            // In ANSI mode, Spark widens integral and FLOAT values to DOUBLE
            // to avoid losing precision.
            if ansi_mode && left != right && wider == &DataType::Float32 {
                Some(DataType::Float64)
            } else {
                Some(wider.clone())
            }
        }
    }
}

/// Follows `DecimalPrecisionTypeCoercion.widerDecimalType` in Spark,
/// which keeps the integral digits when the precision exceeds the maximum.
// TODO: Support `spark.sql.legacy.decimal.retainFractionDigitsOnTruncate`,
//  which keeps the fraction digits instead.
fn wider_decimal_type((p1, s1): (u8, i8), (p2, s2): (u8, i8)) -> DataType {
    let scale = i16::from(s1.max(s2));
    let range = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2));
    let precision = scale + range;
    let max_precision = i16::from(DECIMAL128_MAX_PRECISION);
    if precision <= max_precision {
        DataType::Decimal128(precision as u8, scale as i8)
    } else {
        let scale = (scale - (precision - max_precision)).max(0);
        DataType::Decimal128(DECIMAL128_MAX_PRECISION, scale as i8)
    }
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Decimal128(_, _)) || numeric_precedence(data_type).is_some()
}

/// Follows `UpCastRule.numericPrecedence` in Spark.
fn numeric_precedence(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        DataType::Float32 => Some(4),
        DataType::Float64 => Some(5),
        _ => None,
    }
}

/// Follows `DecimalType.forType` in Spark for integral types.
fn integral_decimal_precision(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(3),
        DataType::Int16 => Some(5),
        DataType::Int32 => Some(10),
        DataType::Int64 => Some(20),
        _ => None,
    }
}

fn coerce_string_temporal_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = argument_types(&arguments, function_context)?;
    let has_string = data_types.iter().any(is_string_type);
    let temporal_type =
        common_temporal_type(&data_types, &function_context.plan_config.session_timezone);
    let arguments = if has_string {
        if let Some(temporal_type) = temporal_type {
            if function_context.plan_config.ansi_mode {
                arguments
                    .into_iter()
                    .zip(data_types.iter())
                    .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
                    .collect::<PlanResult<Vec<_>>>()?
            } else {
                arguments
                    .into_iter()
                    .zip(data_types)
                    .map(|(arg, data_type)| {
                        if is_temporal_type(&data_type) {
                            ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])
                        } else {
                            arg
                        }
                    })
                    .collect()
            }
        } else {
            arguments
        }
    } else {
        arguments
    };
    Ok(arguments)
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
) -> PlanResult<expr::Expr> {
    if data_type == target_type {
        return Ok(arg);
    }
    if is_string_type(data_type) {
        match target_type {
            DataType::Date32 => Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![arg])),
            // This is only reached when ANSI mode requires a temporal common type.
            DataType::Timestamp(_, timezone) => {
                Ok(
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), true, false)?)
                        .call(vec![arg]),
                )
            }
            _ => Ok(cast(arg, target_type.clone())),
        }
    } else if is_temporal_type(data_type) {
        Ok(cast(arg, target_type.clone()))
    } else {
        Ok(arg)
    }
}

fn common_temporal_type(data_types: &[DataType], session_timezone: &Arc<str>) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(is_date_type)
            .then_some(DataType::Date32)
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || matches!(data_type, DataType::Timestamp(_, _))
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(coalesce)),
        ("if", F::custom(if_expr)),
        ("ifnull", F::binary(expr_fn::nvl)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::binary(expr_fn::nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::binary(expr_fn::nvl)),
        ("nvl2", F::custom(nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

/// Create a zero literal with the same type as the input expression
fn create_zero_literal(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(0)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::UInt8 => ScalarValue::UInt8(Some(0)),
        DataType::UInt16 => ScalarValue::UInt16(Some(0)),
        DataType::UInt32 => ScalarValue::UInt32(Some(0)),
        DataType::UInt64 => ScalarValue::UInt64(Some(0)),
        DataType::Float32 => ScalarValue::Float32(Some(0.0)),
        DataType::Float64 => ScalarValue::Float64(Some(0.0)),
        DataType::Decimal128(precision, scale) => {
            ScalarValue::Decimal128(Some(0), *precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            ScalarValue::Decimal256(Some(0.into()), *precision, *scale)
        }
        // For non-numeric types, default to Int32
        _ => ScalarValue::Int32(Some(0)),
    }
}

/// Implementation of nullifzero function with type-aware casting
fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nullif(arg, zero_literal)
    Ok(expr_fn::nullif(arg, zero_literal))
}

/// Implementation of zeroifnull function with type-aware casting
fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nvl(arg, zero_literal)
    Ok(expr_fn::nvl(arg, zero_literal))
}
