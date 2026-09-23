use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::binary::type_union_coercion;
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
    let branch_values = coerce_string_temporal_values(branch_values, &function_context)?;
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    resolve_numeric_conditional(
        expr::Case {
            expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
            when_then_expr,
            else_expr: None,
        },
        &function_context,
    )
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let (then_expr, else_expr) =
        coerce_string_temporal_values(vec![then_expr, else_expr], &function_context)?.two()?;
    resolve_numeric_conditional(
        expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
            else_expr: Some(Box::new(else_expr)),
        },
        &function_context,
    )
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

fn resolve_numeric_conditional(
    case: expr::Case,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<expr::Expr> {
    let data_types = case
        .when_then_expr
        .iter()
        .map(|(_, value)| value)
        .chain(case.else_expr.iter())
        .map(|value| value.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if !data_types
        .iter()
        .all(|data_type| data_type.is_numeric() || data_type == &DataType::Null)
    {
        // TODO: Match Spark's ANSI numeric/string and recursive complex coercion.
        // Preserve the existing analyzer handling of these branches until then.
        return Ok(expr::Expr::Case(case));
    }
    let common_type = data_types.iter().try_fold(DataType::Null, |left, right| {
        conditional_common_type(&left, right, function_context.plan_config.ansi_mode)
    });
    let Some(common_type) = common_type else {
        return Ok(expr::Expr::Case(case));
    };
    if data_types.iter().all(|data_type| data_type == &common_type) {
        return Ok(expr::Expr::Case(case));
    }
    let analyzer_type = data_types.iter().try_fold(DataType::Null, |left, right| {
        type_union_coercion(&left, right)
    });
    if analyzer_type.as_ref() != Some(&common_type) {
        // TODO: Match Spark's ANSI integral/FLOAT and decimal/floating promotion,
        // and precision-38 scale reduction with HALF_UP rounding. These require
        // coercion after all branch types resolve: eager casts change numeric
        // siblings when a projected branch later resolves to STRING or DECIMAL.
        return Ok(expr::Expr::Case(case));
    }

    // Expr::Case reports the first non-null THEN type before DataFusion analysis.
    // An outer CASE exposes the common type while leaving the original CASE
    // intact for analysis. Projected branches can later resolve to STRING or
    // DECIMAL: coercing or reordering the original branches changes their values,
    // formatting, or decimal capacity. The non-null zero preserves nullability;
    // its unreachable branch is removed after analysis during simplification.
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(lit(false)),
            Box::new(lit(create_zero_literal(&common_type))),
        )],
        else_expr: Some(Box::new(expr::Expr::Case(case))),
    }))
}

fn conditional_common_type(left: &DataType, right: &DataType, ansi: bool) -> Option<DataType> {
    use DataType::*;

    match (left, right) {
        (left, right) if left == right => Some(left.clone()),
        (Null, other) | (other, Null) => Some(other.clone()),
        (Decimal128(..), Float32 | Float64) | (Float32 | Float64, Decimal128(..)) => Some(Float64),
        (Float32, Int8 | Int16 | Int32 | Int64) | (Int8 | Int16 | Int32 | Int64, Float32)
            if ansi =>
        {
            Some(Float64)
        }
        (Decimal128(..), _) | (_, Decimal128(..)) if left.is_numeric() && right.is_numeric() => {
            let decimal = |data_type: &DataType| match data_type {
                Int8 => Some((3, 0)),
                Int16 => Some((5, 0)),
                Int32 => Some((10, 0)),
                Int64 => Some((20, 0)),
                Decimal128(p, s) => Some((i16::from(*p), i16::from(*s))),
                _ => None,
            };
            match (decimal(left), decimal(right)) {
                (Some((p1, s1)), Some((p2, s2))) => {
                    // Spark's widerDecimalType and boundedPreferIntegralDigits.
                    let scale = s1.max(s2);
                    let precision = (p1 - s1).max(p2 - s2) + scale;
                    let scale = if precision > 38 {
                        (scale - (precision - 38)).max(0)
                    } else {
                        scale
                    };
                    Some(Decimal128(precision.min(38) as u8, scale as i8))
                }
                _ => type_union_coercion(left, right),
            }
        }
        // The analyzer's numeric union coercion handles the remaining widths.
        _ => type_union_coercion(left, right),
    }
}

fn coerce_string_temporal_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
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
        ("nvl2", F::ternary(expr_fn::nvl2)),
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
