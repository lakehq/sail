use arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, try_cast, ExprSchemable};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let mut when_then_expr = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        if let Some(result) = iter.next() {
            when_then_expr.push((Box::new(condition), Box::new(result)));
        } else {
            when_then_expr.push((Box::new(lit(true)), Box::new(condition)));
            break;
        }
    }
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr: None,
    }))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn is_string_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Returns `true` for temporal types whose presence, together with at least
/// one String argument, triggers Spark-compatible coercion in `coalesce`.
fn is_temporal_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

/// Spark-compatible coalesce that handles mixed String and Date/Timestamp arguments.
///
/// In Spark, `coalesce(string_col, date_col)` coerces all arguments to the temporal
/// type (the tightest common type). String values that cannot be cast to the temporal
/// type are treated as null via `TryCast`, matching Spark's non-ANSI cast semantics.
fn spark_coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let schema = function_context.schema;
    let types: Vec<DataType> = arguments
        .iter()
        .map(|arg| arg.get_type(schema.as_ref()))
        .collect::<Result<_, _>>()?;

    let has_string = types.iter().any(is_string_type);
    let has_temporal = types.iter().any(is_temporal_type);

    // When String and temporal types are mixed, cast all string arguments to the
    // temporal type so the result preserves the temporal type. This matches
    // Spark's coercion behavior where the tightest common type wins.
    // Invalid strings become null via TryCast (Spark non-ANSI cast semantics).
    let arguments = if has_string && has_temporal {
        let target_temporal = types
            .iter()
            .find(|t| is_temporal_type(t))
            .cloned()
            .unwrap_or(DataType::Date32);

        arguments
            .into_iter()
            .zip(types.iter())
            .map(|(arg, dt)| {
                if is_string_type(dt) {
                    try_cast(arg, target_temporal.clone())
                } else {
                    arg
                }
            })
            .collect()
    } else {
        arguments
    };

    Ok(expr_fn::coalesce(arguments))
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(spark_coalesce)),
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
