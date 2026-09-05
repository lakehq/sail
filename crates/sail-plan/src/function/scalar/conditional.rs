use std::sync::Arc;

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_common::nested_struct::{
    requires_nested_struct_cast, validate_data_type_compatibility,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_case::{SparkCase, SparkCaseCast};
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

mod coercion;
mod nullability;

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() < 2 {
        return Err(PlanError::invalid(
            "CASE requires a WHEN condition and result",
        ));
    }
    let mut conditions = Vec::new();
    let mut branch_values = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                conditions.push(condition);
                branch_values.push(result);
            }
            None => {
                // The unpaired final argument is ELSE.
                branch_values.push(condition);
                break;
            }
        }
    }
    // Spark resolves every result and validates every condition, including those
    // after a literal TRUE. Keep the full tree for child-expression validation.
    let (mut branch_values, branch_nullable) =
        coercion::coerce_case_values(branch_values, &function_context)?;
    for condition in &conditions {
        let data_type = condition.get_type(function_context.schema)?;
        if data_type != DataType::Boolean {
            return Err(PlanError::analysis(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] CASE WHEN condition must be BOOLEAN, got {data_type}"
            )));
        }
    }
    let prefix = conditions.iter().position(|condition| {
        matches!(
            condition,
            expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _)
        )
    });
    let mut nullable = prefix.is_none() && branch_values.len() == conditions.len();
    for value in branch_nullable
        .iter()
        .take(prefix.map_or(branch_values.len(), |i| i + 1))
    {
        nullable |= value;
    }
    let else_expr = if branch_values.len() > conditions.len() {
        branch_values.pop().map(Box::new)
    } else {
        None
    };
    let case = expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: conditions
            .into_iter()
            .zip(branch_values)
            .map(|(condition, value)| (Box::new(condition), Box::new(value)))
            .collect(),
        else_expr,
    });
    let case = case
        .transform_up(|value| {
            let expr::Expr::Cast(cast) = value else {
                return Ok(Transformed::no(value));
            };
            let from = cast.expr.get_type(function_context.schema)?;
            let to = cast.field.data_type();
            if requires_nested_struct_cast(&from, to) {
                validate_data_type_compatibility("", &from, to)?;
            } else if !can_cast_types(&from, to) {
                return datafusion_common::plan_err!("Unsupported CAST from {from} to {to}");
            }
            Ok(Transformed::yes(
                ScalarUDF::from(SparkCaseCast::new())
                    .call(vec![*cast.expr, lit(ScalarValue::try_from(to)?)]),
            ))
        })?
        .data;
    // DataFusion narrows CASE nullability using predicates, unlike Spark. Keep
    // Spark's schema independent of later simplification and selected-column metadata.
    Ok(ScalarUDF::from(SparkCase::new(nullable)).call(vec![case]))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let (then_expr, else_expr) =
        coerce_conditional_values(vec![then_expr, else_expr], &function_context)?.two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
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

fn coerce_conditional_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let arguments = coerce_string_temporal_values(arguments, function_context)?;
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if !data_types
        .iter()
        .all(|data_type| data_type.is_numeric() || data_type.is_null())
    {
        return Ok(arguments);
    }

    // Spark's CaseWhen/If coercion casts every result branch before its type is
    // consumed. DataFusion's uncoerced CASE reports the first non-null branch,
    // which is too early for Sail's schema, typeof, and sequence resolution.
    let has_float = data_types.iter().any(DataType::is_floating);
    let target_type = if has_float
        && (data_types.iter().any(DataType::is_decimal)
            || (function_context.plan_config.ansi_mode
                && data_types.iter().any(DataType::is_integer)))
    {
        // Spark promotes decimal/floating branches, and ANSI integral/floating
        // branches, to DOUBLE instead of DataFusion's decimal/float choice.
        Some(DataType::Float64)
    } else {
        get_coerce_type_for_case_expression(&data_types, None)
    };
    let Some(target_type) = target_type else {
        return Ok(arguments);
    };
    arguments
        .into_iter()
        .map(|arg| Ok(arg.cast_to(&target_type, function_context.schema)?))
        .collect()
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
