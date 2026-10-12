use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{ExprSchema, ScalarValue};
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
    let mut when_then_expr: Vec<_> = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    // CaseWhen.nullable ignores the missing ELSE when a branch is always true, and takes the
    // FIRST literal-true condition as the last branch that counts: every branch after it is
    // unreachable (conditionalExpressions.scala:191-200). That branch is an explicit ELSE;
    // moving it preserves every branch's type checking. The branches after it are dropped
    // only when their type is the retained one's (or an untyped NULL) and carries no field
    // metadata and their conditions are BOOLEAN, so that dropping them can neither change the
    // result type -- an interval's field range, for one, lives in metadata, not in the Arrow
    // type -- nor hide a type error Spark would report. Otherwise only a trailing `WHEN true` becomes the ELSE.
    let schema = function_context.schema;
    let is_true = |condition: &expr::Expr| {
        matches!(
            condition,
            expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _)
        )
    };
    let last_index = when_then_expr.len().saturating_sub(1);
    let first_true = when_then_expr
        .iter()
        .position(|(condition, _)| is_true(condition));
    let cut = match first_true {
        Some(index) if index < last_index => {
            let kept_type = when_then_expr[index].1.get_type(schema).ok();
            let droppable = kept_type.as_ref().is_some_and(is_plain_branch_type)
                && when_then_expr[index + 1..]
                    .iter()
                    .all(|(condition, value)| {
                        // `CaseWhen.checkInputDataTypes` requires every condition to be BOOLEAN,
                        // the ones after a `WHEN true` included (conditionalExpressions.scala:207).
                        condition
                            .get_type(schema)
                            .is_ok_and(|data_type| data_type == DataType::Boolean)
                            && value.get_type(schema).is_ok_and(|data_type| {
                                Some(&data_type) == kept_type.as_ref()
                                    || data_type == DataType::Null
                            })
                    });
            if droppable {
                Some(index)
            } else if is_true(&when_then_expr[last_index].0) {
                Some(last_index)
            } else {
                None
            }
        }
        Some(index) => Some(index),
        None => None,
    };
    let else_expr = cut.and_then(|index| {
        when_then_expr.truncate(index + 1);
        when_then_expr.pop().map(|(_, value)| value)
    });
    if when_then_expr.is_empty() {
        if let Some(value) = else_expr {
            return Ok(*value);
        }
        return Ok(lit(ScalarValue::Null));
    }
    // Spark's CaseWhen.nullable ORs the nullability of every branch VALUE and the ELSE
    // (conditionalExpressions.scala:191-200); a missing ELSE is nullable. It never asks
    // whether a branch can be reached. DataFusion's own `nullable()` drops a branch whose
    // condition cannot hold when its value is NULL (`c IS NOT NULL`, `c > 0`, a literal
    // false), so it can declare a narrower schema than Spark for the same values.
    //
    // Only the branch VALUES are inspected, never `case_expr.nullable(schema)`: that call
    // also (re-)validates the whole tree, including higher-order lambda bodies inside the
    // WHEN conditions, which defeats Sail's separate mechanism for discarding an unchecked,
    // never-evaluated higher-order return type inside a provably null-typed branch (see
    // "Null-typed predicates discard unchecked higher-order returns" in map_filter.feature).
    let spark_nullable = when_then_expr
        .iter()
        .any(|(_, value)| value.nullable(schema).unwrap_or(true))
        || else_expr
            .as_ref()
            .is_none_or(|value| value.nullable(schema).unwrap_or(true));
    let case_expr = expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr,
        else_expr,
    });
    // The wrapper changes the plan text (it survives inside aggregate arguments, window and
    // ORDER BY expressions, and into their generated names), so it is added only where it
    // changes the schema: Spark says nullable and DataFusion does not.
    if spark_nullable && !datafusion_declares_nullable(&case_expr, schema) {
        // The same `CASE WHEN true THEN <expr> END` wrapper `cast.rs` uses: its missing ELSE
        // makes the analyzed schema nullable.
        Ok(datafusion_expr::when(lit(true), case_expr).end()?)
    } else {
        Ok(case_expr)
    }
}

/// DataFusion's own nullability for `case_expr`, asked only where that is safe. The question
/// validates the whole tree, so it is never put to an expression holding a higher-order
/// function: a nested lambda body would be checked here, before Sail's own deferred check
/// ("Null-typed predicates discard unchecked higher-order returns" in map_filter.feature).
/// Whenever it cannot be answered the schema is taken as nullable, which adds no wrapper.
fn datafusion_declares_nullable(case_expr: &expr::Expr, schema: &dyn ExprSchema) -> bool {
    let has_higher_order = case_expr
        .exists(|node| Ok(matches!(node, expr::Expr::HigherOrderFunction(_))))
        .unwrap_or(true);
    has_higher_order || case_expr.nullable(schema).unwrap_or(true)
}

/// A branch type whose Arrow type is the whole Spark type: no interval field range, TIME
/// precision or other field metadata decides the result type.
fn is_plain_branch_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Date32
            | DataType::Date64
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
