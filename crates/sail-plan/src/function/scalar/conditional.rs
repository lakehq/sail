use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::coercion::{
    build_rename_target_type, needs_struct_field_rename, spark_wider_numeric_type_of,
    spark_wider_type,
};
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
    let branch_values = widen_numeric_values(branch_values, &function_context)?;
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
    let (then_expr, else_expr) = widen_numeric_values(
        coerce_string_temporal_values(vec![then_expr, else_expr], &function_context)?,
        &function_context,
    )?
    .two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

/// `nvl`/`ifnull` are `Coalesce(Seq(left, right))` in Spark (`nullExpressions.scala:246`).
/// DataFusion's `nvl` coerces every container to `Utf8` -- `nvl(array, array)` is a STRING, and so
/// is `nvl(NULL, array('2'))` -- and a STRING is an arithmetic operand, so
/// `2 / nvl(NULL, array('2'))` resolved where Spark refuses an ARRAY. A container therefore goes
/// through `coalesce`, which keeps its type.
///
/// Scalars stay on `nvl`: `coalesce` refuses `nvl('a', 1)`, which Sail answers like Spark today.
fn nvl(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (left, right) = arguments.two()?;
    let schema = function_context.schema;
    let data_type = |expr: &expr::Expr| expr.get_type(schema).ok();
    let (left_type, right_type) = (data_type(&left), data_type(&right));
    let is_container = |expr: &expr::Expr| {
        matches!(
            expr.get_type(schema),
            Ok(DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Map(_, _)
                | DataType::Struct(_))
        )
    };
    if is_container(&left) || is_container(&right) {
        // `coalesce` alone cannot type two containers whose leaves differ -- an array of structs
        // whose leaves widen, a map whose values need a promotion, two structs whose field names
        // differ only by case -- so the common type is computed the way `findWiderTypeForTwo` does
        // and both sides are cast to it first (`TypeCoercionHelper.scala:141`).
        if let (Some(left_type), Some(right_type)) = (&left_type, &right_type)
            && left_type != right_type
            && let Some(common) = spark_wider_type(left_type, right_type)
        {
            let to_common = |expr: expr::Expr, from: &DataType| {
                let expr = if needs_struct_field_rename(from, &common) {
                    ScalarUDF::new_from_impl(SparkStructRename::new(build_rename_target_type(
                        from, &common,
                    )))
                    .call(vec![expr])
                } else {
                    expr
                };
                cast(expr, common.clone())
            };
            return Ok(expr_fn::coalesce(vec![
                to_common(left, left_type),
                to_common(right, right_type),
            ]));
        }
        let widens = match (&left_type, &right_type) {
            (Some(left_type), Some(right_type)) => coalesce_widens_leaves(left_type, right_type),
            _ => true,
        };
        if !widens {
            return Ok(expr_fn::nvl(left, right));
        }
        return Ok(expr_fn::coalesce(vec![left, right]));
    }
    // DataFusion's `nvl` coerces an INTERVAL or a TIME to `Utf8` as well, so
    // `DATE + nvl(NULL, INTERVAL '1' DAY)` was refused and `nvl(NULL, INTERVAL '1' DAY) * 2` answered
    // NULL with ANSI off, where Spark's `Coalesce` keeps the interval or the TIME. A pair with a
    // string stays on `nvl`: `coalesce` cannot type a TIME beside a string.
    let is_interval_or_time = |expr: &expr::Expr| {
        matches!(
            expr.get_type(schema),
            Ok(DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Time32(_)
                | DataType::Time64(_))
        )
    };
    let is_string = |expr: &expr::Expr| expr.get_type(schema).is_ok_and(|t| is_string_type(&t));
    if (is_interval_or_time(&left) || is_interval_or_time(&right))
        && !is_string(&left)
        && !is_string(&right)
    {
        return Ok(expr_fn::coalesce(vec![left, right]));
    }
    // DataFusion's `nvl` coerces a DATE or TIMESTAMP to `Utf8`, so a datetime pair goes through
    // `coalesce`, widened first the way Sail's `coalesce` widens it: `nvl(date, '...')` is a DATE
    // with ANSI on and a STRING with it off, as in Spark.
    // TODO: `coalesce` cannot type a TIMESTAMP beside a DATE yet, so that pair stays on `nvl`.
    let is_temporal = |t: &Option<DataType>| t.as_ref().is_some_and(is_temporal_type);
    let is_date = |t: &Option<DataType>| t.as_ref().is_some_and(is_date_type);
    let is_timestamp = |t: &Option<DataType>| matches!(t, Some(DataType::Timestamp(_, _)));
    let timestamp_beside_date = (is_timestamp(&left_type) && is_date(&right_type))
        || (is_date(&left_type) && is_timestamp(&right_type));
    if (is_temporal(&left_type) || is_temporal(&right_type)) && !timestamp_beside_date {
        let arguments = coerce_string_temporal_values(vec![left, right], &function_context)?;
        return Ok(expr_fn::coalesce(arguments));
    }
    Ok(expr_fn::nvl(left, right))
}

/// Whether `coalesce` types two containers: every pair of leaves is equal, numeric, string, timestamp
/// or NULL. A leaf pair that needs a string or datetime promotion is not widened by it yet.
fn coalesce_widens_leaves(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (
            DataType::List(left)
            | DataType::LargeList(left)
            | DataType::FixedSizeList(left, _)
            | DataType::ListView(left)
            | DataType::LargeListView(left),
            DataType::List(right)
            | DataType::LargeList(right)
            | DataType::FixedSizeList(right, _)
            | DataType::ListView(right)
            | DataType::LargeListView(right),
        ) => coalesce_widens_leaves(left.data_type(), right.data_type()),
        (DataType::Map(left, _), DataType::Map(right, _)) => {
            coalesce_widens_leaves(left.data_type(), right.data_type())
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right.iter()).all(|(left, right)| {
                    coalesce_widens_leaves(left.data_type(), right.data_type())
                })
        }
        // A container beside a scalar is refused by `coalesce`, as Spark refuses it.
        (left, right) if left.is_nested() || right.is_nested() => true,
        (left, right) => {
            left == right
                || left.is_null()
                || right.is_null()
                || (left.is_numeric() && right.is_numeric())
                || (is_string_type(left) && is_string_type(right))
                || matches!(
                    (left, right),
                    (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
                )
        }
    }
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

/// Widens the branches of a `CASE`/`IF` to their common numeric type, the way
/// `CaseWhenCoercion` does (`TypeCoercion.scala`, `findWiderCommonType`; the pairwise rule lives in
/// [`spark_wider_numeric_type_of`]). DataFusion types the
/// expression by its FIRST branch, so `CASE WHEN false THEN -2147483648 ELSE 3000000000L END`
/// declared an INT while carrying a BIGINT value: the rows were right, and the schema lied, which
/// broke `toArrow` and `CREATE TABLE AS SELECT`. Only an all-numeric set is widened here; a string
/// or a datetime beside it is the business of `coerce_string_temporal_values`, and a container has
/// no numeric common type to find.
fn widen_numeric_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if data_types.len() < 2
        || !data_types.iter().all(DataType::is_numeric)
        || data_types.windows(2).all(|pair| pair[0] == pair[1])
    {
        return Ok(arguments);
    }
    let Some(common) = spark_wider_numeric_type_of(&data_types) else {
        return Ok(arguments);
    };
    Ok(arguments
        .into_iter()
        .zip(data_types)
        .map(|(argument, data_type)| {
            if data_type == common {
                argument
            } else {
                cast(argument, common.clone())
            }
        })
        .collect())
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
        ("ifnull", F::custom(nvl)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::binary(expr_fn::nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::custom(nvl)),
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
