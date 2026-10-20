use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast, when};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};
use crate::function::datetime_utils::{cast_temporal, coerce_temporal_arguments};

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
    let fields = arguments
        .iter()
        .map(|arg| {
            arg.to_field(function_context.schema)
                .map(|(_, field)| field)
        })
        .collect::<Result<Vec<_>, _>>()?;
    with_interval_qualifier(expr_fn::coalesce(arguments), &fields, &function_context)
}

fn nullif(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (left, right) = input.arguments.two()?;
    let field = left.to_field(input.function_context.schema)?.1;
    with_interval_qualifier(
        expr_fn::nullif(left, right),
        &[field],
        &input.function_context,
    )
}

// NULLIF keeps its first argument's type; COALESCE widens its result arguments.
// Restore only known qualifiers, leaving native evaluation and nullability intact.
fn with_interval_qualifier(
    result: expr::Expr,
    fields: &[FieldRef],
    context: &FunctionContextInput<'_>,
) -> PlanResult<expr::Expr> {
    let mut interval = None::<spec::SparkIntervalMetadata>;
    for field in fields {
        if field.data_type().is_null() {
            continue;
        }
        if !matches!(
            field.data_type(),
            DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(IntervalUnit::YearMonth)
        ) {
            return Ok(result);
        }
        let Some(metadata) = field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) else {
            return Ok(result);
        };
        let candidate = spec::SparkIntervalMetadata::from_json(metadata)?;
        interval = match interval {
            Some(current) => current.wider(candidate),
            None => Some(candidate),
        };
        if interval.is_none() {
            return Ok(result);
        }
    }
    let Some(interval) = interval else {
        return Ok(result);
    };
    let field = result.to_field(context.schema)?.1;
    if fields
        .iter()
        .any(|source| !source.data_type().is_null() && source.data_type() != field.data_type())
    {
        return Ok(result);
    }
    let mut field = field.as_ref().clone();
    field.metadata_mut().insert(
        spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
        interval.to_json()?,
    );
    // A field-aware cast retains the qualifier through folding and projection.
    Ok(expr::Expr::Cast(expr::Cast::new_from_field(
        Box::new(result),
        Arc::new(field),
    )))
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
                    .map(|(arg, data_type)| {
                        coerce_to_temporal(
                            arg,
                            data_type,
                            &temporal_type,
                            &function_context.plan_config.session_timezone,
                        )
                    })
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
        coerce_temporal_arguments(arguments, function_context)?
    };
    Ok(arguments)
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
    timezone: &Arc<str>,
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
        Ok(cast_temporal(
            arg,
            data_type,
            target_type.clone(),
            timezone,
            false,
            None,
        )?)
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
        ("nullif", F::custom(nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::binary(expr_fn::nvl)),
        ("nvl2", F::ternary(expr_fn::nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let condition = match arg.get_type(function_context.schema)? {
        data_type if is_string_type(&data_type) => {
            let value = if ansi_mode {
                cast(arg.clone(), DataType::Int64)
            } else {
                ScalarUDF::from(SparkCastStringToInt32::new()).call(vec![arg.clone()])
            };
            value.eq(lit(0_i32))
        }
        DataType::Boolean if !ansi_mode => arg.clone().eq(lit(false)),
        data_type if data_type.is_numeric() || data_type == DataType::Null => {
            arg.clone().eq(lit(0_i32))
        }
        data_type => {
            return Err(PlanError::invalid(format!(
                "nullifzero cannot compare data type {data_type} with an integer zero"
            )));
        }
    };
    Ok(when(condition, lit(ScalarValue::Null)).otherwise(arg)?)
}

fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let (arg, zero) = match arg.get_type(function_context.schema)? {
        DataType::Float32 if ansi_mode => (cast(arg, DataType::Float64), lit(0_i32)),
        data_type if is_string_type(&data_type) && ansi_mode => {
            (cast(arg, DataType::Int64), lit(0_i32))
        }
        data_type if is_string_type(&data_type) => (arg, lit("0")),
        DataType::Decimal128(precision, scale) => {
            // The INT zero contributes ten integer digits. Keep integer digits when
            // the common decimal type exceeds Spark's 38-digit precision limit.
            let integer_digits = (i16::from(precision) - i16::from(scale)).max(10);
            let scale = i16::from(scale).max(0);
            let precision = (integer_digits + scale).min(38) as u8;
            let scale = scale.min((38 - integer_digits).max(0)) as i8;
            let data_type = DataType::Decimal128(precision, scale);
            let arg = if ansi_mode {
                cast(arg, data_type)
            } else {
                try_cast(arg, data_type)
            };
            (arg, lit(0_i32))
        }
        data_type if data_type.is_numeric() || data_type == DataType::Null => (arg, lit(0_i32)),
        data_type => {
            return Err(PlanError::invalid(format!(
                "zeroifnull cannot combine data type {data_type} with an integer zero"
            )));
        }
    };
    Ok(expr_fn::coalesce(vec![arg, zero]))
}
