use std::ops::Mul;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::{ExprSchemable, ScalarUDF, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use super::datetime::timezone_cast;
use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

pub(crate) fn cast_to_date(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = input.arguments.one()?;
    let data_type = arg
        .to_field(input.function_context.schema)?
        .1
        .data_type()
        .clone();
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkDate::new(false))),
            args: vec![arg],
        }))
    } else if matches!(data_type, DataType::Timestamp(_, Some(_))) {
        Ok(timezone_cast(
            arg,
            DataType::Date32,
            &input.function_context.plan_config.session_timezone,
            false,
        ))
    } else {
        Ok(expr::Expr::Cast(expr::Cast::new(
            Box::new(arg),
            DataType::Date32,
        )))
    }
}

fn cast_to_string(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = input.arguments.one()?;
    Ok(ScalarUDF::from(SparkToUtf8::new(Arc::clone(
        &input.function_context.plan_config.session_timezone,
    )))
    .call(vec![arg]))
}

fn cast_to_time(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = input.arguments.one()?;
    let data_type = arg
        .to_field(input.function_context.schema)?
        .1
        .data_type()
        .clone();
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkTime::new(false))),
            args: vec![arg],
        }))
    } else {
        Ok(expr::Expr::Cast(expr::Cast::new(
            Box::new(arg),
            DataType::Time64(TimeUnit::Microsecond),
        )))
    }
}

fn cast_to_timestamp(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = input.arguments.one()?;
    let data_type = arg
        .to_field(input.function_context.schema)?
        .1
        .data_type()
        .clone();
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                Some(input.function_context.plan_config.session_timezone.clone()),
                input.function_context.plan_config.ansi_mode,
                false,
            )?)),
            args: vec![arg],
        }))
    } else {
        let arg = if data_type.is_numeric() {
            arg.mul(lit(1_000_000_i64))
        } else {
            arg
        };
        Ok(timezone_cast(
            arg,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            &input.function_context.plan_config.session_timezone,
            false,
        ))
    }
}

pub(super) fn list_built_in_conversion_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("bigint", F::cast(DataType::Int64)),
        ("binary", F::cast(DataType::Binary)),
        ("boolean", F::cast(DataType::Boolean)),
        ("cast", F::unknown("cast")),
        ("date", F::custom(cast_to_date)),
        ("decimal", F::cast(DataType::Decimal128(10, 0))),
        ("double", F::cast(DataType::Float64)),
        ("float", F::cast(DataType::Float32)),
        ("int", F::cast(DataType::Int32)),
        ("smallint", F::cast(DataType::Int16)),
        ("string", F::custom(cast_to_string)),
        ("time", F::custom(cast_to_time)),
        ("timestamp", F::custom(cast_to_timestamp)),
        ("tinyint", F::cast(DataType::Int8)),
    ]
}
