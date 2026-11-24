// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::{ExprSchemable, ScalarUDF, expr};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::conversion::to_string::ToUtf8;
use sail_function::scalar::datetime::date::Date;
use sail_function::scalar::datetime::time::Time;

use super::datetime::to_timestamp;
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
            func: Arc::new(ScalarUDF::from(Date::new(false))),
            args: vec![arg],
        }))
    } else {
        Ok(expr::Expr::Cast(expr::Cast::new(
            Box::new(arg),
            DataType::Date32,
        )))
    }
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
            func: Arc::new(ScalarUDF::from(Time::new(false))),
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
    if input.arguments.len() != 1 {
        return Err(crate::error::PlanError::invalid(
            "timestamp requires 1 argument",
        ));
    }
    to_timestamp(input, false)
}

fn cast_to_string(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let value = input.arguments.one()?;
    let data_type = value.get_type(input.function_context.schema)?;
    if matches!(
        data_type,
        DataType::Timestamp(_, _)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    ) {
        let timezone = Arc::clone(&input.function_context.plan_config.session_timezone);
        Ok(ScalarUDF::from(ToUtf8::new().with_timezone(Some(timezone))).call(vec![value]))
    } else {
        Ok(datafusion_expr::cast(value, DataType::Utf8))
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
