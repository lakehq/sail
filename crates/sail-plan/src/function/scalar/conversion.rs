use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::{expr, ExprSchemable, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

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
    } else {
        Ok(expr::Expr::Cast(expr::Cast::new(
            Box::new(arg),
            DataType::Date32,
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
                false,
            )?)),
            args: vec![arg],
        }))
    } else {
        Ok(expr::Expr::Cast(expr::Cast::new(
            Box::new(arg),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )))
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
        ("string", F::cast(DataType::Utf8)),
        ("timestamp", F::custom(cast_to_timestamp)),
        ("tinyint", F::cast(DataType::Int8)),
    ]
}
