use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

use crate::analyzer::model::Expr;
use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn cast_to_date(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let arg = input.arguments.one()?;
    let data_type = &arg.result_type.resolved("conversion input")?.data_type;
    if data_type.is_string() {
        context.scalar_udf(Arc::new(ScalarUDF::from(SparkDate::new(false))), vec![arg])
    } else {
        context.cast(arg, DataType::Date32)
    }
}

fn cast_to_time(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let arg = input.arguments.one()?;
    let data_type = &arg.result_type.resolved("conversion input")?.data_type;
    if data_type.is_string() {
        context.scalar_udf(Arc::new(ScalarUDF::from(SparkTime::new(false))), vec![arg])
    } else {
        context.cast(arg, DataType::Time64(TimeUnit::Microsecond))
    }
}

fn cast_to_timestamp(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let arg = input.arguments.one()?;
    let data_type = &arg.result_type.resolved("conversion input")?.data_type;
    if data_type.is_string() {
        context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                Some(context.config.session_timezone.clone()),
                context.config.ansi_mode,
                false,
            )?)),
            vec![arg],
        )
    } else {
        context.cast(arg, DataType::Timestamp(TimeUnit::Microsecond, None))
    }
}

pub(super) fn list_built_in_conversion_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("bigint", F::cast(DataType::Int64)),
        ("binary", F::cast(DataType::Binary)),
        ("boolean", F::cast(DataType::Boolean)),
        ("cast", F::kernel("cast")),
        ("date", F::custom(cast_to_date)),
        ("decimal", F::cast(DataType::Decimal128(10, 0))),
        ("double", F::cast(DataType::Float64)),
        ("float", F::cast(DataType::Float32)),
        ("int", F::cast(DataType::Int32)),
        ("smallint", F::cast(DataType::Int16)),
        ("string", F::cast(DataType::Utf8)),
        ("time", F::custom(cast_to_time)),
        ("timestamp", F::custom(cast_to_timestamp)),
        ("tinyint", F::cast(DataType::Int8)),
    ]
}
