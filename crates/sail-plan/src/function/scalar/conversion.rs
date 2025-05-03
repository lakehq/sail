use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_expr::{expr, ExprSchemable, ScalarUDF};

use crate::error::PlanResult;
use crate::extension::function::datetime::spark_date::SparkDate;
use crate::extension::function::datetime::spark_timestamp::SparkTimestamp;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::utils::ItemTaker;

fn date(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = input.arguments.one()?;
    let (data_type, _) = arg.data_type_and_nullable(input.function_context.schema)?;
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkDate::new())),
            args: vec![arg],
        }))
    } else {
        Ok(expr_fn::to_date(vec![arg]))
    }
}

fn timestamp(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arg = input.arguments.one()?;
    let (data_type, _) = arg.data_type_and_nullable(input.function_context.schema)?;
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkTimestamp::try_new(Some(
                input.function_context.plan_config.session_timezone.clone(),
            ))?)),
            args: vec![arg],
        }))
    } else {
        Ok(expr_fn::to_timestamp_micros(vec![arg]))
    }
}

pub(super) fn list_built_in_conversion_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("bigint", F::cast(DataType::Int64)),
        ("binary", F::cast(DataType::Binary)),
        ("boolean", F::cast(DataType::Boolean)),
        ("cast", F::unknown("cast")),
        ("date", F::custom(date)),
        ("decimal", F::cast(DataType::Decimal128(10, 0))),
        ("double", F::cast(DataType::Float64)),
        ("float", F::cast(DataType::Float32)),
        ("int", F::cast(DataType::Int32)),
        ("smallint", F::cast(DataType::Int16)),
        ("string", F::cast(DataType::Utf8)),
        ("timestamp", F::custom(timestamp)),
        ("tinyint", F::cast(DataType::Int8)),
    ]
}
