use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;

use crate::error::PlanResult;
use crate::function::common::{Function, FunctionInput};
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

fn timestamp(input: FunctionInput) -> PlanResult<Expr> {
    if input.arguments.len() == 1 {
        let arg = input.arguments.one()?;
        match arg {
            Expr::Literal(ScalarValue::Utf8(Some(timestamp_string))) => {
                let timestamp_micros =
                    string_to_timestamp_nanos(&timestamp_string).map(|x| x / 1_000)?;
                let timezone = PlanResolver::resolve_timezone(
                    &sail_common::spec::TimeZoneInfo::SQLConfigured,
                    input.plan_config.system_timezone.as_str(),
                    &input.plan_config.timestamp_type,
                )?;
                Ok(Expr::Literal(ScalarValue::TimestampMicrosecond(
                    Some(timestamp_micros),
                    timezone,
                )))
            }
            _ => Ok(expr_fn::to_timestamp_micros(vec![arg])),
        }
    } else {
        Ok(expr_fn::to_timestamp_micros(input.arguments))
    }
}

pub(super) fn list_built_in_conversion_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("bigint", F::cast(DataType::Int64)),
        ("binary", F::cast(DataType::Binary)),
        ("boolean", F::cast(DataType::Boolean)),
        ("cast", F::unknown("cast")),
        ("date", F::cast(DataType::Date32)),
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
