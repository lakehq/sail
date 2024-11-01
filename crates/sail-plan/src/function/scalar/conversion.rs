use std::sync::Arc;

use arrow_cast::parse::string_to_timestamp_nanos;
use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;

use crate::error::PlanResult;
use crate::function::common::{Function, FunctionContext};
use crate::utils::ItemTaker;

fn timestamp(args: Vec<Expr>, function_context: &FunctionContext) -> PlanResult<Expr> {
    if args.len() == 1 {
        let arg = args.one()?;
        match arg {
            // FIXME: Sail's SQL parser should parse the timestamp string into a timestamp
            Expr::Literal(ScalarValue::Utf8(Some(timestamp_string))) => {
                let timestamp_micros =
                    string_to_timestamp_nanos(&timestamp_string).map(|x| x / 1_000)?;
                let timezone: Arc<str> = function_context.plan_config().time_zone.clone().into();
                Ok(Expr::Literal(ScalarValue::TimestampMicrosecond(
                    Some(timestamp_micros),
                    Some(timezone),
                )))
            }
            _ => Ok(expr_fn::to_timestamp(vec![arg])),
        }
    } else {
        Ok(expr_fn::to_timestamp_micros(args))
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
