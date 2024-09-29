use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_expr::expr;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::function::common::Function;
use crate::utils::ItemTaker;

fn cast(args: Vec<expr::Expr>, _config: Arc<PlanConfig>) -> PlanResult<expr::Expr> {
    let (source_expr, target_type) = args.two()?;
    match target_type {
        expr::Expr::Literal(literal) => Ok(expr::Expr::Cast(expr::Cast {
            expr: Box::new(source_expr),
            data_type: literal.data_type(),
        })),
        other => Err(PlanError::InvalidArgument(format!(
            "Cast target type must be a literal, got: {other}"
        ))),
    }
}

pub(super) fn list_built_in_conversion_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("bigint", F::cast(DataType::Int64)),
        ("binary", F::cast(DataType::Binary)),
        ("boolean", F::cast(DataType::Boolean)),
        ("cast", F::custom(cast)),
        ("date", F::var_arg(expr_fn::to_date)),
        ("decimal", F::cast(DataType::Decimal128(10, 0))),
        ("double", F::cast(DataType::Float64)),
        ("float", F::cast(DataType::Float32)),
        ("int", F::cast(DataType::Int32)),
        ("smallint", F::cast(DataType::Int16)),
        ("string", F::cast(DataType::Utf8)),
        ("timestamp", F::var_arg(expr_fn::to_timestamp_micros)),
        ("tinyint", F::cast(DataType::Int8)),
    ]
}
