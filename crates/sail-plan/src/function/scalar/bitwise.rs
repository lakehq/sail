use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::abs;
use datafusion_expr::{cast, expr, lit, when, ExprSchemable, Operator};
use datafusion_spark::function::bitwise::expr_fn as bitwise_fn;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn shiftrightunsigned(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (value, shift) = arguments.two()?;

    let input_type = value.clone().get_type(function_context.schema)?;

    let (unsigned_type, max_const) = match input_type.clone() {
        DataType::Int32 => Ok((DataType::UInt32, u32::MAX as u64)),
        DataType::Int64 => Ok((DataType::UInt64, u64::MAX)),
        wrong_type => Err(PlanError::InvalidArgument(format!(
            "`shiftrightunsigned`: unsupported input type {wrong_type:?}"
        ))),
    }?;

    let unsigned = when(
        value.clone().lt(lit(0)),
        lit(max_const) - (abs(value.clone()) - lit(1)),
    )
    .otherwise(value.clone())?;

    Ok(cast(cast(unsigned, unsigned_type) >> shift, input_type))
}

pub(super) fn list_built_in_bitwise_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("&", F::binary_op(Operator::BitwiseAnd)),
        ("^", F::binary_op(Operator::BitwiseXor)),
        ("bit_count", F::unary(bitwise_fn::bit_count)),
        ("bitwise_not", F::unary(bitwise_fn::bitwise_not)),
        ("bit_get", F::binary(bitwise_fn::bit_get)),
        ("getbit", F::binary(bitwise_fn::bit_get)),
        ("shiftleft", F::binary_op(Operator::BitwiseShiftLeft)),
        ("<<", F::binary_op(Operator::BitwiseShiftLeft)),
        ("shiftright", F::binary_op(Operator::BitwiseShiftRight)),
        (">>", F::binary_op(Operator::BitwiseShiftRight)),
        ("shiftrightunsigned", F::custom(shiftrightunsigned)),
        (">>>", F::custom(shiftrightunsigned)),
        ("|", F::binary_op(Operator::BitwiseOr)),
        ("~", F::unary(|arg| (-arg) - lit(1))),
    ]
}
