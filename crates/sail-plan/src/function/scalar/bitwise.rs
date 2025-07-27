use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::abs;
use datafusion_expr::{cast, expr, lit, when, ExprSchemable, Operator};

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::utils::ItemTaker;

fn bit_count(input: expr::Expr) -> expr::Expr {
    let mut count = cast(input, DataType::UInt64);

    let masks = [
        0x5555555555555555u64, // 0101...
        0x3333333333333333u64, // 0011...
        0x0F0F0F0F0F0F0F0Fu64, // 00001111...
        0x0101010101010101u64, // for sum
    ];

    // parallel reduction
    count = (count.clone() & lit(masks[0])) + ((count.clone() >> lit(1)) & lit(masks[0]));
    count = (count.clone() & lit(masks[1])) + ((count.clone() >> lit(2)) & lit(masks[1]));
    count = (count.clone() & lit(masks[2])) + ((count.clone() >> lit(4)) & lit(masks[2]));
    count = (count.clone() * lit(masks[3])) >> lit(56);

    expr::Expr::Cast(expr::Cast {
        expr: Box::new(count),
        data_type: DataType::Int32,
    })
}

fn bit_get(value: expr::Expr, position: expr::Expr) -> expr::Expr {
    cast((value >> position) & lit(1), DataType::Int8)
}

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
        ("bit_count", F::unary(bit_count)),
        ("bit_get", F::binary(bit_get)),
        ("getbit", F::binary(bit_get)),
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
