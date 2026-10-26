use arrow_schema::DataType;
use datafusion::functions::math;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::analyzer::model::Expr;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::function::scalar::misc::raw_negative;

fn shiftrightunsigned(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, shift) = values.two()?;
    let target = value
        .result_type
        .resolved("unsigned shift input")?
        .data_type
        .clone();
    let (unsigned, max) = match target {
        DataType::Int32 => (DataType::UInt32, u64::from(u32::MAX)),
        DataType::Int64 => (DataType::UInt64, u64::MAX),
        _ => {
            return Err(PlanError::invalid(format!(
                "`shiftrightunsigned`: unsupported input type {target:?}"
            )));
        }
    };
    let copy = context.copy_expression(&value)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let negative = context.binary(copy, Operator::Lt, zero)?;
    let copy = context.copy_expression(&value)?;
    let absolute = context.scalar_udf(math::abs(), vec![copy])?;
    let one = context.literal(ScalarValue::Int32(Some(1)));
    let distance = context.binary(absolute, Operator::Minus, one)?;
    let maximum = context.literal(ScalarValue::UInt64(Some(max)));
    let wrapped = context.binary(maximum, Operator::Minus, distance)?;
    let value = context.case(vec![(negative, wrapped)], Some(value))?;
    let value = context.cast(value, unsigned)?;
    let value = context.binary(value, Operator::BitwiseShiftRight, shift)?;
    context.cast(value, target)
}

fn resolve_bitwise_not(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let negative = raw_negative(context, value);
    let one = context.literal(ScalarValue::Int32(Some(1)));
    context.binary(negative, Operator::Minus, one)
}

pub(super) fn list_built_in_bitwise_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("&", F::binary_op(Operator::BitwiseAnd)),
        ("^", F::binary_op(Operator::BitwiseXor)),
        (
            "bit_count",
            F::scalar_udf(datafusion_spark::function::bitwise::bit_count),
        ),
        (
            "bitwise_not",
            F::scalar_udf(datafusion_spark::function::bitwise::bitwise_not),
        ),
        (
            "bit_get",
            F::scalar_udf(datafusion_spark::function::bitwise::bit_get),
        ),
        (
            "getbit",
            F::scalar_udf(datafusion_spark::function::bitwise::bit_get),
        ),
        ("shiftleft", F::binary_op(Operator::BitwiseShiftLeft)),
        ("<<", F::binary_op(Operator::BitwiseShiftLeft)),
        ("shiftright", F::binary_op(Operator::BitwiseShiftRight)),
        (">>", F::binary_op(Operator::BitwiseShiftRight)),
        ("shiftrightunsigned", F::custom(shiftrightunsigned)),
        (">>>", F::custom(shiftrightunsigned)),
        ("|", F::binary_op(Operator::BitwiseOr)),
        ("~", F::custom(resolve_bitwise_not)),
    ]
}
