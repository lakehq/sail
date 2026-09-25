use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::abs;
use datafusion_expr::{BinaryExpr, ExprSchemable, Operator, cast, expr, lit, when};
use datafusion_spark::function::bitwise::expr_fn as bitwise_fn;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionBuilder, ScalarFunctionInput};

fn shiftrightunsigned(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (value, shift) = arguments.two()?;

    let input_type = value.clone().get_type(function_context.schema)?;
    let (value, input_type) = match input_type {
        DataType::Decimal128(_, _) if !function_context.plan_config.ansi_mode => (
            // Preserve decimal values supported by the existing unsigned conversion.
            // TODO: Support full DECIMAL-to-INT wrapping beyond that range.
            value.cast_to(&DataType::Int64, function_context.schema)?,
            DataType::Int32,
        ),
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => (
            // TODO: Match Spark's non-ANSI FLOAT/DOUBLE-to-INT saturation;
            //  generic casts currently reject out-of-range values.
            value.cast_to(&DataType::Int32, function_context.schema)?,
            DataType::Int32,
        ),
        input_type => (value, input_type),
    };

    // TODO: Implicitly cast TINYINT and SMALLINT inputs to INT like Spark.
    // TODO: Support the BIGINT minimum, whose absolute value overflows below.
    let (unsigned_type, max_const) = match input_type.clone() {
        DataType::Int32 => Ok((DataType::UInt32, u32::MAX as u64)),
        DataType::Int64 => Ok((DataType::UInt64, u64::MAX)),
        wrong_type => Err(PlanError::InvalidArgument(format!(
            "`shiftrightunsigned`: unsupported input type {wrong_type:?}"
        ))),
    }?;

    // Keep the integer sign test correct if DataFusion unwraps a decimal-to-integer cast.
    let unsigned = when(
        value.clone().lt_eq(lit(-1)),
        lit(max_const) - (abs(cast(value.clone(), DataType::Int64)) - lit(1)),
    )
    .otherwise(value.clone())?;

    // TODO: Match Spark's 5-bit INT and 6-bit BIGINT shift-count masking;
    //  DataFusion's coercion can reject negative counts or use the wrong width.
    // TODO: Reinterpret unsigned results with the sign bit set as signed INT/BIGINT;
    //  the checked cast currently rejects negative inputs shifted by zero.
    Ok(cast(cast(unsigned, unsigned_type) >> shift, input_type))
}

/// Shifts like Spark, which casts the count to INT so that the result keeps the value's type.
fn signed_shift(op: Operator) -> ScalarFunction {
    ScalarFunctionBuilder::custom(move |input| {
        let ScalarFunctionInput {
            arguments,
            function_context,
        } = input;
        let (value, shift) = arguments.two()?;
        let shift = if shift.get_type(function_context.schema)? == DataType::Int64 {
            // Only the low six bits select the shift, so masking first keeps the result
            // and avoids casting a count outside the INT range.
            // TODO: Reject BIGINT shift counts outside the INT range in ANSI mode.
            (shift & lit(63_i64)).cast_to(&DataType::Int32, function_context.schema)?
        } else {
            shift
        };
        Ok(expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(value),
            op,
            right: Box::new(shift),
        }))
    })
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
        ("shiftleft", signed_shift(Operator::BitwiseShiftLeft)),
        ("<<", signed_shift(Operator::BitwiseShiftLeft)),
        ("shiftright", signed_shift(Operator::BitwiseShiftRight)),
        (">>", signed_shift(Operator::BitwiseShiftRight)),
        ("shiftrightunsigned", F::custom(shiftrightunsigned)),
        (">>>", F::custom(shiftrightunsigned)),
        ("|", F::binary_op(Operator::BitwiseOr)),
        ("~", F::unary(|arg| (-arg) - lit(1))),
    ]
}
