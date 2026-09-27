use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, ExprSchemable, Operator, ScalarUDF, expr, lit, when};
use datafusion_spark::function::bitwise::expr_fn as bitwise_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::conditional::SparkConditionalCast;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionBuilder, ScalarFunctionInput};

fn shiftrightunsigned(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (value, shift) = arguments.two()?;
    let shift_type = shift.get_type(function_context.schema)?;
    // Spark evaluates the left operand first and skips the count cast for NULL.
    // A scalar UDF evaluates both arguments eagerly, so retain that boundary for
    // nullable inputs before introducing a fallible INT cast of the count.
    // An INT (or smaller) literal cannot fail. Keep the boundary for columns:
    // projection pushdown can replace them with fallible expressions later.
    let infallible_count = matches!(shift, expr::Expr::Literal(..))
        && matches!(
            shift_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32
        );
    let null_value = (!infallible_count && value.nullable(function_context.schema)?)
        .then(|| value.clone().is_null());

    let ansi_mode = function_context
        .plan_config
        .view_conditional_ansi_mode
        .unwrap_or(function_context.plan_config.ansi_mode);
    let int_cast = ScalarUDF::from(SparkConditionalCast::new(DataType::Int32));
    let value = match value.get_type(function_context.schema)? {
        DataType::Decimal128(_, _) if !ansi_mode => {
            // Spark's non-ANSI decimal conversion retains the low 32 bits.
            // TODO: Support DECIMAL values outside the shared BIGINT cast's range.
            let value =
                ScalarUDF::from(SparkConditionalCast::new(DataType::Int64)).call(vec![value]);
            ((value << lit(32_i64)) >> lit(32_i64))
                .cast_to(&DataType::Int32, function_context.schema)?
        }
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            // TODO: Match Spark's non-ANSI FLOAT/DOUBLE-to-INT saturation;
            // generic casts currently reject out-of-range values.
            int_cast.call(vec![value])
        }
        _ => value,
    };
    let shift = if shift_type == DataType::Int32 {
        shift
    } else if !ansi_mode && shift_type == DataType::Int64 {
        (shift & lit(63_i64)).cast_to(&DataType::Int32, function_context.schema)?
    } else {
        // TODO: Match non-ANSI fractional count saturation and DECIMAL wrapping;
        // shared checked casts reject those out-of-range counts.
        int_cast.call(vec![shift])
    };
    // The existing kernel reinterprets signed bits directly, masks the count to
    // the value's width, and evaluates each argument once.
    let result = bitwise_fn::shiftrightunsigned(value, shift);
    if let Some(null_value) = null_value {
        let null = if result.get_type(function_context.schema)? == DataType::Int64 {
            ScalarValue::Int64(None)
        } else {
            ScalarValue::Int32(None)
        };
        Ok(when(null_value, lit(null)).otherwise(result)?)
    } else {
        Ok(result)
    }
}

/// Shifts like Spark, which casts the count to INT so that the result keeps the value's type.
fn signed_shift(op: Operator) -> ScalarFunction {
    ScalarFunctionBuilder::custom(move |input| {
        let ScalarFunctionInput {
            arguments,
            function_context,
        } = input;
        let (value, shift) = arguments.two()?;
        // A BIGINT value already shifts by the count's low six bits, so it keeps a BIGINT count.
        let shift = if value.get_type(function_context.schema)? != DataType::Int64
            && shift.get_type(function_context.schema)? == DataType::Int64
        {
            // Only the low six bits select the shift, so masking first keeps the result
            // and avoids casting a count outside the INT range.
            // TODO: Check BIGINT-to-INT overflow before masking in ANSI mode;
            //  overflowing counts are currently accepted instead of rejected.
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
