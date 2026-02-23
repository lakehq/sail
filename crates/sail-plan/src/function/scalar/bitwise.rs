use datafusion_expr::Operator;
use datafusion_spark::function::bitwise::expr_fn as bitwise_fn;

use crate::function::common::ScalarFunction;

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
        (
            "shiftrightunsigned",
            F::binary(bitwise_fn::shiftrightunsigned),
        ),
        (">>>", F::binary(bitwise_fn::shiftrightunsigned)),
        ("|", F::binary_op(Operator::BitwiseOr)),
        ("~", F::unary(bitwise_fn::bitwise_not)),
    ]
}
