use datafusion_expr::Operator;

use crate::function::common::Function;

pub(super) fn list_built_in_bitwise_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("&", F::binary_op(Operator::BitwiseAnd)),
        ("^", F::binary_op(Operator::BitwiseXor)),
        ("bit_count", F::unknown("bit_count")),
        ("bit_get", F::unknown("bit_get")),
        ("getbit", F::unknown("getbit")),
        // "shiftleft" is defined in math functions
        ("shiftright", F::binary_op(Operator::BitwiseShiftRight)),
        ("shiftrightunsigned", F::unknown("shiftrightunsigned")),
        ("|", F::binary_op(Operator::BitwiseOr)),
        ("~", F::unknown("~")),
    ]
}
