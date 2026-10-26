use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_bitwise_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "bit_count",
            F::scalar_udf(datafusion_spark::function::bitwise::bit_count),
        ),
        (
            "bit_get",
            F::scalar_udf(datafusion_spark::function::bitwise::bit_get),
        ),
        (
            "bitwise_not",
            F::scalar_udf(datafusion_spark::function::bitwise::bitwise_not),
        ),
        (
            "getbit",
            F::scalar_udf(datafusion_spark::function::bitwise::bit_get),
        ),
    ]
}
