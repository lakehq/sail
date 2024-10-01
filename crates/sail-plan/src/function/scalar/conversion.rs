use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;

use crate::function::common::Function;

pub(super) fn list_built_in_conversion_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("bigint", F::cast(DataType::Int64)),
        ("binary", F::cast(DataType::Binary)),
        ("boolean", F::cast(DataType::Boolean)),
        ("cast", F::unknown("cast")),
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
