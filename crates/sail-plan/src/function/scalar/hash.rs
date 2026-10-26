use datafusion::functions::{crypto, string};
use datafusion_common::ScalarValue;
use datafusion_spark::function::hash::xxhash64::SparkXxhash64;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::hash::spark_murmur3_hash::SparkMurmur3Hash;

use crate::analyzer::model::{Expr, ExprKind};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn sha2(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, bits) = values.two()?;
    let function = match bits.kind.resolved("sha2 bit length")? {
        ExprKind::Literal(ScalarValue::Int32(Some(0 | 256))) => crypto::sha256(),
        ExprKind::Literal(ScalarValue::Int32(Some(224))) => crypto::sha224(),
        ExprKind::Literal(ScalarValue::Int32(Some(384))) => crypto::sha384(),
        ExprKind::Literal(ScalarValue::Int32(Some(512))) => crypto::sha512(),
        _ => {
            return Err(PlanError::invalid(
                "The second argument of sha2 must be a literal integer.",
            ));
        }
    };
    let value = context.scalar_udf(function, vec![value])?;
    let value = context.scalar_udf(datafusion_spark::function::math::hex(), vec![value])?;
    context.scalar_udf(string::lower(), vec![value])
}

pub(super) fn list_built_in_hash_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        (
            "crc32",
            F::scalar_udf(datafusion_spark::function::hash::crc32),
        ),
        ("hash", F::udf(SparkMurmur3Hash::new())),
        ("md5", F::scalar_udf(crypto::md5)),
        ("sha", F::scalar_udf(datafusion_spark::function::hash::sha1)),
        (
            "sha1",
            F::scalar_udf(datafusion_spark::function::hash::sha1),
        ),
        ("sha2", F::custom(sha2)),
        ("xxhash64", F::udf(SparkXxhash64::new())),
    ]
}
