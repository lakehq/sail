use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr};
use datafusion_spark::function::hash::expr_fn as hash_fn;
use datafusion_spark::function::hash::xxhash64::SparkXxhash64;
use datafusion_spark::function::math::hex::SparkHex;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::hash::spark_murmur3_hash::SparkMurmur3Hash;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn sha2(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let (expr, bit_length) = arguments.two()?;
    let result = match bit_length {
        expr::Expr::Literal(ScalarValue::Int32(Some(0)), _metadata)
        | expr::Expr::Literal(ScalarValue::Int32(Some(256)), _metadata) => expr_fn::sha256(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(224)), _metadata) => expr_fn::sha224(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(384)), _metadata) => expr_fn::sha384(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(512)), _metadata) => expr_fn::sha512(expr),
        _ => {
            return Err(crate::error::PlanError::invalid(
                "The second argument of sha2 must be a literal integer.",
            ));
        }
    };
    let hex = expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkHex::new())),
        args: vec![result],
    });
    Ok(expr_fn::lower(hex))
}

/// `xxhash64` of a DAY-TIME interval hashes its microseconds as a long (`hash.scala:556-557`), which
/// is what casting the interval to a `BIGINT` hands the upstream kernel: the kernel itself has no
/// arm for `Duration`, and a `date - date` difference reaches it as one.
fn xxhash64(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = arguments
        .into_iter()
        .map(
            |argument| match argument.get_type(function_context.schema.as_ref())? {
                DataType::Duration(_) => Ok(cast(argument, DataType::Int64)),
                _ => Ok(argument),
            },
        )
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkXxhash64::new())),
        args: arguments,
    }))
}

pub(super) fn list_built_in_hash_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("crc32", F::unary(hash_fn::crc32)),
        ("hash", F::udf(SparkMurmur3Hash::new())),
        ("md5", F::unary(expr_fn::md5)),
        ("sha", F::unary(hash_fn::sha1)),
        ("sha1", F::unary(hash_fn::sha1)),
        ("sha2", F::custom(sha2)),
        ("xxhash64", F::custom(xxhash64)),
    ]
}
