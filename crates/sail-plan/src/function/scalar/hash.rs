use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, ScalarUDF};

use crate::error::PlanResult;
use crate::extension::function::math::spark_hex_unhex::SparkHex;
use crate::extension::function::spark_murmur3_hash::SparkMurmur3Hash;
use crate::extension::function::spark_xxhash64::SparkXxhash64;
use crate::function::common::{Function, FunctionInput};
use crate::utils::ItemTaker;

fn sha2(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let (expr, bit_length) = arguments.two()?;
    let result = match bit_length {
        expr::Expr::Literal(ScalarValue::Int32(Some(0)))
        | expr::Expr::Literal(ScalarValue::Int32(Some(256))) => expr_fn::sha256(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(224))) => expr_fn::sha224(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(384))) => expr_fn::sha384(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(512))) => expr_fn::sha512(expr),
        _ => {
            return Err(crate::error::PlanError::invalid(
                "The second argument of sha2 must be a literal integer.",
            ))
        }
    };
    let hex = expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkHex::new())),
        args: vec![result],
    });
    Ok(expr_fn::lower(hex))
}

pub(super) fn list_built_in_hash_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("crc32", F::unknown("crc32")),
        ("hash", F::udf(SparkMurmur3Hash::new())),
        ("md5", F::unary(expr_fn::md5)),
        ("sha", F::unknown("sha")),
        ("sha1", F::unknown("sha1")),
        ("sha2", F::custom(sha2)),
        ("xxhash64", F::udf(SparkXxhash64::new())),
    ]
}
