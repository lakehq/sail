use datafusion::functions::expr_fn;

use crate::function::common::Function;

pub(super) fn list_built_in_hash_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("crc32", F::unknown("crc32")),
        ("hash", F::unknown("hash")),
        ("md5", F::unary(expr_fn::md5)),
        ("sha", F::unknown("sha")),
        ("sha1", F::unknown("sha1")),
        ("sha2", F::unknown("sha2")),
        ("xxhash64", F::unknown("xxhash64")),
    ]
}
