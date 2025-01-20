use md5::{Digest, Md5};
use num_bigint::BigUint;

/// Generates a unique function name by combining the base name with a hash of the Python function payload.
/// Without this, lambda functions with the name `<lambda>` will be treated as the same function
/// by logical plan optimization rules (e.g. common sub-expression elimination), resulting in
/// incorrect logical plans.
pub fn get_udf_name(name: &str, payload: &[u8]) -> String {
    // Hash collision is possible in theory, but nearly impossible in practice
    // since we use a strong hash function here.
    let hash: Vec<u8> = Md5::digest(payload).to_vec();
    let hash = BigUint::from_bytes_be(&hash).to_str_radix(36);
    format!("{name}@{hash}")
}
