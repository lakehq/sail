pub mod pyspark_map_iter_udf;
pub mod pyspark_udaf;
pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod unresolved_pyspark_udf;

use sha2::{Digest, Sha256};

/// Generates a unique function name by combining the base name with a hash of the Python function payload.
/// Without this, lambda functions with the name `<lambda>` will be treated as the same function
/// by logical plan optimization rules (e.g. common sub-expression elimination), resulting in
/// incorrect logical plans.
pub fn get_udf_name(name: &str, payload: &[u8]) -> String {
    // Hash collision is possible in theory, but nearly impossible in practice
    // since we use a strong hash function here.
    let hash = hex::encode(Sha256::digest(payload));
    format!("{name}@{hash}")
}
