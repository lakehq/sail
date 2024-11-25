pub mod pyspark_map_iter_udf;
pub mod pyspark_udaf;
pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod unresolved_pyspark_udf;

use std::hash::{DefaultHasher, Hash, Hasher};

/// Generates a unique function name by combining the base name with a hash of the Python function's bytes.
/// Without this, lambda functions with the name `<lambda>` will be treated as the same function
/// by logical plan optimization rules (e.g. common sub-expression elimination), resulting in
/// incorrect logical plans.
pub fn get_udf_name(function_name: &str, function_bytes: &[u8]) -> String {
    // FIXME: Hash collision is possible
    let mut hasher = DefaultHasher::new();
    function_bytes.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{function_name}@0x{hash:x}")
}
