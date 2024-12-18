use md5::{Digest, Md5};
use num_bigint::BigUint;

pub mod pyspark_batch_collector;
pub mod pyspark_cogroup_map_udf;
pub mod pyspark_group_map_udf;
pub mod pyspark_map_iter_udf;
pub mod pyspark_udaf;
pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod pyspark_unresolved_udf;

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

#[derive(Debug, Copy, Clone)]
pub enum ColumnMatch {
    ByName,
    ByPosition,
}

impl ColumnMatch {
    pub fn by_name(value: bool) -> Self {
        if value {
            ColumnMatch::ByName
        } else {
            ColumnMatch::ByPosition
        }
    }

    pub fn is_by_name(&self) -> bool {
        match self {
            ColumnMatch::ByName => true,
            ColumnMatch::ByPosition => false,
        }
    }
}
