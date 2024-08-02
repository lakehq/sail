use sail_common::spec;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::StorageLevel;

impl TryFrom<StorageLevel> for spec::StorageLevel {
    type Error = SparkError;

    fn try_from(level: StorageLevel) -> SparkResult<spec::StorageLevel> {
        let StorageLevel {
            use_disk,
            use_memory,
            use_off_heap,
            deserialized,
            replication,
        } = level;
        let replication = usize::try_from(replication).required("replication")?;
        Ok(spec::StorageLevel {
            use_disk,
            use_memory,
            use_off_heap,
            deserialized,
            replication,
        })
    }
}
