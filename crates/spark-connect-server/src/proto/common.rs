use crate::error::{SparkError, SparkResult};
use crate::spark::connect::StorageLevel;
use framework_common::spec;

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
        Ok(spec::StorageLevel {
            use_disk,
            use_memory,
            use_off_heap,
            deserialized,
            replication,
        })
    }
}
