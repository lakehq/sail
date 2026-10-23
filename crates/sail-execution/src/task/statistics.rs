use sail_system_store::types::{TaskOutputPartitionStatistics, TaskOutputStatistics};

use crate::driver::r#gen;

impl From<TaskOutputStatistics> for r#gen::TaskOutputStatistics {
    fn from(value: TaskOutputStatistics) -> Self {
        Self {
            complete: value.complete,
            partitions: value
                .partitions
                .into_iter()
                .map(|p| r#gen::TaskOutputPartitionStatistics {
                    num_rows: p.num_rows,
                    batch_bytes: p.batch_bytes,
                    stored_bytes: p.stored_bytes,
                })
                .collect(),
        }
    }
}

impl From<r#gen::TaskOutputStatistics> for TaskOutputStatistics {
    fn from(value: r#gen::TaskOutputStatistics) -> Self {
        Self {
            complete: value.complete,
            partitions: value
                .partitions
                .into_iter()
                .map(|p| TaskOutputPartitionStatistics {
                    num_rows: p.num_rows,
                    batch_bytes: p.batch_bytes,
                    stored_bytes: p.stored_bytes,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn wire_roundtrip_preserves_unknown_zero_and_incomplete() -> Result<(), prost::DecodeError> {
        for complete in [false, true] {
            let stats = TaskOutputStatistics {
                complete,
                partitions: vec![
                    TaskOutputPartitionStatistics {
                        num_rows: 0,
                        batch_bytes: 0,
                        stored_bytes: None,
                    },
                    TaskOutputPartitionStatistics {
                        num_rows: 0,
                        batch_bytes: 0,
                        stored_bytes: Some(0),
                    },
                    TaskOutputPartitionStatistics {
                        num_rows: 100,
                        batch_bytes: 800,
                        stored_bytes: Some(200),
                    },
                ],
            };
            let bytes = r#gen::TaskOutputStatistics::from(stats.clone()).encode_to_vec();
            assert_eq!(
                TaskOutputStatistics::from(r#gen::TaskOutputStatistics::decode(bytes.as_slice())?),
                stats
            );
        }
        Ok(())
    }
}
