// SPDX-License-Identifier: Apache-2.0

use uuid::Uuid;

/// Builds the name of a data file produced by a listing table write.
///
/// The name is `part-<partition>-<write_id>-c<counter><suffix>`, where
/// `partition` is the index of the writer partition that produced the file,
/// `write_id` is the identifier shared by all files of one write, and `counter`
/// is the index of the file among the files that the writer partition produced
/// in the same directory. The suffix is either empty or starts with a dot
/// (e.g. `.zstd.parquet`).
///
/// The name does not depend on the attempt of the writer task. Only the files
/// of one attempt of each writer partition are ever published, so a write
/// produces the same names whether or not its tasks were attempted again.
pub fn part_file_name(partition: usize, write_id: &Uuid, counter: u32, suffix: &str) -> String {
    let write_id = write_id.as_hyphenated();
    format!("part-{partition:05}-{write_id}-c{counter:03}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const WRITE_ID: Uuid = Uuid::from_u128(0x0f1e2d3c_4b5a_4978_8a9b_acbdcedfe0f1);

    #[test]
    fn test_part_file_name() {
        assert_eq!(
            part_file_name(0, &WRITE_ID, 0, ".zstd.parquet"),
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.zstd.parquet"
        );
        assert_eq!(
            part_file_name(12, &WRITE_ID, 3, ".parquet"),
            "part-00012-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c003.parquet"
        );
        assert_eq!(
            part_file_name(123456, &WRITE_ID, 1000, ""),
            "part-123456-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c1000"
        );
    }
}
