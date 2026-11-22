// SPDX-License-Identifier: Apache-2.0

use uuid::Uuid;

/// The components of a data file name produced by a listing table write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartFileName {
    /// The index of the writer partition that produced the file.
    pub partition: usize,
    /// The identifier shared by all files of one write.
    pub write_id: Uuid,
    /// The index of the file among the files that the writer partition
    /// produced in the same directory.
    pub counter: u32,
    /// The attempt number of the writer task that produced the file.
    pub attempt: usize,
}

/// Returns the file name without the suffix.
fn part_file_stem(partition: usize, write_id: &Uuid, counter: u32, attempt: usize) -> String {
    let write_id = write_id.as_hyphenated();
    if attempt == 0 {
        format!("part-{partition:05}-{write_id}-c{counter:03}")
    } else {
        format!("part-{partition:05}-{write_id}-c{counter:03}-a{attempt}")
    }
}

/// Builds the name of a data file.
/// The name is `part-<partition>-<write_id>-c<counter>[-a<attempt>]<suffix>`
/// where the attempt is present only for retried attempts.
/// The suffix is either empty or starts with a dot (e.g. `.zstd.parquet`).
pub fn part_file_name(
    partition: usize,
    write_id: &Uuid,
    counter: u32,
    attempt: usize,
    suffix: &str,
) -> String {
    let mut name = part_file_stem(partition, write_id, counter, attempt);
    name.push_str(suffix);
    name
}

/// Parses a file name built by [`part_file_name`].
/// Returns [`None`] for any name that [`part_file_name`] cannot produce.
pub fn parse_part_file_name(name: &str) -> Option<PartFileName> {
    fn split_digits(s: &str) -> Option<(&str, &str)> {
        let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        (end > 0).then(|| s.split_at(end))
    }

    let rest = name.strip_prefix("part-")?;
    let (partition, rest) = split_digits(rest)?;
    let rest = rest.strip_prefix('-')?;
    let (write_id, rest) = rest.split_at_checked(uuid::fmt::Hyphenated::LENGTH)?;
    let rest = rest.strip_prefix("-c")?;
    let (counter, rest) = split_digits(rest)?;
    let attempt = match rest.strip_prefix("-a").and_then(split_digits) {
        Some((attempt, _)) => attempt.parse().ok()?,
        None => 0,
    };
    let parsed = PartFileName {
        partition: partition.parse().ok()?,
        write_id: Uuid::try_parse(write_id).ok()?,
        counter: counter.parse().ok()?,
        attempt,
    };
    // Building the name again rejects all non-canonical spellings
    // such as missing zero padding or an upper-case identifier.
    let stem = part_file_stem(
        parsed.partition,
        &parsed.write_id,
        parsed.counter,
        parsed.attempt,
    );
    let suffix = name.strip_prefix(stem.as_str())?;
    (suffix.is_empty() || suffix.starts_with('.')).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WRITE_ID: Uuid = Uuid::from_u128(0x0f1e2d3c_4b5a_4978_8a9b_acbdcedfe0f1);

    #[test]
    fn test_part_file_name() {
        assert_eq!(
            part_file_name(0, &WRITE_ID, 0, 0, ".zstd.parquet"),
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.zstd.parquet"
        );
        assert_eq!(
            part_file_name(12, &WRITE_ID, 3, 0, ".parquet"),
            "part-00012-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c003.parquet"
        );
        assert_eq!(
            part_file_name(7, &WRITE_ID, 1, 2, ".snappy.parquet"),
            "part-00007-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c001-a2.snappy.parquet"
        );
        assert_eq!(
            part_file_name(123456, &WRITE_ID, 1000, 0, ""),
            "part-123456-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c1000"
        );
    }

    #[test]
    fn test_part_file_name_round_trip() {
        for partition in [0, 1, 99999, 100000, 1234567] {
            for counter in [0, 1, 999, 1000] {
                for attempt in [0, 1, 2, 10] {
                    for suffix in ["", ".parquet", ".zstd.parquet", ".a1.parquet"] {
                        let name = part_file_name(partition, &WRITE_ID, counter, attempt, suffix);
                        assert_eq!(
                            parse_part_file_name(&name),
                            Some(PartFileName {
                                partition,
                                write_id: WRITE_ID,
                                counter,
                                attempt,
                            }),
                            "{name}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_parse_part_file_name_other_write() {
        let other = Uuid::from_u128(1);
        let name = part_file_name(0, &other, 0, 0, ".parquet");
        let parsed = parse_part_file_name(&name);
        assert_eq!(parsed.map(|x| x.write_id), Some(other));
        assert_ne!(parsed.map(|x| x.write_id), Some(WRITE_ID));
    }

    #[test]
    fn test_parse_part_file_name_no_match() {
        for name in [
            "",
            "_SUCCESS",
            "part-00000",
            // the file names of `DataSinkExec`
            "a1B2c3D4e5F6g7H8_0.zst.parquet",
            "a1B2c3D4e5F6g7H8_0.parquet",
            // missing or malformed components
            "part-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-000.parquet",
            "part-00000-0f1e2d3c4b5a49788a9bacbdcedfe0f1-c000.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0fg-c000.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000-a.parquet",
            "part--0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.parquet",
            // non-canonical spellings
            "part-0-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.parquet",
            "part-000000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.parquet",
            "part-00000-0F1E2D3C-4B5A-4978-8A9B-ACBDCEDFE0F1-c000.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c0.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c0000.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000-a0.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000-a02.parquet",
            "Part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.parquet",
            // unexpected text around the name
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000-b1.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000_1.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000parquet",
            ".part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000-a1-a2.parquet",
            "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfé0f1-c000.parquet",
        ] {
            assert_eq!(parse_part_file_name(name), None, "{name}");
        }
    }

    #[test]
    fn test_parse_part_file_name_attempt() {
        let name = "part-00000-0f1e2d3c-4b5a-4978-8a9b-acbdcedfe0f1-c000-a2.parquet";
        assert_eq!(parse_part_file_name(name).map(|x| x.attempt), Some(2));
    }
}
