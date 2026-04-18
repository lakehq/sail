// https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
//
// Copyright 2023-2024 The Delta Kernel Rust Authors
// Portions Copyright 2025-2026 LakeSail, Inc.
// Ported and modified in 2026 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use object_store::path::{Path, DELIMITER};
use uuid::Uuid;

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/path.rs#L23-L25>
pub const DELTA_LOG_DIR: &str = "_delta_log";
pub const SIDECARS_DIR: &str = "_sidecars";

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/last_checkpoint_hint.rs#L14-L17>
pub const LAST_CHECKPOINT_FILE: &str = "_last_checkpoint";

pub fn delta_log_root_path() -> Path {
    Path::from(DELTA_LOG_DIR)
}

pub fn last_checkpoint_path() -> Path {
    Path::from(format!("{DELTA_LOG_DIR}/{LAST_CHECKPOINT_FILE}"))
}

pub fn delta_log_prefix_path(version: i64) -> Path {
    Path::from(format!("{DELTA_LOG_DIR}/{version:020}"))
}

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/path.rs#L328-L339>
pub fn checkpoint_path(version: i64) -> Path {
    Path::from(format!("{DELTA_LOG_DIR}/{version:020}.checkpoint.parquet"))
}

pub fn commit_path(version: i64) -> Path {
    Path::from_iter([DELTA_LOG_DIR, &format!("{version:020}.json")])
}

pub fn checksum_path(version: i64) -> Path {
    Path::from_iter([DELTA_LOG_DIR, &format!("{version:020}.crc")])
}

pub fn temp_commit_path(token: &str) -> Path {
    Path::from_iter([DELTA_LOG_DIR, &format!("_commit_{token}.json.tmp")])
}

pub fn sidecars_dir_path() -> Path {
    Path::from(format!("{DELTA_LOG_DIR}/{SIDECARS_DIR}"))
}

pub fn sidecar_file_path(sidecar_filename: &str) -> Path {
    Path::from(format!("{DELTA_LOG_DIR}/{SIDECARS_DIR}/{sidecar_filename}"))
}

pub fn uuid_checkpoint_path(version: i64, uuid: &Uuid) -> Path {
    Path::from(format!(
        "{DELTA_LOG_DIR}/{version:020}.checkpoint.{uuid}.parquet"
    ))
}

pub fn delta_log_file_path(table_root_path: &str, filename: &str) -> Path {
    Path::from(format!(
        "{}{}{}{}{}",
        table_root_path, DELIMITER, DELTA_LOG_DIR, DELIMITER, filename
    ))
}

pub fn parse_version_prefix(filename: &str) -> Option<i64> {
    let prefix = filename.get(0..20)?;
    if !prefix.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    prefix.parse::<i64>().ok()
}

pub fn parse_commit_version(filename: &str) -> Option<i64> {
    if filename.len() != 25 || !filename.ends_with(".json") {
        return None;
    }
    parse_version_prefix(filename)
}

pub fn parse_checksum_version(filename: &str) -> Option<i64> {
    if filename.len() != 24 || !filename.ends_with(".crc") {
        return None;
    }
    parse_version_prefix(filename)
}

pub fn parse_checkpoint_version(filename: &str) -> Option<i64> {
    // Handles three checkpoint naming schemes:
    // 1. Classic: `{version:020}.checkpoint.parquet`
    // 2. Multi-part: `{version:020}.checkpoint.{part:010}.{total:010}.parquet`
    // 3. UUID-named V2: `{version:020}.checkpoint.{uuid}.parquet`
    if !filename.contains(".checkpoint") || !filename.ends_with(".parquet") {
        return None;
    }
    parse_version_prefix(filename)
}

/// Returns `true` if the checkpoint filename uses the UUID-named V2 naming scheme.
pub fn is_uuid_checkpoint_filename(filename: &str) -> bool {
    // UUID-named: `{version:020}.checkpoint.{uuid}.parquet`
    // The UUID part is 36 chars (8-4-4-4-12), total = 20 + 12 + 36 + 8 = 76
    // Pattern: 20 digits + ".checkpoint." + 36 UUID + ".parquet"
    if filename.len() != 76 || !filename.ends_with(".parquet") {
        return false;
    }
    if parse_version_prefix(filename).is_none() {
        return false;
    }
    let Some(rest) = filename.get(20..) else {
        return false;
    };
    if !rest.starts_with(".checkpoint.") {
        return false;
    }
    let uuid_part = &rest[12..48]; // 36-char UUID
    Uuid::parse_str(uuid_part).is_ok()
}

/// Parses a compacted JSON filename and returns the (start_version, end_version) pair.
///
/// Expected pattern: `{start:020}.{end:020}.compacted.json`
/// where `end > start`.
pub fn parse_compacted_json_versions(filename: &str) -> Option<(i64, i64)> {
    // Total length: 20 + 1 + 20 + ".compacted.json" (15) = 56
    if filename.len() != 56 || !filename.ends_with(".compacted.json") {
        return None;
    }
    let start = parse_version_prefix(filename)?;
    if filename.as_bytes().get(20).copied() != Some(b'.') {
        return None;
    }
    let end_str = filename.get(21..41)?;
    if !end_str.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let end = end_str.parse::<i64>().ok()?;
    if end <= start {
        return None;
    }
    Some((start, end))
}

pub fn compacted_json_path(start_version: i64, end_version: i64) -> Path {
    Path::from(format!(
        "{DELTA_LOG_DIR}/{start_version:020}.{end_version:020}.compacted.json"
    ))
}

/// Returns `true` if the filename matches the compacted JSON naming convention.
pub fn is_compacted_json_filename(filename: &str) -> bool {
    parse_compacted_json_versions(filename).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_compacted_json_valid() {
        let filename = "00000000000000000004.00000000000000000006.compacted.json";
        assert_eq!(parse_compacted_json_versions(filename), Some((4, 6)));
    }

    #[test]
    fn parse_compacted_json_large_versions() {
        let filename = "00000000000000000100.00000000000000000200.compacted.json";
        assert_eq!(parse_compacted_json_versions(filename), Some((100, 200)));
    }

    #[test]
    fn parse_compacted_json_rejects_equal_versions() {
        let filename = "00000000000000000005.00000000000000000005.compacted.json";
        assert_eq!(parse_compacted_json_versions(filename), None);
    }

    #[test]
    fn parse_compacted_json_rejects_reversed_versions() {
        let filename = "00000000000000000006.00000000000000000004.compacted.json";
        assert_eq!(parse_compacted_json_versions(filename), None);
    }

    #[test]
    fn parse_compacted_json_rejects_commit_file() {
        let filename = "00000000000000000004.json";
        assert_eq!(parse_compacted_json_versions(filename), None);
    }

    #[test]
    fn parse_compacted_json_rejects_checkpoint() {
        let filename = "00000000000000000004.checkpoint.parquet";
        assert_eq!(parse_compacted_json_versions(filename), None);
    }

    #[test]
    fn is_compacted_json_recognizes_valid() {
        assert!(is_compacted_json_filename(
            "00000000000000000001.00000000000000000009.compacted.json"
        ));
    }

    #[test]
    fn is_compacted_json_rejects_commit() {
        assert!(!is_compacted_json_filename("00000000000000000001.json"));
    }

    #[test]
    fn compacted_json_path_formats_correctly() {
        let path = compacted_json_path(4, 6);
        assert_eq!(
            path.as_ref(),
            "_delta_log/00000000000000000004.00000000000000000006.compacted.json"
        );
    }
}
