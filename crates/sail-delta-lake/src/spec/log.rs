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

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/path.rs#L23-L25>
pub const DELTA_LOG_DIR: &str = "_delta_log";

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
    // TODO(v2-checkpoints): Filename parsing only extracts the checkpoint version. It currently
    // does not distinguish classic single-file checkpoints, multipart checkpoints, or UUID-named
    // V2 checkpoints; format-specific validation has to happen elsewhere.
    if !filename.contains(".checkpoint") || !filename.ends_with(".parquet") {
        return None;
    }
    parse_version_prefix(filename)
}
