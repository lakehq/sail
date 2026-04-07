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

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::spec::schema::{ColumnMappingMode, ColumnName};
use crate::spec::{DeltaError as DeltaTableError, DeltaResult};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties.rs#L183-L204>
pub enum DataSkippingNumIndexedCols {
    AllColumns,
    NumColumns(u64),
}

impl TryFrom<&str> for DataSkippingNumIndexedCols {
    type Error = DeltaTableError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let num: i64 = value.parse().map_err(|_| {
            DeltaTableError::generic("couldn't parse DataSkippingNumIndexedCols to an integer")
        })?;
        match num {
            -1 => Ok(Self::AllColumns),
            x if x >= 0 => Ok(Self::NumColumns(x as u64)),
            _ => Err(DeltaTableError::generic(
                "couldn't parse DataSkippingNumIndexedCols to positive integer",
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Default)]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties.rs#L206-L229>
pub enum IsolationLevel {
    #[default]
    Serializable,
    WriteSerializable,
    SnapshotIsolation,
}

impl AsRef<str> for IsolationLevel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Serializable => "Serializable",
            Self::WriteSerializable => "WriteSerializable",
            Self::SnapshotIsolation => "SnapshotIsolation",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => Err(DeltaTableError::generic(format!(
                "Invalid string for IsolationLevel: {s}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties.rs#L31-L180>
pub struct TableProperties {
    pub append_only: Option<bool>,
    pub checkpoint_interval: Option<NonZeroU64>,
    pub checkpoint_write_stats_as_json: Option<bool>,
    pub checkpoint_write_stats_as_struct: Option<bool>,
    pub write_checksum_file_enabled: Option<bool>,
    pub enable_in_commit_timestamps: Option<bool>,
    pub in_commit_timestamp_enablement_version: Option<i64>,
    pub in_commit_timestamp_enablement_timestamp: Option<i64>,
    pub column_mapping_mode: Option<ColumnMappingMode>,
    pub data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,
    pub data_skipping_stats_columns: Option<Vec<ColumnName>>,
    pub deleted_file_retention_duration: Option<Duration>,
    pub isolation_level: Option<IsolationLevel>,
    pub log_retention_duration: Option<Duration>,
    pub enable_expired_log_cleanup: Option<bool>,
    pub log_compaction_interval: Option<NonZeroU64>,
    pub unknown_properties: HashMap<String, String>,
}

impl<K, V, I> From<I> for TableProperties
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    // [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties/deserialize.rs#L20-L35>
    fn from(unparsed: I) -> Self {
        let mut props = TableProperties::default();
        let unparsed = unparsed.into_iter().filter(|(k, v)| {
            try_parse_table_property(&mut props, k.as_ref(), v.as_ref()).is_none()
        });
        props.unknown_properties = unparsed.map(|(k, v)| (k.into(), v.into())).collect();
        props
    }
}

const DEFAULT_LOG_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const DEFAULT_DELETED_FILE_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
// Sail aligns with Spark/Delta's default of checkpointing every 10 committed versions.
const DEFAULT_CHECKPOINT_INTERVAL: NonZeroU64 =
    NonZeroU64::new(10).expect("non-zero checkpoint interval");

impl TableProperties {
    pub fn append_only(&self) -> bool {
        self.append_only.unwrap_or(false)
    }

    pub fn log_retention_duration(&self) -> Duration {
        self.log_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_LOG_RETENTION_SECS))
    }

    pub fn enable_expired_log_cleanup(&self) -> bool {
        self.enable_expired_log_cleanup.unwrap_or(true)
    }

    pub fn checkpoint_interval(&self) -> NonZeroU64 {
        self.checkpoint_interval
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL)
    }

    pub fn write_checksum_file_enabled(&self) -> bool {
        self.write_checksum_file_enabled.unwrap_or(true)
    }

    pub fn enable_in_commit_timestamps(&self) -> bool {
        self.enable_in_commit_timestamps.unwrap_or(false)
    }

    pub fn in_commit_timestamp_enablement_version(&self) -> Option<i64> {
        self.in_commit_timestamp_enablement_version
    }

    pub fn in_commit_timestamp_enablement_timestamp(&self) -> Option<i64> {
        self.in_commit_timestamp_enablement_timestamp
    }

    pub fn in_commit_timestamp_enablement(&self) -> Option<(i64, i64)> {
        self.in_commit_timestamp_enablement_version
            .zip(self.in_commit_timestamp_enablement_timestamp)
    }

    pub fn deleted_file_retention_duration(&self) -> Duration {
        self.deleted_file_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_DELETED_FILE_RETENTION_SECS))
    }

    pub fn log_compaction_interval(&self) -> Option<u64> {
        self.log_compaction_interval.map(|v| v.get())
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap_or_default()
    }
}

pub fn canonicalize_and_validate_table_properties<K, V, I>(
    properties: I,
) -> DeltaResult<HashMap<String, String>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut canonicalized = HashMap::new();
    for (key, value) in properties {
        let key = canonicalize_table_property_key(key.as_ref()).unwrap_or_else(|| key.as_ref());
        validate_table_property(key, value.as_ref())?;
        canonicalized.insert(key.to_string(), value.as_ref().to_string());
    }
    Ok(canonicalized)
}

/// Map supported property aliases to their canonical Delta table property key.
///
/// Returns `Some(canonical_key)` for recognized modeled properties and aliases, and `None`
/// for unrecognized keys that should be preserved as-is.
fn canonicalize_table_property_key(key: &str) -> Option<&'static str> {
    match key.to_ascii_lowercase().as_str() {
        "delta.appendonly" | "append_only" | "appendonly" => Some("delta.appendOnly"),
        "delta.checkpointinterval" | "checkpoint_interval" | "checkpointinterval" => {
            Some("delta.checkpointInterval")
        }
        "delta.checkpoint.writestatsasjson"
        | "checkpoint_write_stats_as_json"
        | "checkpointwritestatsasjson" => Some("delta.checkpoint.writeStatsAsJson"),
        "delta.checkpoint.writestatsasstruct"
        | "checkpoint_write_stats_as_struct"
        | "checkpointwritestatsasstruct" => Some("delta.checkpoint.writeStatsAsStruct"),
        "delta.writechecksumfile.enabled"
        | "write_checksum_file_enabled"
        | "writechecksumfileenabled" => Some("delta.writeChecksumFile.enabled"),
        "delta.enableincommittimestamps"
        | "enable_in_commit_timestamps"
        | "enableincommittimestamps" => Some("delta.enableInCommitTimestamps"),
        "delta.incommittimestampenablementversion"
        | "in_commit_timestamp_enablement_version"
        | "incommittimestampenablementversion" => Some("delta.inCommitTimestampEnablementVersion"),
        "delta.incommittimestampenablementtimestamp"
        | "in_commit_timestamp_enablement_timestamp"
        | "incommittimestampenablementtimestamp" => {
            Some("delta.inCommitTimestampEnablementTimestamp")
        }
        "delta.columnmapping.mode"
        | "column_mapping_mode"
        | "columnmappingmode"
        | "column_mapping" => Some("delta.columnMapping.mode"),
        "delta.dataskippingnumindexedcols"
        | "data_skipping_num_indexed_cols"
        | "dataskippingnumindexedcols" => Some("delta.dataSkippingNumIndexedCols"),
        "delta.dataskippingstatscolumns"
        | "data_skipping_stats_columns"
        | "dataskippingstatscolumns" => Some("delta.dataSkippingStatsColumns"),
        "delta.deletedfileretentionduration"
        | "deleted_file_retention_duration"
        | "deletedfileretentionduration" => Some("delta.deletedFileRetentionDuration"),
        "delta.isolationlevel" | "isolation_level" | "isolationlevel" => {
            Some("delta.isolationLevel")
        }
        "delta.logretentionduration" | "log_retention_duration" | "logretentionduration" => {
            Some("delta.logRetentionDuration")
        }
        "delta.enableexpiredlogcleanup"
        | "enable_expired_log_cleanup"
        | "enableexpiredlogcleanup" => Some("delta.enableExpiredLogCleanup"),
        "delta.logcompactioninterval" | "log_compaction_interval" | "logcompactioninterval" => {
            Some("delta.logCompactionInterval")
        }
        _ => None,
    }
}

/// Resolve whether an external option key should be routed into Delta table properties.
///
/// Known modeled aliases are canonicalized to the exact Delta table property name. Any other key
/// with a `delta.` prefix is treated as a pass-through table property so newer protocol features
/// can still be persisted without first teaching Sail about them.
pub fn route_table_property_key(key: &str) -> Option<String> {
    if let Some(canonical) = canonicalize_table_property_key(key) {
        return Some(canonical.to_string());
    }

    if key.len() >= 6 && key[..6].eq_ignore_ascii_case("delta.") {
        if key.starts_with("delta.") {
            return Some(key.to_string());
        }
        return Some(format!("delta.{}", &key[6..]));
    }

    None
}

/// Validate modeled Delta table property values while allowing unknown properties through.
///
/// Known properties are parsed using the same type-specific rules Delta snapshots rely on
/// (boolean, positive integer, interval, column mapping mode, etc.). Unknown properties are
/// accepted so they can still be persisted in `metaData.configuration`.
fn validate_table_property(key: &str, value: &str) -> DeltaResult<()> {
    match key {
        "delta.appendOnly"
        | "delta.checkpoint.writeStatsAsJson"
        | "delta.checkpoint.writeStatsAsStruct"
        | "delta.writeChecksumFile.enabled"
        | "delta.enableInCommitTimestamps"
        | "delta.enableExpiredLogCleanup" => parse_bool(value).map(|_| ()).ok_or_else(|| {
            DeltaTableError::generic(format!("invalid boolean value for {key}: {value}"))
        }),
        "delta.checkpointInterval" | "delta.logCompactionInterval" => {
            parse_positive_int(value).map(|_| ()).ok_or_else(|| {
                DeltaTableError::generic(format!(
                    "invalid value for {key}: expected positive integer"
                ))
            })
        }
        "delta.inCommitTimestampEnablementVersion" => {
            parse_non_negative_i64(value).map(|_| ()).ok_or_else(|| {
                DeltaTableError::generic(format!(
                    "invalid value for {key}: expected non-negative integer"
                ))
            })
        }
        "delta.inCommitTimestampEnablementTimestamp" => {
            parse_i64(value).map(|_| ()).ok_or_else(|| {
                DeltaTableError::generic(format!("invalid value for {key}: expected integer"))
            })
        }
        "delta.columnMapping.mode" => ColumnMappingMode::try_from(value).map(|_| ()),
        "delta.dataSkippingNumIndexedCols" => {
            DataSkippingNumIndexedCols::try_from(value).map(|_| ())
        }
        "delta.dataSkippingStatsColumns" => ColumnName::parse_column_name_list(value).map(|_| ()),
        "delta.deletedFileRetentionDuration" | "delta.logRetentionDuration" => {
            parse_interval(value).map(|_| ()).ok_or_else(|| {
                DeltaTableError::generic(format!(
                    "invalid value for {key}: expected Delta interval literal"
                ))
            })
        }
        "delta.isolationLevel" => IsolationLevel::from_str(value).map(|_| ()),
        _ => Ok(()),
    }
}

fn try_parse_table_property(props: &mut TableProperties, key: &str, value: &str) -> Option<()> {
    // [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties/deserialize.rs#L37-L104>
    match key {
        "delta.appendOnly" => props.append_only = Some(parse_bool(value)?),
        "delta.checkpointInterval" => props.checkpoint_interval = Some(parse_positive_int(value)?),
        "delta.checkpoint.writeStatsAsJson" => {
            props.checkpoint_write_stats_as_json = Some(parse_bool(value)?)
        }
        "delta.checkpoint.writeStatsAsStruct" => {
            props.checkpoint_write_stats_as_struct = Some(parse_bool(value)?)
        }
        "delta.writeChecksumFile.enabled" => {
            props.write_checksum_file_enabled = Some(parse_bool(value)?)
        }
        "delta.enableInCommitTimestamps" => {
            props.enable_in_commit_timestamps = Some(parse_bool(value)?)
        }
        "delta.inCommitTimestampEnablementVersion" => {
            props.in_commit_timestamp_enablement_version = Some(parse_non_negative_i64(value)?)
        }
        "delta.inCommitTimestampEnablementTimestamp" => {
            props.in_commit_timestamp_enablement_timestamp = Some(parse_i64(value)?)
        }
        "delta.columnMapping.mode" => {
            props.column_mapping_mode = ColumnMappingMode::try_from(value).ok()
        }
        "delta.dataSkippingNumIndexedCols" => {
            props.data_skipping_num_indexed_cols = DataSkippingNumIndexedCols::try_from(value).ok()
        }
        "delta.dataSkippingStatsColumns" => {
            props.data_skipping_stats_columns = ColumnName::parse_column_name_list(value).ok()
        }
        "delta.deletedFileRetentionDuration" => {
            props.deleted_file_retention_duration = parse_interval(value)
        }
        "delta.isolationLevel" => {
            props.isolation_level = IsolationLevel::from_str(value).ok();
        }
        "delta.logRetentionDuration" => props.log_retention_duration = parse_interval(value),
        "delta.enableExpiredLogCleanup" => {
            props.enable_expired_log_cleanup = Some(parse_bool(value)?)
        }
        "delta.logCompactionInterval" => {
            props.log_compaction_interval = Some(parse_positive_int(value)?)
        }
        _ => return None,
    }
    Some(())
}

fn parse_positive_int(s: &str) -> Option<NonZeroU64> {
    // [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties/deserialize.rs#L106-L122>
    let n: i64 = s.parse().ok()?;
    if n <= 0 {
        return None;
    }
    NonZeroU64::new(n as u64)
}

fn parse_bool(s: &str) -> Option<bool> {
    // [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties/deserialize.rs#L124-L132>
    match s {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_i64(s: &str) -> Option<i64> {
    s.parse().ok()
}

fn parse_non_negative_i64(s: &str) -> Option<i64> {
    let value = parse_i64(s)?;
    (value >= 0).then_some(value)
}

fn parse_interval(s: &str) -> Option<Duration> {
    // [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_properties/deserialize.rs#L142-L146>
    const SECONDS_PER_MINUTE: u64 = 60;
    const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
    const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
    const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

    let mut it = s.split_whitespace();
    if it.next() != Some("interval") {
        return None;
    }
    let number = it.next()?.parse::<i64>().ok()?;
    if number < 0 {
        return None;
    }
    let number = number as u64;
    match it.next()? {
        "nanosecond" | "nanoseconds" => Some(Duration::from_nanos(number)),
        "microsecond" | "microseconds" => Some(Duration::from_micros(number)),
        "millisecond" | "milliseconds" => Some(Duration::from_millis(number)),
        "second" | "seconds" => Some(Duration::from_secs(number)),
        "minute" | "minutes" => Some(Duration::from_secs(number * SECONDS_PER_MINUTE)),
        "hour" | "hours" => Some(Duration::from_secs(number * SECONDS_PER_HOUR)),
        "day" | "days" => Some(Duration::from_secs(number * SECONDS_PER_DAY)),
        "week" | "weeks" => Some(Duration::from_secs(number * SECONDS_PER_WEEK)),
        _ => None,
    }
}

/// Resolve the effective `DataSkippingNumIndexedCols` for a given table configuration.
pub fn resolve_data_skipping_num_indexed_cols(
    props: &TableProperties,
) -> DeltaResult<DataSkippingNumIndexedCols> {
    Ok(props
        .data_skipping_num_indexed_cols
        .unwrap_or(DataSkippingNumIndexedCols::AllColumns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_interval_default_is_ten() {
        assert_eq!(TableProperties::default().checkpoint_interval().get(), 10);
    }

    #[test]
    fn test_write_checksum_file_enabled_default_is_true() {
        assert!(TableProperties::default().write_checksum_file_enabled());
    }

    #[test]
    fn test_canonicalize_table_property_aliases() -> DeltaResult<()> {
        let props = canonicalize_and_validate_table_properties([
            ("column_mapping_mode", "name"),
            ("checkpoint_interval", "7"),
            ("write_checksum_file_enabled", "false"),
            ("enable_in_commit_timestamps", "true"),
            ("custom.key", "value"),
        ])?;

        assert_eq!(
            props.get("delta.columnMapping.mode"),
            Some(&"name".to_string())
        );
        assert_eq!(
            props.get("delta.checkpointInterval"),
            Some(&"7".to_string())
        );
        assert_eq!(
            props.get("delta.writeChecksumFile.enabled"),
            Some(&"false".to_string())
        );
        assert_eq!(
            props.get("delta.enableInCommitTimestamps"),
            Some(&"true".to_string())
        );
        assert_eq!(props.get("custom.key"), Some(&"value".to_string()));
        Ok(())
    }

    #[test]
    fn test_invalid_modeled_property_is_rejected() {
        let result =
            canonicalize_and_validate_table_properties([("delta.checkpointInterval", "0")]);
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err
                .to_string()
                .contains("invalid value for delta.checkpointInterval"));
        }
    }

    #[test]
    fn test_route_table_property_key() {
        assert_eq!(
            route_table_property_key("column_mapping_mode"),
            Some("delta.columnMapping.mode".to_string())
        );
        assert_eq!(
            route_table_property_key("enable_in_commit_timestamps"),
            Some("delta.enableInCommitTimestamps".to_string())
        );
        assert_eq!(
            route_table_property_key("write_checksum_file_enabled"),
            Some("delta.writeChecksumFile.enabled".to_string())
        );
        assert_eq!(
            route_table_property_key("append_only"),
            Some("delta.appendOnly".to_string())
        );
        assert_eq!(
            route_table_property_key("Delta.featureFlag"),
            Some("delta.featureFlag".to_string())
        );
        assert_eq!(route_table_property_key("mergeSchema"), None);
    }

    #[test]
    fn test_in_commit_timestamp_properties_are_typed() {
        let props = TableProperties::from([
            ("delta.enableInCommitTimestamps", "true"),
            ("delta.inCommitTimestampEnablementVersion", "3"),
            ("delta.inCommitTimestampEnablementTimestamp", "123"),
        ]);

        assert!(props.enable_in_commit_timestamps());
        assert_eq!(props.in_commit_timestamp_enablement_version(), Some(3));
        assert_eq!(props.in_commit_timestamp_enablement_timestamp(), Some(123));
        assert_eq!(props.in_commit_timestamp_enablement(), Some((3, 123)));
    }
}
