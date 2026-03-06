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
    pub column_mapping_mode: Option<ColumnMappingMode>,
    pub data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,
    pub data_skipping_stats_columns: Option<Vec<ColumnName>>,
    pub deleted_file_retention_duration: Option<Duration>,
    pub isolation_level: Option<IsolationLevel>,
    pub log_retention_duration: Option<Duration>,
    pub enable_expired_log_cleanup: Option<bool>,
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
const DEFAULT_CHECKPOINT_INTERVAL: NonZeroU64 =
    NonZeroU64::new(100).expect("non-zero checkpoint interval");

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

    pub fn deleted_file_retention_duration(&self) -> Duration {
        self.deleted_file_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_DELETED_FILE_RETENTION_SECS))
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap_or_default()
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
