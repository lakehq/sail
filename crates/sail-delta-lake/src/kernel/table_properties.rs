// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/table/config.rs>
use std::num::NonZeroU64;
use std::time::Duration;

use delta_kernel::table_properties::{IsolationLevel, TableProperties};

/// Convenience helpers for accessing common table properties with sensible defaults.
pub trait TablePropertiesExt {
    fn append_only(&self) -> bool;
    fn log_retention_duration(&self) -> Duration;
    fn enable_expired_log_cleanup(&self) -> bool;
    fn checkpoint_interval(&self) -> NonZeroU64;
    fn deleted_file_retention_duration(&self) -> Duration;
    fn isolation_level(&self) -> IsolationLevel;
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

const DEFAULT_LOG_RETENTION_SECS: u64 = 30 * SECONDS_PER_DAY;
const DEFAULT_DELETED_FILE_RETENTION_SECS: u64 = SECONDS_PER_WEEK;
const DEFAULT_CHECKPOINT_INTERVAL: NonZeroU64 =
    NonZeroU64::new(100).expect("non-zero checkpoint interval");

impl TablePropertiesExt for TableProperties {
    fn append_only(&self) -> bool {
        self.append_only.unwrap_or(false)
    }

    fn log_retention_duration(&self) -> Duration {
        self.log_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_LOG_RETENTION_SECS))
    }

    fn enable_expired_log_cleanup(&self) -> bool {
        self.enable_expired_log_cleanup.unwrap_or(true)
    }

    fn checkpoint_interval(&self) -> NonZeroU64 {
        self.checkpoint_interval
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL)
    }

    fn deleted_file_retention_duration(&self) -> Duration {
        self.deleted_file_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_DELETED_FILE_RETENTION_SECS))
    }

    fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap_or_default()
    }
}
