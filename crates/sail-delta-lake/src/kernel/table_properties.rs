use std::num::NonZeroU64;
use std::sync::LazyLock;
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
const DEFAULT_DELETED_FILE_RETENTION_SECS: u64 = 1 * SECONDS_PER_WEEK;

static DEFAULT_CHECKPOINT_INTERVAL: LazyLock<NonZeroU64> =
    LazyLock::new(|| NonZeroU64::new(100).expect("non zero"));

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
            .unwrap_or_else(|| *DEFAULT_CHECKPOINT_INTERVAL)
    }

    fn deleted_file_retention_duration(&self) -> Duration {
        self.deleted_file_retention_duration
            .unwrap_or(Duration::from_secs(DEFAULT_DELETED_FILE_RETENTION_SECS))
    }

    fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap_or_default()
    }
}
