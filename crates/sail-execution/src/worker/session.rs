// SPDX-License-Identifier: Apache-2.0

//! The driver session settings shipped with each job to workers.
//!
//! A job runs with the settings of the driver session at the time the job was submitted. Tasks
//! that run on the driver use that session directly. Tasks that run on a worker get a task context
//! that the worker builds from these settings once per job (see `JobContextFactory`).

use std::collections::HashMap;
use std::sync::LazyLock;

use datafusion::common::config::{ConfigField, ConfigOptions, ParquetOptions};
use datafusion::common::{Result, internal_datafusion_err, internal_err};
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use prost::Message;
use sail_common_datafusion::session::time_zone::{
    set_spark_session_time_zone, spark_session_time_zone,
};

use crate::worker::r#gen::JobSession;

const DATAFUSION_PREFIX: &str = "datafusion.";
const PARQUET_PREFIX: &str = "datafusion.execution.parquet.";

/// The value of each DataFusion option in its default configuration.
static DEFAULT_ENTRIES: LazyLock<HashMap<String, Option<String>>> =
    LazyLock::new(|| entry_values(&ConfigOptions::default()).collect());

/// Returns the key and the printed value of each option.
fn entry_values(options: &ConfigOptions) -> impl Iterator<Item = (String, Option<String>)> {
    options
        .entries()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
}

impl JobSession {
    /// Captures the settings of a driver session.
    ///
    /// Only options that differ from the DataFusion defaults are captured by key, since the
    /// worker applies them on top of the defaults. The Parquet options are captured as a whole,
    /// since some of them cannot be unset by key. The Spark session time zone is kept in a
    /// configuration extension that has no entries, so it is captured separately.
    pub(crate) fn capture(options: &ConfigOptions) -> Result<Self> {
        let mut values = HashMap::new();
        for (key, value) in entry_values(options) {
            if !key.starts_with(DATAFUSION_PREFIX) || key.starts_with(PARQUET_PREFIX) {
                continue;
            }
            if DEFAULT_ENTRIES.get(&key) == Some(&value) {
                continue;
            }
            let Some(value) = value else {
                return internal_err!("cannot ship the unset session option {key} to workers");
            };
            values.insert(key, value);
        }
        let parquet = gen_datafusion_common::ParquetOptions::try_from(&options.execution.parquet)?
            .encode_to_vec();
        Ok(Self {
            options: values,
            parquet,
            spark_time_zone: spark_session_time_zone(options).map(ToString::to_string),
        })
    }

    /// Returns the options of the driver session.
    pub(crate) fn config_options(&self) -> Result<ConfigOptions> {
        let mut options = ConfigOptions::default();
        for (key, value) in &self.options {
            let Some(field) = key.strip_prefix(DATAFUSION_PREFIX) else {
                return internal_err!("invalid session option from the driver: {key}");
            };
            // Set the field itself rather than calling `ConfigOptions::set`, which expands
            // some keys to several fields.
            ConfigField::set(&mut options, field, value)
                .map_err(|e| e.context(format!("cannot apply session option {key}")))?;
        }
        let parquet = gen_datafusion_common::ParquetOptions::decode(self.parquet.as_slice())
            .map_err(|e| {
                internal_datafusion_err!("invalid Parquet options from the driver: {e}")
            })?;
        options.execution.parquet = ParquetOptions::try_from(&parquet)?;
        if let Some(time_zone) = &self.spark_time_zone {
            set_spark_session_time_zone(&mut options, time_zone)?;
        }
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::config::{ConfigNonZeroUsize, SpillCompression};
    use datafusion::common::parquet_config::DFTimeUnit;
    use sail_common_datafusion::session::time_zone::session_time_zone;

    use super::*;

    fn entries(options: &ConfigOptions) -> Vec<(String, Option<String>)> {
        entry_values(options).collect()
    }

    /// The options are shipped by their printed values, so each printed value must set the
    /// option to the same value. Options that are unset by default are never shipped unset.
    #[test]
    fn printed_option_values_set_the_same_values() -> Result<()> {
        let defaults = entries(&ConfigOptions::default());
        for (key, value) in &defaults {
            if key.starts_with(PARQUET_PREFIX) {
                continue;
            }
            let (Some(field), Some(value)) = (key.strip_prefix(DATAFUSION_PREFIX), value) else {
                continue;
            };
            let mut options = ConfigOptions::default();
            ConfigField::set(&mut options, field, value)?;
            assert_eq!(entries(&options), defaults, "option {key} = {value}");
        }
        Ok(())
    }

    #[test]
    fn job_session_round_trips_driver_options() -> Result<()> {
        let mut driver = ConfigOptions::default();
        driver.execution.batch_size = ConfigNonZeroUsize::try_new(7)?;
        driver.execution.target_partitions = 3;
        driver.execution.collect_statistics = false;
        driver.execution.spill_compression = SpillCompression::Zstd;
        driver.execution.enable_nlj_coordinated_fallback = false;
        driver.catalog.create_default_catalog_and_schema = false;
        driver.catalog.information_schema = false;
        driver.optimizer.prefer_hash_join = false;
        driver.optimizer.enable_join_dynamic_filter_pushdown = false;
        driver.execution.parquet.metadata_size_hint = None;
        driver.execution.parquet.coerce_int96 = Some(DFTimeUnit::Nanosecond);
        driver.execution.parquet.binary_as_string = true;
        driver.execution.parquet.compression = None;
        set_spark_session_time_zone(&mut driver, "+05:30:15")?;

        let session = JobSession::decode(JobSession::capture(&driver)?.encode_to_vec().as_slice())
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        assert!(
            session
                .options
                .keys()
                .all(|key| !key.starts_with(PARQUET_PREFIX))
        );
        let options = session.config_options()?;

        assert_eq!(entries(&options), entries(&driver));
        assert_eq!(options.execution.parquet, driver.execution.parquet);
        assert_eq!(session_time_zone(&options), Some("+05:30:15"));
        assert!(!options.optimizer.enable_join_dynamic_filter_pushdown);
        assert!(options.optimizer.enable_topk_dynamic_filter_pushdown);
        Ok(())
    }

    #[test]
    fn job_session_without_changes_has_no_options() -> Result<()> {
        let session = JobSession::capture(&ConfigOptions::default())?;
        assert!(session.options.is_empty());
        assert_eq!(session.spark_time_zone, None);
        assert_eq!(
            entries(&session.config_options()?),
            entries(&ConfigOptions::default())
        );
        Ok(())
    }

    #[test]
    fn job_session_rejects_unknown_options() {
        let session = JobSession {
            options: HashMap::from([("datafusion.execution.unknown".to_string(), "1".to_string())]),
            ..Default::default()
        };
        assert!(session.config_options().is_err());
    }
}
