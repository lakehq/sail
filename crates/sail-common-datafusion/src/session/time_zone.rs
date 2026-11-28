// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use datafusion_common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion_common::{Result, config_err};

use crate::utils::datetime::arrow_timezone_label;

#[derive(Clone, Debug)]
struct SparkSessionTimeZone(String);

impl ConfigExtension for SparkSessionTimeZone {
    const PREFIX: &'static str = "sail_spark_session_time_zone";
}

impl ExtensionOptions for SparkSessionTimeZone {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        config_err!("unknown session time zone option: {key}")
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// Keep the original Spark time zone alongside the label used by Arrow and DataFusion.
pub fn set_spark_session_time_zone(options: &mut ConfigOptions, time_zone: &str) -> Result<()> {
    options.execution.time_zone = Some(arrow_timezone_label(time_zone).parse()?);
    options
        .extensions
        .insert(SparkSessionTimeZone(time_zone.to_owned()));
    Ok(())
}

/// Read the Spark session time zone set by [`set_spark_session_time_zone`], if any.
pub fn spark_session_time_zone(options: &ConfigOptions) -> Option<&str> {
    options
        .extensions
        .get::<SparkSessionTimeZone>()
        .map(|zone| zone.0.as_str())
}

/// Read the session time zone with full Spark offset precision when available.
pub fn session_time_zone(options: &ConfigOptions) -> Option<&str> {
    spark_session_time_zone(options).or_else(|| {
        options
            .execution
            .time_zone
            .as_ref()
            .map(|zone| zone.as_str())
    })
}
