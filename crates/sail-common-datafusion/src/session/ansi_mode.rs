// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use datafusion_common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion_common::{Result, config_err};

/// `spark.sql.ansi.enabled` of the Spark session, for planning code outside the resolver.
///
/// It is not shipped to workers. Code that runs on workers takes the value from the plan, so the
/// value must be read while planning on the driver.
#[derive(Clone, Debug)]
struct SparkSessionAnsiMode(bool);

impl ConfigExtension for SparkSessionAnsiMode {
    const PREFIX: &'static str = "sail_spark_session_ansi_mode";
}

impl ExtensionOptions for SparkSessionAnsiMode {
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
        config_err!("unknown session ANSI mode option: {key}")
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

pub fn set_spark_session_ansi_mode(options: &mut ConfigOptions, ansi_mode: bool) {
    options.extensions.insert(SparkSessionAnsiMode(ansi_mode));
}

/// Read the value set by [`set_spark_session_ansi_mode`], which is Sail's default (true) when it
/// was not set.
pub fn spark_session_ansi_mode(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<SparkSessionAnsiMode>()
        .is_none_or(|ansi_mode| ansi_mode.0)
}

#[cfg(test)]
mod tests {
    use datafusion_common::config::ConfigOptions;

    use super::{set_spark_session_ansi_mode, spark_session_ansi_mode};

    #[test]
    fn ansi_mode_is_on_unless_set_off() {
        let mut options = ConfigOptions::new();
        assert!(spark_session_ansi_mode(&options));
        set_spark_session_ansi_mode(&mut options, false);
        assert!(!spark_session_ansi_mode(&options));
        set_spark_session_ansi_mode(&mut options, true);
        assert!(spark_session_ansi_mode(&options));
    }
}
