use std::collections::HashMap;
use std::sync::Arc;

use sail_common::datetime::warn_if_spark_session_timezone_mismatches_local_timezone;
use sail_plan::config::{PlanConfig, TimestampType};
use sail_plan::formatter::DefaultPlanFormatter;
use sail_python_udf::config::PySparkUdfConfig;

use crate::error::{SparkError, SparkResult};
use crate::spark::config::{
    SPARK_CONFIG, SPARK_SQL_ANSI_ENABLED, SPARK_SQL_EXECUTION_ARROW_MAX_RECORDS_PER_BATCH,
    SPARK_SQL_EXECUTION_ARROW_USE_LARGE_VAR_TYPES,
    SPARK_SQL_EXECUTION_PANDAS_CONVERT_TO_ARROW_ARRAY_SAFELY, SPARK_SQL_GLOBAL_TEMP_DATABASE,
    SPARK_SQL_LEGACY_EXECUTION_PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME,
    SPARK_SQL_SESSION_TIME_ZONE, SPARK_SQL_SOURCES_DEFAULT, SPARK_SQL_TIMESTAMP_TYPE,
    SPARK_SQL_WAREHOUSE_DIR,
};
use crate::spark::connect as sc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct ConfigKeyValue {
    pub key: String,
    pub value: Option<String>,
}

impl From<sc::KeyValue> for ConfigKeyValue {
    fn from(kv: sc::KeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
        }
    }
}

impl From<ConfigKeyValue> for sc::KeyValue {
    fn from(kv: ConfigKeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
        }
    }
}

pub(crate) struct SparkRuntimeConfig {
    config: HashMap<String, String>,
}

impl SparkRuntimeConfig {
    pub(crate) fn new() -> Self {
        Self {
            config: HashMap::new(),
        }
    }

    fn validate_removed_key(key: &str, value: &str) -> SparkResult<()> {
        if let Some(entry) = SPARK_CONFIG.get(key) {
            if entry.removed.is_some() && entry.default_value != Some(value) {
                return Err(SparkError::invalid(format!(
                    "configuration has been removed: {}",
                    key
                )));
            }
        }
        Ok(())
    }

    fn get_by_key(&self, key: &str) -> Option<&str> {
        // TODO: Spark allows variable substitution via Java system properties, environment variables,
        //   or other configuration values. This is not supported here.
        if let Some(value) = self.config.get(key) {
            return Some(value.as_str());
        }
        let entry = SPARK_CONFIG.get(key);
        for alt in entry.map(|x| x.alternatives).unwrap_or(&[]) {
            if let Some(value) = self.config.get(*alt) {
                return Some(value.as_str());
            }
        }
        None
    }

    pub(crate) fn get(&self, key: &str) -> SparkResult<Option<&str>> {
        if let Some(value) = self.get_by_key(key) {
            return Ok(Some(value));
        }
        let entry = SPARK_CONFIG.get(key);
        if let Some(fallback) = entry.and_then(|x| x.fallback) {
            return self.get(fallback);
        }
        if let Some(entry) = entry {
            return Ok(entry.default_value);
        }
        Err(SparkError::invalid(format!(
            "configuration not found: {key}"
        )))
    }

    pub(crate) fn get_with_default<'a>(
        &'a self,
        key: &'a str,
        default: Option<&'a str>,
    ) -> SparkResult<Option<&'a str>> {
        if let Some(value) = self.get_by_key(key) {
            return Ok(Some(value));
        }
        let entry = SPARK_CONFIG.get(key);
        if let Some(fallback) = entry.and_then(|x| x.fallback) {
            return self.get_with_default(fallback, default);
        }
        Ok(default)
    }

    pub(crate) fn set(&mut self, key: String, value: String) -> SparkResult<()> {
        Self::validate_removed_key(key.as_str(), value.as_str())?;
        self.config.insert(key, value);
        Ok(())
    }

    pub(crate) fn unset(&mut self, key: &str) -> SparkResult<()> {
        self.config.remove(key);
        Ok(())
    }

    pub(crate) fn get_all(&self, prefix: Option<&str>) -> SparkResult<Vec<ConfigKeyValue>> {
        let iter: Box<dyn Iterator<Item = _>> = match prefix {
            None => Box::new(self.config.iter()),
            Some(prefix) => Box::new(
                self.config
                    .iter()
                    .filter(move |(k, _)| k.starts_with(prefix)),
            ),
        };
        Ok(iter
            .map(|(k, v)| ConfigKeyValue {
                key: k.to_string(),
                value: Some(v.to_string()),
            })
            .collect())
    }

    pub(crate) fn is_modifiable(key: &str) -> bool {
        SPARK_CONFIG
            .get(key)
            .map(|entry| !entry.is_static && entry.removed.is_none())
            .unwrap_or(false)
    }

    fn get_warning(key: &str) -> Option<&str> {
        SPARK_CONFIG
            .get(key)
            .and_then(|entry| entry.deprecated.as_ref())
            .map(|x| x.comment)
    }

    pub(crate) fn get_warnings(kv: &[ConfigKeyValue]) -> Vec<String> {
        kv.iter()
            .flat_map(|x| Self::get_warning(x.key.as_str()))
            .map(|x| x.to_string())
            .collect()
    }

    pub(crate) fn get_warnings_by_keys(keys: &[String]) -> Vec<String> {
        keys.iter()
            .flat_map(|x| Self::get_warning(x.as_str()))
            .map(|x| x.to_string())
            .collect()
    }
}

impl TryFrom<&SparkRuntimeConfig> for PlanConfig {
    type Error = SparkError;

    fn try_from(config: &SparkRuntimeConfig) -> SparkResult<Self> {
        let mut output = PlanConfig::new()?;

        if let Some(value) = config
            .get(SPARK_SQL_SESSION_TIME_ZONE)?
            .map(|x| x.to_string())
        {
            output.session_timezone = value;
            warn_if_spark_session_timezone_mismatches_local_timezone(
                output.session_timezone.as_str(),
                output.system_timezone.as_str(),
            )?;
        }

        if let Some(value) = config
            .get(SPARK_SQL_EXECUTION_ARROW_USE_LARGE_VAR_TYPES)?
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.arrow_use_large_var_types = value;
        }

        if let Some(value) = config
            .get(SPARK_SQL_SOURCES_DEFAULT)?
            .map(|x| x.to_string())
        {
            output.default_bounded_table_file_format = value;
        }

        if let Some(value) = config.get(SPARK_SQL_WAREHOUSE_DIR)? {
            output.default_warehouse_directory = value.to_string();
        }

        if let Some(value) = config.get(SPARK_SQL_GLOBAL_TEMP_DATABASE)? {
            output.global_temp_database = value.to_string();
        }

        if let Some(value) = config.get(SPARK_SQL_TIMESTAMP_TYPE)? {
            let value = value.to_uppercase().trim().to_string();
            if value == "TIMESTAMP_NTZ" {
                output.timestamp_type = TimestampType::TimestampNtz;
            } else if value.is_empty() || value == "TIMESTAMP_LTZ" {
                output.timestamp_type = TimestampType::TimestampLtz;
            } else {
                return Err(SparkError::invalid(format!(
                    "invalid timestamp type: {value}"
                )));
            }
        }

        if let Some(value) = config
            .get(SPARK_SQL_ANSI_ENABLED)?
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.ansi_mode = value;
        }

        output.plan_formatter = Arc::new(DefaultPlanFormatter);
        output.pyspark_udf_config = Arc::new(PySparkUdfConfig::try_from(config)?);

        Ok(output)
    }
}

impl TryFrom<&SparkRuntimeConfig> for PySparkUdfConfig {
    type Error = SparkError;

    fn try_from(config: &SparkRuntimeConfig) -> SparkResult<Self> {
        let mut output = PySparkUdfConfig::default();

        if let Some(value) = config
            .get(SPARK_SQL_SESSION_TIME_ZONE)?
            .map(|x| x.to_string())
        {
            output.session_timezone = value;
        }

        if let Some(value) = config
            .get(SPARK_SQL_LEGACY_EXECUTION_PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME)?
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.pandas_grouped_map_assign_columns_by_name = value;
        }

        if let Some(value) = config
            .get(SPARK_SQL_EXECUTION_PANDAS_CONVERT_TO_ARROW_ARRAY_SAFELY)?
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.pandas_convert_to_arrow_array_safely = value;
        }

        if let Some(value) = config
            .get(SPARK_SQL_EXECUTION_ARROW_MAX_RECORDS_PER_BATCH)?
            .map(|x| x.parse::<usize>())
            .transpose()?
        {
            output.arrow_max_records_per_batch = value;
        }

        Ok(output)
    }
}
