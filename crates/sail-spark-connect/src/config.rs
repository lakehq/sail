use std::collections::HashMap;
use std::sync::Arc;

use sail_plan::config::{
    DefaultTimestampType, MapKeyDedupPolicy, PlanConfig, StoreAssignmentPolicy,
};
use sail_python_udf::config::PySparkUdfConfig;

use crate::error::{SparkError, SparkResult};
use crate::spark::config::{
    SPARK_CONFIG_V3_5, SPARK_CONFIG_V4_0, SPARK_CONFIG_V4_1, SPARK_CONFIG_V4_2, SparkConfigKey,
};
use crate::spark::connect;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct ConfigKeyValue {
    pub key: String,
    pub value: Option<String>,
}

impl From<connect::KeyValue> for ConfigKeyValue {
    fn from(kv: connect::KeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
        }
    }
}

impl From<ConfigKeyValue> for connect::KeyValue {
    fn from(kv: ConfigKeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
        }
    }
}

pub(crate) struct SparkRuntimeConfig {
    entries:
        &'static phf::Map<&'static str, &'static crate::spark::config::SparkConfigEntry<'static>>,
    config: HashMap<String, String>,
}

impl SparkRuntimeConfig {
    pub(crate) fn try_new() -> SparkResult<Self> {
        let entries = match get_pyspark_version() {
            Ok(version) => {
                let mut parts = version.split('.');
                match (parts.next(), parts.next()) {
                    // Use the Spark 3.5 configuration to provide best-effort support
                    // for all 3.x versions.
                    (Some("3"), _) => &SPARK_CONFIG_V3_5,
                    (Some("4"), Some("0")) => &SPARK_CONFIG_V4_0,
                    (Some("4"), Some("1")) => &SPARK_CONFIG_V4_1,
                    (Some("4"), Some("2")) => &SPARK_CONFIG_V4_2,
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "unsupported PySpark version: {version}"
                        )));
                    }
                }
            }
            Err(_) => {
                // Use the earliest Spark configuration when we cannot determine the PySpark version,
                // which can happen when running Rust tests for example.
                &SPARK_CONFIG_V3_5
            }
        };
        Ok(Self {
            entries,
            config: HashMap::new(),
        })
    }

    fn validate_removed_key(&self, key: &str, value: &str) -> SparkResult<()> {
        if let Some(entry) = self.entries.get(key)
            && entry.removed.is_some()
            && entry.default_value != Some(value)
        {
            return Err(SparkError::invalid(format!(
                "configuration has been removed: {key}"
            )));
        }
        Ok(())
    }

    fn get_by_key(&self, key: &str) -> Option<&str> {
        // TODO: Spark allows variable substitution via Java system properties, environment variables,
        //   or other configuration values. This is not supported here.
        if let Some(value) = self.config.get(key) {
            return Some(value.as_str());
        }
        let entry = self.entries.get(key);
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
        let entry = self.entries.get(key);
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

    pub(crate) fn get_option(&self, key: &str) -> Option<&str> {
        if let Some(value) = self.get_by_key(key) {
            return Some(value);
        }
        let entry = self.entries.get(key);
        if let Some(fallback) = entry.and_then(|x| x.fallback) {
            return self.get_option(fallback);
        }
        entry.and_then(|x| x.default_value)
    }

    pub(crate) fn get_with_default<'a>(
        &'a self,
        key: &'a str,
        default: Option<&'a str>,
    ) -> Option<&'a str> {
        if let Some(value) = self.get_by_key(key) {
            return Some(value);
        }
        let entry = self.entries.get(key);
        if let Some(fallback) = entry.and_then(|x| x.fallback) {
            return self.get_with_default(fallback, default);
        }
        default
    }

    pub(crate) fn set(&mut self, key: String, value: String) -> SparkResult<()> {
        self.validate_removed_key(key.as_str(), value.as_str())?;
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

    pub(crate) fn is_modifiable(&self, key: &str) -> bool {
        self.entries
            .get(key)
            .map(|entry| !entry.is_static && entry.removed.is_none())
            .unwrap_or(false)
    }

    fn get_warning(&self, key: &str) -> Option<&str> {
        self.entries
            .get(key)
            .and_then(|entry| entry.deprecated.as_ref())
            .map(|x| x.comment)
    }

    pub(crate) fn get_warnings(&self, kv: &[ConfigKeyValue]) -> Vec<String> {
        kv.iter()
            .flat_map(|x| self.get_warning(x.key.as_str()))
            .map(|x| x.to_string())
            .collect()
    }

    pub(crate) fn get_warnings_by_keys(&self, keys: &[String]) -> Vec<String> {
        keys.iter()
            .flat_map(|x| self.get_warning(x.as_str()))
            .map(|x| x.to_string())
            .collect()
    }
}

pub(crate) fn get_pyspark_version() -> SparkResult<String> {
    use pyo3::Python;
    use pyo3::prelude::PyAnyMethods;
    use pyo3::types::PyModule;

    Python::attach(|py| {
        let module = PyModule::import(py, "pyspark")?;
        let version: String = module.getattr("__version__")?.extract()?;
        Ok(version)
    })
    .map_err(|e: pyo3::PyErr| SparkError::invalid(format!("failed to get PySpark version: {e}")))
}

// We must use `get_option` when extracting values from `SparkRuntimeConfig`
// since not all configuration keys are supported in all versions of Spark.

impl TryFrom<&SparkRuntimeConfig> for PlanConfig {
    type Error = SparkError;

    fn try_from(config: &SparkRuntimeConfig) -> SparkResult<Self> {
        let mut output = PlanConfig::new()?;

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE)
            .map(|x| x.to_string())
        {
            output.session_timezone = Arc::from(value);
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_EXECUTION_ARROW_USE_LARGE_VAR_TYPES)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.arrow_use_large_var_types = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_SOURCES_DEFAULT)
            .map(|x| x.to_string())
        {
            output.default_table_file_format = value;
        }

        if let Some(value) = config.get_option(SparkConfigKey::SPARK_SQL_WAREHOUSE_DIR) {
            output.default_warehouse_directory = value.to_string();
        }

        if let Some(value) = config.get_option(SparkConfigKey::SPARK_SQL_TIMESTAMP_TYPE) {
            let value = value.to_uppercase().trim().to_string();
            if value == "TIMESTAMP_NTZ" {
                output.default_timestamp_type = DefaultTimestampType::TimestampNtz;
            } else if value.is_empty() || value == "TIMESTAMP_LTZ" {
                output.default_timestamp_type = DefaultTimestampType::TimestampLtz;
            } else {
                return Err(SparkError::invalid(format!(
                    "invalid timestamp type: {value}"
                )));
            }
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_ANSI_ENABLED)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.ansi_mode = value;
        }

        if let Some(value) = config.get_option(SparkConfigKey::SPARK_SQL_STORE_ASSIGNMENT_POLICY) {
            output.store_assignment_policy = match value.trim().to_ascii_uppercase().as_str() {
                "ANSI" => StoreAssignmentPolicy::Ansi,
                "STRICT" => StoreAssignmentPolicy::Strict,
                "LEGACY" => StoreAssignmentPolicy::Legacy,
                _ => {
                    return Err(SparkError::invalid(format!(
                        "invalid store assignment policy: {value}"
                    )));
                }
            };
        }

        if let Some(value) = config.get_option(SparkConfigKey::SPARK_SQL_MAP_KEY_DEDUP_POLICY) {
            output.map_key_dedup_policy = match value.trim().to_ascii_uppercase().as_str() {
                "EXCEPTION" => MapKeyDedupPolicy::Exception,
                "LAST_WIN" => MapKeyDedupPolicy::LastWin,
                _ => {
                    return Err(SparkError::invalid(format!(
                        "invalid map key dedup policy: {value}"
                    )));
                }
            };
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_CROSS_JOIN_ENABLED)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.cross_join_enabled = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_CASE_SENSITIVE)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.case_sensitive = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_PIVOT_MAX_VALUES)
            .map(|x| x.trim().parse::<usize>())
            .transpose()?
        {
            output.pivot_max_values = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_TVF_ALLOW_MULTIPLE_TABLE_ARGUMENTS_ENABLED)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.tvf_allow_multiple_table_arguments = value;
        }

        if let Some(value) = config.get_option(SparkConfigKey::SPARK_SQL_FILES_MAX_PARTITION_BYTES)
        {
            output.file_scan_partitioning.max_partition_bytes =
                parse_spark_bytes(SparkConfigKey::SPARK_SQL_FILES_MAX_PARTITION_BYTES, value)?;
        }
        if let Some(value) = config.get_option(SparkConfigKey::SPARK_SQL_FILES_OPEN_COST_IN_BYTES) {
            output.file_scan_partitioning.open_cost_bytes =
                parse_spark_bytes(SparkConfigKey::SPARK_SQL_FILES_OPEN_COST_IN_BYTES, value)?;
        }
        output.file_scan_partitioning.min_partitions = config
            .get_option(SparkConfigKey::SPARK_SQL_FILES_MIN_PARTITION_NUM)
            .map(|value| (SparkConfigKey::SPARK_SQL_FILES_MIN_PARTITION_NUM, value))
            .or_else(|| {
                config
                    .get_option(SparkConfigKey::SPARK_SQL_LEAF_NODE_DEFAULT_PARALLELISM)
                    .map(|value| {
                        (
                            SparkConfigKey::SPARK_SQL_LEAF_NODE_DEFAULT_PARALLELISM,
                            value,
                        )
                    })
            })
            .map(|(key, value)| parse_positive_usize(key, value))
            .transpose()?;
        output.file_scan_partitioning.max_partitions = config
            .get_option(SparkConfigKey::SPARK_SQL_FILES_MAX_PARTITION_NUM)
            .map(|value| {
                parse_positive_usize(SparkConfigKey::SPARK_SQL_FILES_MAX_PARTITION_NUM, value)
            })
            .transpose()?;

        output.pyspark_udf_config = Arc::new(PySparkUdfConfig::try_from(config)?);

        Ok(output)
    }
}

fn parse_spark_bytes(key: &str, value: &str) -> SparkResult<i64> {
    let original = value;
    let (value, sign) = match value.strip_prefix('-') {
        Some(value) => (value, -1),
        None => (value, 1),
    };
    let value = value.trim().to_ascii_lowercase();
    let number_end = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(number_end);
    let multiplier = match suffix {
        "" | "b" => 1,
        "k" | "kb" => 1_i64 << 10,
        "m" | "mb" => 1_i64 << 20,
        "g" | "gb" => 1_i64 << 30,
        "t" | "tb" => 1_i64 << 40,
        "p" | "pb" => 1_i64 << 50,
        _ => {
            return Err(SparkError::invalid(format!(
                "invalid byte size for {key}: {original}"
            )));
        }
    };
    number
        .parse::<i64>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .and_then(|number| number.checked_mul(sign))
        .ok_or_else(|| SparkError::invalid(format!("invalid byte size for {key}: {original}")))
}

fn parse_positive_usize(key: &str, value: &str) -> SparkResult<usize> {
    match value.trim().parse::<i32>() {
        Ok(value) if value > 0 => Ok(value as usize),
        _ => Err(SparkError::invalid(format!(
            "invalid positive integer for {key}: {value}"
        ))),
    }
}

impl TryFrom<&SparkRuntimeConfig> for PySparkUdfConfig {
    type Error = SparkError;

    fn try_from(config: &SparkRuntimeConfig) -> SparkResult<Self> {
        let mut output = PySparkUdfConfig::default();

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE)
            .map(|x| x.to_string())
        {
            output.session_timezone = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_LEGACY_EXECUTION_PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.pandas_grouped_map_assign_columns_by_name = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_EXECUTION_PANDAS_CONVERT_TO_ARROW_ARRAY_SAFELY)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.pandas_convert_to_arrow_array_safely = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_EXECUTION_ARROW_MAX_RECORDS_PER_BATCH)
            .map(|x| x.parse::<i128>())
            .transpose()?
        {
            output.arrow_max_records_per_batch = if value <= 0 || value > usize::MAX as i128 {
                usize::MAX
            } else {
                value as usize
            };
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_EXECUTION_ARROW_USE_LARGE_VAR_TYPES)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.arrow_use_large_var_types = value;
        }

        if let Some(value) = config
            .get_option(
                SparkConfigKey::SPARK_SQL_LEGACY_EXECUTION_PYTHON_UDF_PANDAS_CONVERSION_ENABLED,
            )
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.python_udf_pandas_conversion_enabled = value;
        }

        if let Some(value) = config
            .get_option(
                SparkConfigKey::SPARK_SQL_LEGACY_EXECUTION_PYTHON_UDTF_PANDAS_CONVERSION_ENABLED,
            )
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.python_udtf_pandas_conversion_enabled = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_EXECUTION_PYTHON_UDF_PANDAS_INT_TO_DECIMAL_COERCION_ENABLED)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.python_udf_pandas_int_to_decimal_coercion_enabled = value;
        }

        if let Some(value) = config
            .get_option(
                SparkConfigKey::SPARK_SQL_EXECUTION_PYTHON_UDF_PANDAS_PREFER_INT_EXTENSION_DTYPE,
            )
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.python_udf_pandas_prefer_int_extension_dtype = value;
        }

        if let Some(value) = config
            .get_option(SparkConfigKey::SPARK_SQL_EXECUTION_PYSPARK_BINARY_AS_BYTES)
            .map(|x| x.to_lowercase().parse::<bool>())
            .transpose()?
        {
            output.binary_as_bytes = value;
        }

        Ok(output)
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_config() -> SparkRuntimeConfig {
        pyo3::Python::initialize();
        SparkRuntimeConfig::try_new().unwrap()
    }

    #[test]
    fn spark_file_partition_defaults_match_sql_conf() {
        let runtime = runtime_config();

        let config = PlanConfig::try_from(&runtime).unwrap();

        assert_eq!(config.file_scan_partitioning.max_partition_bytes, 128 << 20);
        assert_eq!(config.file_scan_partitioning.open_cost_bytes, 4 << 20);
        assert_eq!(config.file_scan_partitioning.min_partitions, None);
        assert_eq!(config.file_scan_partitioning.max_partitions, None);
    }

    #[test]
    fn parses_spark_file_partition_overrides() {
        let mut runtime = runtime_config();
        runtime
            .set(
                SparkConfigKey::SPARK_SQL_FILES_MAX_PARTITION_BYTES.to_string(),
                "64MB".to_string(),
            )
            .unwrap();
        runtime
            .set(
                SparkConfigKey::SPARK_SQL_FILES_OPEN_COST_IN_BYTES.to_string(),
                "8m".to_string(),
            )
            .unwrap();
        runtime
            .set(
                SparkConfigKey::SPARK_SQL_FILES_MIN_PARTITION_NUM.to_string(),
                "3".to_string(),
            )
            .unwrap();
        runtime
            .set(
                SparkConfigKey::SPARK_SQL_FILES_MAX_PARTITION_NUM.to_string(),
                "7".to_string(),
            )
            .unwrap();

        let config = PlanConfig::try_from(&runtime).unwrap();

        assert_eq!(config.file_scan_partitioning.max_partition_bytes, 64 << 20);
        assert_eq!(config.file_scan_partitioning.open_cost_bytes, 8 << 20);
        assert_eq!(config.file_scan_partitioning.min_partitions, Some(3));
        assert_eq!(config.file_scan_partitioning.max_partitions, Some(7));
    }

    #[test]
    fn spark_file_min_partitions_fall_back_to_leaf_node_default() {
        let mut runtime = runtime_config();
        runtime
            .set(
                SparkConfigKey::SPARK_SQL_LEAF_NODE_DEFAULT_PARALLELISM.to_string(),
                "5".to_string(),
            )
            .unwrap();

        let config = PlanConfig::try_from(&runtime).unwrap();
        assert_eq!(config.file_scan_partitioning.min_partitions, Some(5));

        runtime
            .set(
                SparkConfigKey::SPARK_SQL_FILES_MIN_PARTITION_NUM.to_string(),
                "3".to_string(),
            )
            .unwrap();

        let config = PlanConfig::try_from(&runtime).unwrap();
        assert_eq!(config.file_scan_partitioning.min_partitions, Some(3));
    }

    #[test]
    fn spark_file_partition_counts_use_positive_int_range() {
        assert_eq!(
            parse_positive_usize("test", "2147483647").unwrap(),
            i32::MAX as usize
        );
        for value in ["0", "-1", "2147483648"] {
            assert!(parse_positive_usize("test", value).is_err());
        }
    }

    #[test]
    fn spark_byte_sizes_use_binary_units() {
        assert_eq!(parse_spark_bytes("test", "1kb").unwrap(), 1 << 10);
        assert_eq!(parse_spark_bytes("test", "2MB").unwrap(), 2 << 20);
        assert_eq!(parse_spark_bytes("test", "-3GB").unwrap(), -(3_i64 << 30));
        assert!(parse_spark_bytes("test", "1.5mb").is_err());
        assert!(parse_spark_bytes("test", "9223372036854775808b").is_err());
    }
}
