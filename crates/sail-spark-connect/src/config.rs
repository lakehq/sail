use std::collections::HashMap;

use sail_common::config::ConfigKeyValue;

use crate::error::{SparkError, SparkResult};
use crate::spark::config::SPARK_CONFIG;
use crate::spark::connect as sc;

pub(crate) struct ConfigKeyValueList(Vec<ConfigKeyValue>);

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

impl From<Vec<ConfigKeyValue>> for ConfigKeyValueList {
    fn from(kv: Vec<ConfigKeyValue>) -> Self {
        Self(kv)
    }
}

impl From<ConfigKeyValueList> for Vec<ConfigKeyValue> {
    fn from(kv: ConfigKeyValueList) -> Self {
        kv.0
    }
}

impl From<Vec<sc::KeyValue>> for ConfigKeyValueList {
    fn from(kv: Vec<sc::KeyValue>) -> Self {
        Self(kv.into_iter().map(|x| x.into()).collect())
    }
}

impl From<ConfigKeyValueList> for Vec<sc::KeyValue> {
    fn from(kv: ConfigKeyValueList) -> Self {
        kv.0.into_iter().map(|x| x.into()).collect()
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

    pub(crate) fn get_all(&self, prefix: Option<&str>) -> SparkResult<ConfigKeyValueList> {
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
            .collect::<Vec<_>>()
            .into())
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

    pub(crate) fn get_warnings(kv: &ConfigKeyValueList) -> Vec<String> {
        kv.0.iter()
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
