use std::collections::HashMap;

use num_traits::Zero;
use serde::Deserialize;

use crate::error::{PlanError, PlanResult};

pub mod csv;
pub mod json;
pub mod parquet;

const fn default_true() -> bool {
    true
}

pub(crate) trait DataSourceOptions: Sized {
    const SOURCE_CONFIG: &'static str;
    fn try_from_options(options: HashMap<String, String>) -> PlanResult<Self>;
}

pub(crate) fn load_options<T: DataSourceOptions>(
    user_options: HashMap<String, String>,
) -> PlanResult<T> {
    let user_options_normalized: HashMap<String, String> = user_options
        .into_iter()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect();
    let config_items: Vec<ConfigItem> =
        serde_yaml::from_str(T::SOURCE_CONFIG).map_err(|e| PlanError::internal(e.to_string()))?;
    let options: HashMap<String, String> = config_items
        .into_iter()
        .filter_map(|item| match item.resolve_value(&user_options_normalized) {
            Some(value) => {
                if item.supported {
                    Some(Ok((item.key, value)))
                } else {
                    Some(Err(PlanError::unsupported(format!(
                        "Data Source option '{}' is not supported yet.",
                        item.key
                    ))))
                }
            }
            None => {
                if item.supported {
                    Some(Ok((item.key, item.default.clone())))
                } else {
                    None
                }
            }
        })
        .collect::<PlanResult<HashMap<String, String>>>()?;
    T::try_from_options(options)
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigItem {
    key: String,
    #[serde(default)]
    alias: Vec<String>,
    default: String,
    #[expect(unused)]
    description: String,
    #[serde(default = "default_true")]
    supported: bool,
}

impl ConfigItem {
    // user_options_normalized is expected to be a HashMap with all keys in lowercase
    fn resolve_value(&self, user_options_normalized: &HashMap<String, String>) -> Option<String> {
        // TODO: If both the key and its alias are present, this duplication is silently ignored,
        //  and the user cannot tell which one is chosen without looking at the code.
        if let Some(value) = user_options_normalized.get(&self.key.to_lowercase()) {
            return Some(value.clone());
        }
        for alias in &self.alias {
            if let Some(value) = user_options_normalized.get(&alias.to_lowercase()) {
                return Some(value.clone());
            }
        }
        None
    }
}

pub(crate) fn parse_non_empty_string(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

pub(crate) fn parse_non_empty_char(value: &str) -> PlanResult<Option<char>> {
    match value.chars().count() {
        0 => Ok(None),
        1 => Ok(value.chars().next()),
        _ => Err(PlanError::internal(format!(
            "Expected a single character, but got: '{value}'"
        ))),
    }
}

pub(crate) fn parse_bool(value: &str) -> PlanResult<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(PlanError::internal(format!(
            "Invalid boolean value: '{value}'"
        ))),
    }
}

pub(crate) fn parse_non_empty_usize(value: &str) -> PlanResult<Option<usize>> {
    let value: usize = value
        .parse()
        .map_err(|e| PlanError::internal(format!("Invalid usize value: {e}")))?;
    if value.is_zero() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub(crate) fn parse_usize(value: &str) -> PlanResult<usize> {
    value
        .parse()
        .map_err(|e| PlanError::internal(format!("Invalid usize value: {e}")))
}

pub(crate) fn parse_f64(value: &str) -> PlanResult<f64> {
    value
        .parse()
        .map_err(|e| PlanError::internal(format!("Invalid f64 value: {e}")))
}

pub(crate) fn parse_u64(value: &str) -> PlanResult<u64> {
    value
        .parse()
        .map_err(|e| PlanError::internal(format!("Invalid u64 value: {e}")))
}
