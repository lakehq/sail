use std::collections::HashMap;

use serde::Deserialize;

use crate::error::{PlanError, PlanResult};

pub mod csv;
pub mod json;

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
        .filter(|item| item.supported)
        .map(|item| {
            let value = item.resolve_value(&user_options_normalized);
            let key = item.key;
            (key, value)
        })
        .collect();
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
    fn resolve_value(&self, user_options_normalized: &HashMap<String, String>) -> String {
        // TODO: If both the key and its alias are present, this duplication is silently ignored,
        //  and the user cannot tell which one is chosen without looking at the code.
        if let Some(value) = user_options_normalized.get(&self.key.to_lowercase()) {
            return value.clone();
        }
        for alias in &self.alias {
            if let Some(value) = user_options_normalized.get(&alias.to_lowercase()) {
                return value.clone();
            }
        }
        self.default.clone()
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
