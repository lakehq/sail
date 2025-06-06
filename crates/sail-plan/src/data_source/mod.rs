use crate::error::{PlanError, PlanResult};
use serde::Deserialize;
use std::collections::HashMap;

pub mod csv;

const fn default_true() -> bool {
    true
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
    fn resolve_value(&self, user_options: &HashMap<String, String>) -> String {
        if let Some(value) = user_options.get(&self.key) {
            return value.clone();
        }
        for alias in &self.alias {
            if let Some(value) = user_options.get(alias) {
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

pub(crate) fn parse_bool(value: &str) -> PlanResult<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(PlanError::internal(format!(
            "Invalid boolean value: '{value}'"
        ))),
    }
}
