use secrecy::SecretString;

use super::AppConfig;
use crate::error::{CommonError, CommonResult};

impl AppConfig {
    /// Returns the current application configuration as a list of `(key, value)` pairs.
    /// Keys use dot notation.
    /// The `AppConfig` instance is serialized to a TOML table and then recursively walked
    /// to produce key-value pairs. Arrays are converted to their TOML string representation.
    pub fn raw(&self) -> CommonResult<Vec<(String, String)>> {
        let table =
            toml::Table::try_from(self).map_err(|e| CommonError::InvalidArgument(e.to_string()))?;
        let mut pairs = Vec::new();
        walk_toml_table(&table, String::new(), &mut pairs);
        Ok(pairs)
    }
}

/// Recursively walks a TOML table, emitting `(dot.notation.key, value_string)` pairs
/// for every primitive (string, integer, float, boolean) and array leaf value.
fn walk_toml_table(table: &toml::Table, prefix: String, pairs: &mut Vec<(String, String)>) {
    for (key, value) in table {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            toml::Value::Table(t) => walk_toml_table(t, full_key, pairs),
            toml::Value::Array(_) => pairs.push((full_key, value.to_string())),
            toml::Value::String(s) => pairs.push((full_key, s.clone())),
            toml::Value::Integer(i) => pairs.push((full_key, i.to_string())),
            toml::Value::Float(f) => pairs.push((full_key, f.to_string())),
            toml::Value::Boolean(b) => pairs.push((full_key, b.to_string())),
            toml::Value::Datetime(d) => pairs.push((full_key, d.to_string())),
        }
    }
}

pub fn serialize_non_zero<T, S>(v: &Option<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: num_traits::Zero + serde::Serialize,
{
    match v {
        Some(x) => x.serialize(s),
        None => T::zero().serialize(s),
    }
}

pub fn serialize_non_empty_string<S>(v: &Option<String>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(v.as_deref().unwrap_or(""))
}

pub fn serialize_optional_secret<S>(v: &Option<SecretString>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // Only called when Some (combined with skip_serializing_if = "Option::is_none")
    let _ = v;
    s.serialize_str("[REDACTED]")
}
