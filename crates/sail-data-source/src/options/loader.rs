use std::collections::HashMap;

use datafusion_common::{plan_datafusion_err, Result};
use serde::de::value::MapDeserializer;

use crate::options::DataSourceOptions;

pub fn load_options<T: DataSourceOptions>(options: HashMap<String, String>) -> Result<T> {
    // Here we load options while ignoring unknown keys.
    // This is to avoid deserializing unknown external table options for table read/write.
    let options = options
        .into_iter()
        .map(|(k, v)| (k.to_lowercase(), v))
        .filter(|(k, _)| T::ALLOWED_KEYS.contains(&k.as_str()));
    T::deserialize(<MapDeserializer<'_, _, serde::de::value::Error>>::new(
        options,
    ))
    .map_err(|e| plan_datafusion_err!("{}", e.to_string()))
}

pub fn load_default_options<T: DataSourceOptions>() -> Result<T> {
    let options = T::DEFAULT_VALUES
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    load_options(options)
}

#[cfg(test)]
pub fn build_options(options: &[(&str, &str)]) -> HashMap<String, String> {
    options
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
