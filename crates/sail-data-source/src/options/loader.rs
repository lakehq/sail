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

/// Merge multiple option layers into a single HashMap.
/// Later options override earlier ones.
pub fn merge_options(options: Vec<HashMap<String, String>>) -> HashMap<String, String> {
    let mut merged = HashMap::new();
    for layer in options {
        merged.extend(layer);
    }
    merged
}

#[cfg(test)]
pub fn build_options(options: &[(&str, &str)]) -> HashMap<String, String> {
    options
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_options_empty() {
        let result = merge_options(vec![]);
        assert_eq!(result, HashMap::new());
    }

    #[test]
    fn test_merge_options_single_layer() {
        let options = vec![HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ])];
        let result = merge_options(options);
        assert_eq!(result.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_merge_options_override() {
        let options = vec![
            HashMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ]),
            HashMap::from([
                ("key2".to_string(), "overridden".to_string()),
                ("key3".to_string(), "value3".to_string()),
            ]),
        ];
        let result = merge_options(options);
        assert_eq!(result.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result.get("key2"), Some(&"overridden".to_string()));
        assert_eq!(result.get("key3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_merge_options_multiple_layers() {
        let options = vec![
            HashMap::from([("a".to_string(), "1".to_string())]),
            HashMap::from([("b".to_string(), "2".to_string())]),
            HashMap::from([("a".to_string(), "3".to_string())]), // Override first layer
        ];
        let result = merge_options(options);
        assert_eq!(result.get("a"), Some(&"3".to_string()));
        assert_eq!(result.get("b"), Some(&"2".to_string()));
    }
}
