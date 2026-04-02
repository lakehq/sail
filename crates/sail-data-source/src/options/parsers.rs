use crate::error::{DataSourceError, DataSourceResult};

pub fn parse_string(_key: &str, value: &str) -> DataSourceResult<String> {
    Ok(value.to_string())
}

pub fn parse_usize(key: &str, value: &str) -> DataSourceResult<usize> {
    value
        .parse::<usize>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        })
}

pub fn parse_bool(key: &str, value: &str) -> DataSourceResult<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        }),
    }
}

pub fn parse_i64(key: &str, value: &str) -> DataSourceResult<i64> {
    value
        .parse::<i64>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        })
}
