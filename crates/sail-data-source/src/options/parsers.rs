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

pub fn parse_u16(key: &str, value: &str) -> DataSourceResult<u16> {
    value
        .parse::<u16>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        })
}

pub fn parse_u64(key: &str, value: &str) -> DataSourceResult<u64> {
    value
        .parse::<u64>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        })
}

pub fn parse_i64(key: &str, value: &str) -> DataSourceResult<i64> {
    value
        .parse::<i64>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        })
}

pub fn parse_optional_i64(key: &str, value: &str) -> DataSourceResult<Option<i64>> {
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<i64>()
        .map(Some)
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        })
}

pub fn parse_optional_string(_key: &str, value: &str) -> DataSourceResult<Option<String>> {
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(value.to_string()))
}

pub fn parse_non_zero_usize(key: &str, value: &str) -> DataSourceResult<std::num::NonZeroUsize> {
    let n = value
        .parse::<usize>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
        })?;
    std::num::NonZeroUsize::new(n).ok_or_else(|| DataSourceError::InvalidOption {
        key: key.to_string(),
        value: value.to_string(),
    })
}
