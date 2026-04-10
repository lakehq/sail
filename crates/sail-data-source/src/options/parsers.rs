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
            cause: None,
        })
}

pub fn parse_bool(key: &str, value: &str) -> DataSourceResult<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        }),
    }
}

pub fn parse_u16(key: &str, value: &str) -> DataSourceResult<u16> {
    value
        .parse::<u16>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        })
}

pub fn parse_u64(key: &str, value: &str) -> DataSourceResult<u64> {
    value
        .parse::<u64>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        })
}

pub fn parse_i64(key: &str, value: &str) -> DataSourceResult<i64> {
    value
        .parse::<i64>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
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
            cause: None,
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
            cause: None,
        })?;
    std::num::NonZeroUsize::new(n).ok_or_else(|| DataSourceError::InvalidOption {
        key: key.to_string(),
        value: value.to_string(),
        cause: None,
    })
}

pub fn parse_f64(key: &str, value: &str) -> DataSourceResult<f64> {
    value
        .parse::<f64>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        })
}

pub fn parse_char(key: &str, value: &str) -> DataSourceResult<char> {
    let mut chars = value.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        }),
    }
}

pub fn parse_optional_char(key: &str, value: &str) -> DataSourceResult<Option<char>> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_char(key, value).map(Some)
}

pub fn parse_optional_usize(key: &str, value: &str) -> DataSourceResult<Option<usize>> {
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<usize>()
        .map(Some)
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        })
}

pub fn parse_optional_non_zero_usize(key: &str, value: &str) -> DataSourceResult<Option<usize>> {
    if value.is_empty() {
        return Ok(None);
    }
    let n = value
        .parse::<usize>()
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        })?;
    Ok(if n == 0 { None } else { Some(n) })
}

pub fn parse_optional_bool(key: &str, value: &str) -> DataSourceResult<Option<bool>> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_bool(key, value).map(Some)
}

pub fn parse_optional_f64(key: &str, value: &str) -> DataSourceResult<Option<f64>> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_f64(key, value).map(Some)
}

pub fn parse_optional_u64(key: &str, value: &str) -> DataSourceResult<Option<u64>> {
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| DataSourceError::InvalidOption {
            key: key.to_string(),
            value: value.to_string(),
            cause: None,
        })
}
