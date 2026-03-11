use serde::de::Error;
pub(crate) use serde::Deserialize;

pub fn deserialize_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Ok(Some(value))
}

pub fn deserialize_non_empty_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub fn deserialize_char<'de, D>(deserializer: D) -> Result<Option<char>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.chars().count() {
        0 => Err(Error::custom("missing character value")),
        1 => Ok(value.chars().next()),
        _ => Err(Error::custom(format!(
            "expected a single character, but got: {value}"
        ))),
    }
}

pub fn deserialize_optional_char<'de, D>(deserializer: D) -> Result<Option<Option<char>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.chars().count() {
        0 => Ok(Some(None)),
        1 => Ok(Some(value.chars().next())),
        _ => Err(Error::custom(format!(
            "expected zero or one character, but got: {value}"
        ))),
    }
}

pub fn deserialize_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.to_lowercase().as_str() {
        "true" | "1" => Ok(Some(true)),
        "false" | "0" => Ok(Some(false)),
        _ => Err(Error::custom(format!("invalid boolean value: {value}"))),
    }
}

pub fn deserialize_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let value = value
        .parse()
        .map_err(|e| Error::custom(format!("invalid usize value: {e}")))?;
    Ok(Some(value))
}

pub fn deserialize_non_zero_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Ok(None);
    }
    let value = value
        .parse()
        .map_err(|e| Error::custom(format!("invalid usize value: {e}")))?;
    if value == 0 {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub fn deserialize_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let value = value
        .parse()
        .map_err(|e| Error::custom(format!("invalid f64 value: {e}")))?;
    Ok(Some(value))
}

pub fn deserialize_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let value = value
        .parse()
        .map_err(|e| Error::custom(format!("invalid u64 value: {e}")))?;
    Ok(Some(value))
}

pub fn deserialize_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let value = value
        .parse()
        .map_err(|e| Error::custom(format!("invalid i64 value: {e}")))?;
    Ok(Some(value))
}

pub fn deserialize_u16<'de, D>(deserializer: D) -> Result<Option<u16>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let value = value
        .parse()
        .map_err(|e| Error::custom(format!("invalid u16 value: {e}")))?;
    Ok(Some(value))
}
