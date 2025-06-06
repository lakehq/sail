use serde::Deserialize;

pub mod csv;

pub(crate) fn deserialize_data_source_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?.to_lowercase();
    match value.as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(serde::de::Error::custom(format!(
            "Invalid boolean value: {value}"
        ))),
    }
}

pub(crate) fn deserialize_data_source_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value
        .parse::<usize>()
        .map_err(|_| serde::de::Error::custom(format!("Invalid usize value: {value}")))
}
