/// Format version of Iceberg.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FormatVersion {
    /// Version 1
    V1 = 1,
    /// Version 2
    V2 = 2,
}

impl serde::Serialize for FormatVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> serde::Deserialize<'de> for FormatVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        match value {
            1 => Ok(FormatVersion::V1),
            2 => Ok(FormatVersion::V2),
            _ => Err(serde::de::Error::custom(format!(
                "Invalid format version: {}",
                value
            ))),
        }
    }
}

impl Default for FormatVersion {
    fn default() -> Self {
        Self::V2
    }
}
