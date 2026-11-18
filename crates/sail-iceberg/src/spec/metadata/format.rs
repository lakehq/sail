// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: <https://github.com/apache/iceberg-rust/blob/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/table_metadata.rs> Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership.

/// Format version of Iceberg.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum FormatVersion {
    /// Version 1
    V1 = 1,
    /// Version 2
    #[default]
    V2 = 2,
    /// Version 3
    V3 = 3,
}

impl PartialOrd for FormatVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FormatVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
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
            3 => Ok(FormatVersion::V3),
            _ => Err(serde::de::Error::custom(format!(
                "Invalid format version: {}",
                value
            ))),
        }
    }
}
