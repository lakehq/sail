use std::collections::HashMap;

use delta_kernel::actions::Protocol;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::models::{CommitInfo, Metadata};
use crate::kernel::{DeltaResult, DeltaTableError};

/// The SaveMode used when performing a DeltaOperation.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum SaveMode {
    Append,
    Overwrite,
    ErrorIfExists,
    Ignore,
}

impl std::str::FromStr for SaveMode {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "append" => Ok(Self::Append),
            "overwrite" => Ok(Self::Overwrite),
            "error" | "error_if_exists" => Ok(Self::ErrorIfExists),
            "ignore" => Ok(Self::Ignore),
            _ => Err(DeltaTableError::Generic(format!(
                "Invalid save mode provided: {s}, only these are supported: ['append', 'overwrite', 'error', 'ignore']"
            ))),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DeltaOperation {
    Create {
        mode: SaveMode,
        location: String,
        protocol: Protocol,
        metadata: Metadata,
    },
    Write {
        mode: SaveMode,
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_by: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        predicate: Option<String>,
    },
    FileSystemCheck {},
    Restore {
        #[serde(skip_serializing_if = "Option::is_none")]
        version: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        datetime: Option<i64>,
    },
}

impl DeltaOperation {
    pub fn name(&self) -> &str {
        match self {
            Self::Create {
                mode: SaveMode::Overwrite,
                ..
            } => "CREATE OR REPLACE TABLE",
            Self::Create { .. } => "CREATE TABLE",
            Self::Write { .. } => "WRITE",
            Self::FileSystemCheck { .. } => "FSCK",
            Self::Restore { .. } => "RESTORE",
        }
    }

    pub fn operation_parameters(&self) -> DeltaResult<HashMap<String, Value>> {
        if let Some(Some(Some(map))) = serde_json::to_value(self)?
            .as_object()
            .map(|p| p.values().next().map(|q| q.as_object()))
        {
            Ok(map
                .iter()
                .filter(|item| !item.1.is_null())
                .map(|(k, v)| {
                    let value = match v.as_str() {
                        Some(text) => Value::String(text.to_string()),
                        None => v.clone(),
                    };
                    (k.to_owned(), value)
                })
                .collect())
        } else {
            Err(DeltaTableError::Generic(
                "Operation parameters serialized into unexpected shape".into(),
            ))
        }
    }

    pub fn changes_data(&self) -> bool {
        !matches!(self, Self::FileSystemCheck {})
    }

    pub fn get_commit_info(&self) -> CommitInfo {
        CommitInfo {
            operation: Some(self.name().into()),
            operation_parameters: self.operation_parameters().ok(),
            engine_info: Some(format!("sail-delta-lake:{}", env!("CARGO_PKG_VERSION"))),
            ..Default::default()
        }
    }

    pub fn read_predicate(&self) -> Option<String> {
        match self {
            Self::Write { predicate, .. } => predicate.clone(),
            _ => None,
        }
    }

    pub fn read_whole_table(&self) -> bool {
        false
    }
}
