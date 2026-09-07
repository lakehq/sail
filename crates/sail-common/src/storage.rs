use std::fmt;

use serde::{Deserialize, Serialize};

/// Secret material for trusted internal execution-plan transport.
#[derive(Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StorageSecret(String);

impl StorageSecret {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StorageSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct StorageAccessSpec {
    pub credentials: Vec<ScopedStorageCredential>,
    pub refresh: Option<IcebergCredentialSource>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct ScopedStorageCredential {
    pub prefix: String,
    pub s3: S3StorageCredential,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct S3StorageCredential {
    pub connection: S3StorageConnection,
    pub access_key_id: StorageSecret,
    pub secret_access_key: StorageSecret,
    pub session_token: Option<StorageSecret>,
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct S3StorageConnection {
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style_access: bool,
    pub sse_type: Option<String>,
    pub sse_key: Option<String>,
}

#[derive(Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct IcebergCredentialSource {
    pub endpoint: String,
    pub bearer_token: Option<StorageSecret>,
}

impl fmt::Debug for IcebergCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IcebergCredentialSource [REDACTED]")
    }
}
