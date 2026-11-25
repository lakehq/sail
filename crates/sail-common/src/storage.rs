use std::collections::BTreeMap;
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
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct ScopedStorageCredential {
    pub prefix: String,
    pub s3: S3StorageCredential,
    pub refresh: Option<StorageCredentialSource>,
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
pub struct StorageCredentialSource {
    pub endpoint: String,
    pub authentication: CatalogCredentialSource,
    pub headers: BTreeMap<String, StorageSecret>,
    pub request: StorageCredentialRequest,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum StorageCredentialRequest {
    Iceberg,
    UnityTable {
        table_id: String,
        operation: UnityTableOperation,
    },
    UnityPath {
        url: String,
        operation: UnityPathOperation,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UnityTableOperation {
    Read,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UnityPathOperation {
    PathRead,
    PathCreateTable,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum CatalogCredentialSource {
    None,
    Bearer(StorageSecret),
    OAuth2(OAuth2ClientCredentials),
}

#[derive(Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct OAuth2ClientCredentials {
    pub endpoint: String,
    pub client_id: String,
    pub client_secret: StorageSecret,
    pub scope: Option<String>,
}

impl fmt::Debug for OAuth2ClientCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OAuth2ClientCredentials [REDACTED]")
    }
}

impl fmt::Debug for StorageCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StorageCredentialSource [REDACTED]")
    }
}
