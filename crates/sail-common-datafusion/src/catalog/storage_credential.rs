// SPDX-License-Identifier: Apache-2.0

use std::fmt;

/// The access level of a storage credential issued by a catalog.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum StorageAccess {
    Read,
    ReadWrite,
}

/// A temporary storage credential issued by a catalog.
///
/// The credential is not serializable and its secret values are not shown
/// when it is formatted, so that it does not end up in plans, logs, or errors.
#[derive(Clone, Eq, PartialEq)]
pub enum StorageCredential {
    S3 {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    AzureSas {
        token: String,
    },
    GcsOauth {
        token: String,
    },
}

impl fmt::Debug for StorageCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::S3 { .. } => "S3",
            Self::AzureSas { .. } => "AzureSas",
            Self::GcsOauth { .. } => "GcsOauth",
        };
        write!(f, "StorageCredential::{kind}(<redacted>)")
    }
}

/// The storage credential issued by a catalog for the storage location of a table.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableStorageCredentials {
    pub credential: StorageCredential,
    /// The time when the credential expires, in milliseconds since the Unix epoch.
    pub expires_at_ms: Option<i64>,
    /// The access that the credential gives, which can be more than the requested access.
    pub access: StorageAccess,
}
