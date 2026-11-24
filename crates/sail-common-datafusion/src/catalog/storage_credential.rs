// SPDX-License-Identifier: Apache-2.0

use std::fmt;

/// The access level of a storage credential issued by a catalog.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum StorageAccess {
    Read,
    ReadWrite,
}

/// An S3-compatible storage that a catalog names as the storage of a location,
/// instead of AWS S3. The requests to it use path-style addressing.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct S3Endpoint {
    pub url: String,
    /// The region that the requests are signed with.
    pub region: Option<String>,
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
        /// The storage that the credential is for, if it is not AWS S3.
        endpoint: Option<S3Endpoint>,
    },
    /// No credential for an S3-compatible storage, which is accessed with the
    /// credentials of the environment.
    S3Environment {
        endpoint: S3Endpoint,
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
            Self::S3Environment { .. } => "S3Environment",
            Self::AzureSas { .. } => "AzureSas",
            Self::GcsOauth { .. } => "GcsOauth",
        };
        write!(f, "StorageCredential::{kind}(<redacted>)")
    }
}

impl StorageCredential {
    /// The S3-compatible storage that the credential is for, if any.
    pub fn s3_endpoint(&self) -> Option<&S3Endpoint> {
        match self {
            Self::S3 { endpoint, .. } => endpoint.as_ref(),
            Self::S3Environment { endpoint } => Some(endpoint),
            Self::AzureSas { .. } | Self::GcsOauth { .. } => None,
        }
    }

    /// Whether an object store that is created for the other credential can be used
    /// with this credential.
    pub fn is_for_storage_of(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
            && self.s3_endpoint() == other.s3_endpoint()
    }
}

/// The storage credential issued by a catalog for the storage location of a table.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableStorageCredentials {
    pub credential: StorageCredential,
    /// The time when the credential expires, in milliseconds since the Unix epoch.
    pub expires_at_ms: Option<i64>,
    /// The access that the credential is known to give. A credential from a catalog
    /// can give more than the requested access. A credential that a worker gets from
    /// its session has the requested access here, whatever the credential gives.
    pub access: StorageAccess,
}
