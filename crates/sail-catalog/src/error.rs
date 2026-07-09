use std::fmt;

use datafusion_common::DataFusionError;
use thiserror::Error;

pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogObject {
    Catalog,
    Database,
    Schema,
    Namespace,
    Table,
    View,
    Function,
    TemporaryView,
    LogicalPlan,
}

impl fmt::Display for CatalogObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            CatalogObject::Catalog => "Catalog",
            CatalogObject::Database => "Database",
            CatalogObject::Schema => "Schema",
            CatalogObject::Namespace => "Namespace",
            CatalogObject::Table => "Table",
            CatalogObject::View => "View",
            CatalogObject::Function => "Function",
            CatalogObject::TemporaryView => "Temporary View",
            CatalogObject::LogicalPlan => "Logical Plan",
        };
        write!(f, "{name}")
    }
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0} not found: {1}")]
    NotFound(CatalogObject, String),
    #[error("{0} already exists: {1}")]
    AlreadyExists(CatalogObject, String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("authentication expired: {0}")]
    AuthExpired(String),
    #[error("rate limited: {0}")]
    RateLimited(String),
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("read-only catalog object: {0}")]
    ReadOnly(String),
    #[error("unsupported catalog capability: {0}")]
    UnsupportedCapability(String),
    #[error("stale lakehouse metadata: {0}")]
    StaleMetadata(String),
    #[error("commit state unknown: {0}")]
    CommitStateUnknown(String),
    #[error("credential unavailable: {0}")]
    CredentialUnavailable(String),
    #[error("cross-format metadata conversion lag: {0}")]
    ConversionLag(String),
    #[error("cross-format metadata conversion failed: {0}")]
    ConversionFailed(String),
    #[error("remote catalog protocol error: {0}")]
    RemoteProtocol(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("external error: {0}")]
    External(String),
}
