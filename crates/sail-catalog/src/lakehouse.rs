use sail_common_datafusion::catalog::{
    CapabilityFingerprint, LakehouseExecutionContext, LakehouseFormat, LakehouseOperation,
    LakehouseRuntimeAccess, LakehouseStatusHints, ScanAuthority, TableAccessSessionRef,
};
use serde::{Deserialize, Serialize};

use crate::provider::{
    CreateTableMetadataRequirement, CreateTableOptions, GetTableCommitsResponse, TableCommitInfo,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum LakehouseCapability {
    TableAccessSessions,
    CredentialVending,
    CatalogManagedCreate,
    CatalogCommit,
    DeltaRatifiedCommits,
    IcebergRestCommit,
    IcebergRestScanPlanning,
    VersionedCatalogReferences,
    CrossFormatMetadata,
    GovernancePolicies,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct ResolveLakehouseTableRequest {
    pub catalog_table: Vec<String>,
    pub operation: LakehouseOperation,
    pub requested_format: Option<LakehouseFormat>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakehouseResolvedTable {
    pub status: LakehouseStatusHints,
    pub execution: LakehouseExecutionContext,
    pub runtime: Option<LakehouseRuntimeAccess>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakehouseCreateRequest {
    pub catalog_table: Vec<String>,
    pub options: CreateTableOptions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LakehouseCreatePlan {
    pub table: LakehouseResolvedTable,
    pub materialization: LakehouseCreateMaterialization,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LakehouseCreateMaterialization {
    None,
    TableFormat {
        requirement: CreateTableMetadataRequirement,
    },
    CatalogNative {
        payload: serde_json::Value,
    },
    ProviderMediated {
        payload: serde_json::Value,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct BeginTableAccessRequest {
    pub context: LakehouseExecutionContext,
    pub purpose: TableAccessPurpose,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum TableAccessPurpose {
    MetadataRead,
    DataRead,
    DataWrite,
    Commit,
    ScanPlanning,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct TableAccessSession {
    pub reference: TableAccessSessionRef,
    pub context: LakehouseExecutionContext,
    pub expires_at_ms: Option<i64>,
    pub credential_scope: Option<String>,
    pub capability_fingerprint: CapabilityFingerprint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LakehouseScanPlanningRequest {
    pub context: LakehouseExecutionContext,
    pub filters: Vec<serde_json::Value>,
    pub projection: Option<Vec<String>>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LakehouseScanPlanningResponse {
    pub authority: ScanAuthority,
    pub files: Option<Vec<serde_json::Value>>,
    pub residual_filter: Option<serde_json::Value>,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LakehouseCommitRequest {
    pub context: LakehouseExecutionContext,
    pub format: String,
    pub requirements: Vec<serde_json::Value>,
    pub updates: Vec<serde_json::Value>,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LakehouseCommitOutcome {
    Committed {
        context: LakehouseExecutionContext,
        payload: Option<serde_json::Value>,
    },
    Noop {
        context: LakehouseExecutionContext,
    },
    RetryableConflict {
        message: String,
    },
    StateUnknown {
        message: String,
    },
    Rejected {
        message: String,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaRatifiedCommitRequest {
    pub context: LakehouseExecutionContext,
    pub table_uri: String,
    pub start_version: i64,
    pub end_version: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaRatifiedCommitResponse {
    pub latest_table_version: i64,
    pub commits: Vec<DeltaRatifiedCommit>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaRatifiedCommit {
    pub version: i64,
    pub timestamp: i64,
    pub file_name: String,
    pub file_size: i64,
    pub file_modification_timestamp: i64,
}

impl From<TableCommitInfo> for DeltaRatifiedCommit {
    fn from(value: TableCommitInfo) -> Self {
        let TableCommitInfo {
            version,
            timestamp,
            file_name,
            file_size,
            file_modification_timestamp,
        } = value;
        Self {
            version,
            timestamp,
            file_name,
            file_size,
            file_modification_timestamp,
        }
    }
}

impl From<GetTableCommitsResponse> for DeltaRatifiedCommitResponse {
    fn from(value: GetTableCommitsResponse) -> Self {
        let GetTableCommitsResponse {
            latest_table_version,
            commits,
        } = value;
        Self {
            latest_table_version,
            commits: commits.into_iter().map(Into::into).collect(),
        }
    }
}
