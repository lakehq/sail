use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogProviderId(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CatalogTableIdentity {
    pub table_id: Option<String>,
    pub table_uri: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum LakehouseFormat {
    Delta,
    Iceberg,
    Other(String),
}

impl LakehouseFormat {
    pub fn from_format_name(format: &str) -> Self {
        if format.eq_ignore_ascii_case("delta") {
            Self::Delta
        } else if format.eq_ignore_ascii_case("iceberg") {
            Self::Iceberg
        } else {
            Self::Other(format.to_string())
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub enum TableLifecycle {
    Managed,
    External,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub enum LakehouseOperation {
    #[default]
    Read,
    Write,
    Create,
    Register,
    Alter,
    Maintenance,
    WritePrecondition,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub enum MetadataPointerAuthority {
    #[default]
    StorageDiscovery,
    CatalogPropertyCas,
    IcebergRest,
    DeltaRatifiedCommits,
    VersionedCatalog,
    ReadOnlyVirtual,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub enum CommitAuthority {
    #[default]
    Filesystem,
    IcebergMetadataLocationCas,
    IcebergRestCommit,
    VersionedCatalogCommit,
    DeltaRatifiedCommit,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub enum ScanAuthority {
    #[default]
    ClientTableFormat,
    IcebergRestServerSide,
    ProviderNative,
    ReadOnlyVirtual,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum CrossFormatWritePolicy {
    ReadOnly,
    NativeWriteThrough,
    ProviderMediatedWrite,
    Unsupported,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize, Default)]
pub enum LakehouseAuthority {
    #[default]
    PathManaged,
    CatalogRegistered {
        lifecycle: TableLifecycle,
        pointer: MetadataPointerAuthority,
        commit: CommitAuthority,
    },
    CatalogAuthoritative {
        lifecycle: TableLifecycle,
        pointer: MetadataPointerAuthority,
        commit: CommitAuthority,
    },
    ReadOnlyVirtualized {
        source_format: LakehouseFormat,
        exposed_format: LakehouseFormat,
        write_policy: CrossFormatWritePolicy,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakehouseStatusHints {
    pub lifecycle: TableLifecycle,
    pub format: LakehouseFormat,
    pub location: Option<String>,
    pub display_properties: Vec<(String, String)>,
    pub cross_format_summary: Option<CrossFormatStatus>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CrossFormatStatus {
    pub source_format: LakehouseFormat,
    pub exposed_format: LakehouseFormat,
    pub freshness: MappingFreshness,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum MappingFreshness {
    Current,
    Stale,
    Unknown,
    Failed,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct TableAccessSessionRef {
    pub fingerprint: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct IcebergRestTableSessionRef {
    pub fingerprint: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CapabilityFingerprint(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct VersionedCatalogContext {
    pub ref_name: String,
    pub resolved_hash: Option<String>,
    pub mutable: bool,
    pub content_key: Vec<String>,
    pub content_id: Option<String>,
    pub expected_hash: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CrossFormatMetadata {
    pub source_format: LakehouseFormat,
    pub exposed_format: LakehouseFormat,
    pub generated_metadata_location: Option<String>,
    pub source_version: Option<i64>,
    pub converted_source_version: Option<i64>,
    pub converted_at_ms: Option<i64>,
    pub conversion_log_location: Option<String>,
    pub freshness: MappingFreshness,
    pub limitations: Vec<CrossFormatLimitation>,
    pub write_policy: CrossFormatWritePolicy,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CrossFormatLimitation {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct GovernanceContext {
    pub metadata_access: AccessDecision,
    pub data_read: AccessDecision,
    pub data_write: AccessDecision,
    pub table_create: AccessDecision,
    pub table_drop: AccessDecision,
    pub location_policy: LocationPolicyDecision,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum AccessDecision {
    Allowed,
    Denied { reason: String },
    Unknown,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum LocationPolicyDecision {
    Allowed,
    Denied { reason: String },
    Unknown,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakehouseExecutionContext {
    pub catalog_provider_id: CatalogProviderId,
    pub catalog_table: Vec<String>,
    pub table_identity: CatalogTableIdentity,
    pub operation: LakehouseOperation,
    pub format: LakehouseFormat,
    pub authority: LakehouseAuthority,
    pub pointer: MetadataPointerAuthority,
    pub commit: CommitAuthority,
    pub scan: ScanAuthority,
    pub access_session: Option<TableAccessSessionRef>,
    pub rest_session: Option<IcebergRestTableSessionRef>,
    pub versioned_catalog: Option<VersionedCatalogContext>,
    pub cross_format: Option<CrossFormatMetadata>,
    pub governance: Option<GovernanceContext>,
    pub capability_fingerprint: CapabilityFingerprint,
}

impl LakehouseExecutionContext {
    pub fn legacy_catalog_table(catalog_table: Vec<String>, operation: LakehouseOperation) -> Self {
        let fingerprint = catalog_table.join(".");
        Self {
            catalog_provider_id: CatalogProviderId(
                catalog_table.first().cloned().unwrap_or_default(),
            ),
            catalog_table,
            table_identity: CatalogTableIdentity {
                table_id: None,
                table_uri: None,
            },
            operation,
            format: LakehouseFormat::Other("unknown".to_string()),
            authority: LakehouseAuthority::CatalogRegistered {
                lifecycle: TableLifecycle::Unknown,
                pointer: MetadataPointerAuthority::StorageDiscovery,
                commit: CommitAuthority::Filesystem,
            },
            pointer: MetadataPointerAuthority::StorageDiscovery,
            commit: CommitAuthority::Filesystem,
            scan: ScanAuthority::ClientTableFormat,
            access_session: None,
            rest_session: None,
            versioned_catalog: None,
            cross_format: None,
            governance: None,
            capability_fingerprint: CapabilityFingerprint(format!("legacy:{fingerprint}")),
        }
    }

    pub fn catalog_table(&self) -> &[String] {
        &self.catalog_table
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakehouseRuntimeAccess {
    pub provider_id: CatalogProviderId,
    pub capability_key: CapabilityKey,
    pub access_session_key: Option<AccessSessionKey>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CapabilityKey(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct AccessSessionKey(pub String);
