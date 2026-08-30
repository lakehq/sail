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

    pub fn as_str(&self) -> &str {
        match self {
            Self::Delta => "delta",
            Self::Iceberg => "iceberg",
            Self::Other(format) => format,
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
    ClientLakeSource,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_planning_mode: Option<String>,
    #[serde(default)]
    pub storage_credential_count: usize,
    #[serde(default)]
    pub remote_signing_enabled: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct CapabilityFingerprint(pub String);

// TODO: Wire versioned catalog providers into this context, including ref/hash
// mutability and expected-head commit preconditions.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct VersionedCatalogContext {
    pub ref_name: String,
    pub resolved_hash: Option<String>,
    pub mutable: bool,
    pub content_key: Vec<String>,
    pub content_id: Option<String>,
    pub expected_hash: Option<String>,
}

// TODO: Populate this for OneLake/Fabric and UniForm-style virtual projections,
// then enforce freshness, conversion failure, limitations, and explicit
// read-only or write-through policy in planning.
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

// TODO: Map provider authorization and governance decisions into operation-scoped
// planning checks before create, alter, maintenance, commit, or
// catalog-authoritative storage mutation is allowed.
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

/// Stable catalog/table binding captured in a logical plan.
///
/// Operation-scoped access and REST sessions are intentionally excluded and
/// must be reacquired immediately before execution.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakehouseTableBinding {
    pub catalog_provider_id: CatalogProviderId,
    pub catalog_table: Vec<String>,
    pub table_identity: CatalogTableIdentity,
    pub operation: LakehouseOperation,
    pub format: LakehouseFormat,
    pub authority: LakehouseAuthority,
    pub pointer: MetadataPointerAuthority,
    pub commit: CommitAuthority,
    pub scan: ScanAuthority,
    pub versioned_catalog: Option<VersionedCatalogContext>,
    pub cross_format: Option<CrossFormatMetadata>,
    pub governance: Option<GovernanceContext>,
    pub capability_fingerprint: CapabilityFingerprint,
}

impl LakehouseTableBinding {
    pub fn from_execution(context: &LakehouseExecutionContext) -> Self {
        Self {
            catalog_provider_id: context.catalog_provider_id.clone(),
            catalog_table: context.catalog_table.clone(),
            table_identity: context.table_identity.clone(),
            operation: context.operation,
            format: context.format.clone(),
            authority: context.authority.clone(),
            pointer: context.pointer,
            commit: context.commit,
            scan: context.scan,
            versioned_catalog: context.versioned_catalog.clone(),
            cross_format: context.cross_format.clone(),
            governance: context.governance.clone(),
            capability_fingerprint: context.capability_fingerprint.clone(),
        }
    }

    pub fn matches_execution(&self, context: &LakehouseExecutionContext) -> bool {
        self == &Self::from_execution(context)
    }
}

impl LakehouseExecutionContext {
    pub fn catalog_table_context(
        catalog_provider_id: CatalogProviderId,
        catalog_table: Vec<String>,
        table_identity: CatalogTableIdentity,
        operation: LakehouseOperation,
        format: LakehouseFormat,
        authority: LakehouseAuthority,
        scan: ScanAuthority,
    ) -> Self {
        let (pointer, commit) = match &authority {
            LakehouseAuthority::PathManaged => (
                MetadataPointerAuthority::StorageDiscovery,
                CommitAuthority::Filesystem,
            ),
            LakehouseAuthority::CatalogRegistered {
                pointer, commit, ..
            }
            | LakehouseAuthority::CatalogAuthoritative {
                pointer, commit, ..
            } => (*pointer, *commit),
            LakehouseAuthority::ReadOnlyVirtualized { .. } => (
                MetadataPointerAuthority::ReadOnlyVirtual,
                CommitAuthority::ReadOnly,
            ),
        };
        let fingerprint = format!(
            "{}:{}:{format:?}:{authority:?}",
            catalog_provider_id.0,
            catalog_table.join(".")
        );
        Self {
            catalog_provider_id,
            catalog_table,
            table_identity,
            operation,
            format,
            authority,
            pointer,
            commit,
            scan,
            access_session: None,
            rest_session: None,
            versioned_catalog: None,
            cross_format: None,
            governance: None,
            capability_fingerprint: CapabilityFingerprint(fingerprint),
        }
    }

    pub fn catalog_table(&self) -> &[String] {
        &self.catalog_table
    }

    pub fn for_operation(&self, operation: LakehouseOperation) -> Self {
        let mut context = self.clone();
        context.operation = operation;
        context
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn stable_binding_excludes_operation_scoped_sessions() {
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("catalog".to_string()),
            vec![
                "catalog".to_string(),
                "default".to_string(),
                "items".to_string(),
            ],
            CatalogTableIdentity {
                table_id: Some("items-id".to_string()),
                table_uri: Some("s3://bucket/items".to_string()),
            },
            LakehouseOperation::Maintenance,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::ClientLakeSource,
        );
        let binding = LakehouseTableBinding::from_execution(&context);

        context.access_session = Some(TableAccessSessionRef {
            fingerprint: "access".to_string(),
        });
        context.rest_session = Some(IcebergRestTableSessionRef {
            fingerprint: "rest".to_string(),
            scan_planning_mode: Some("client".to_string()),
            storage_credential_count: 1,
            remote_signing_enabled: false,
        });
        assert!(binding.matches_execution(&context));

        context.capability_fingerprint = CapabilityFingerprint("changed".to_string());
        assert!(!binding.matches_execution(&context));
    }

    #[test]
    fn iceberg_rest_session_ref_decodes_legacy_fingerprint_only_json() {
        let session: IcebergRestTableSessionRef =
            serde_json::from_str(r#"{"fingerprint":"rest-session"}"#).unwrap();

        assert_eq!(session.fingerprint, "rest-session");
        assert_eq!(session.scan_planning_mode, None);
        assert_eq!(session.storage_credential_count, 0);
        assert!(!session.remote_signing_enabled);
    }
}
