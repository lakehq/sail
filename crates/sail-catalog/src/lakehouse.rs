use sail_common_datafusion::catalog::delta::{
    unity_table_id_value, DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY,
};
use sail_common_datafusion::catalog::{
    CapabilityFingerprint, CatalogProviderId, CatalogTableIdentity, CommitAuthority,
    LakehouseAuthority, LakehouseExecutionContext, LakehouseFormat, LakehouseOperation,
    LakehouseRuntimeAccess, LakehouseStatusHints, MetadataPointerAuthority, ScanAuthority,
    TableAccessSessionRef, TableKind, TableLifecycle, TableStatus,
};
use serde::{Deserialize, Serialize};

use crate::provider::{
    CreateTableMetadataRequirement, CreateTableOptions, TableFormatCreateMetadataMode,
};

// TODO: Complete the remaining lakehouse catalog capability work:
// typed create/register state machines; typed commit recovery and cleanup
// policy; operation-scoped credentials; REST server-side scan planning; Delta
// inline ratified commits and protocol profiles; Iceberg HMS/Glue/Nessie
// authority state machines; cross-format metadata and governance; and migration
// shim removal after conformance coverage.
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

// TODO: Replace this compatibility materialization shape with protocol-specific
// create/register state machines for UC Delta and Iceberg REST staged/register flows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LakehouseCreateMaterialization {
    None,
    BeforeCatalogTableFormat {
        mode: TableFormatCreateMetadataMode,
        context: Option<Box<LakehouseExecutionContext>>,
    },
    AfterCatalogTableFormat {
        mode: TableFormatCreateMetadataMode,
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

// TODO: Expand this with typed success payloads, retry/reconcile hints, and
// cleanup policy before wiring providers that can return commit-state-unknown
// after side effects.
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

pub fn resolve_lakehouse_table_status(
    provider_id: impl Into<String>,
    catalog_table: Vec<String>,
    status: &TableStatus,
    operation: LakehouseOperation,
    capabilities: &[LakehouseCapability],
) -> LakehouseResolvedTable {
    let provider_id = CatalogProviderId(provider_id.into());
    let (format, lifecycle, location, properties) = match &status.kind {
        TableKind::Table {
            format,
            location,
            properties,
            is_external,
            ..
        } => (
            LakehouseFormat::from_format_name(format),
            table_lifecycle(*is_external),
            location.clone(),
            properties.clone(),
        ),
        _ => (
            LakehouseFormat::Other("view".to_string()),
            TableLifecycle::Unknown,
            None,
            Vec::new(),
        ),
    };
    let authority = resolved_lakehouse_authority(&format, lifecycle, &properties, capabilities);
    let scan = resolved_scan_authority(&authority, capabilities);
    let table_identity = CatalogTableIdentity {
        table_id: resolved_table_id(&format, &properties),
        table_uri: location.clone(),
    };
    let execution = LakehouseExecutionContext::catalog_table_context(
        provider_id,
        catalog_table,
        table_identity,
        operation,
        format.clone(),
        authority,
        scan,
    );
    LakehouseResolvedTable {
        status: LakehouseStatusHints {
            lifecycle,
            format,
            location,
            display_properties: properties,
            cross_format_summary: None,
        },
        execution,
        runtime: None,
    }
}

pub fn plan_lakehouse_create_from_requirement(
    provider_id: impl Into<String>,
    catalog_table: Vec<String>,
    options: &CreateTableOptions,
    requirement: CreateTableMetadataRequirement,
    capabilities: &[LakehouseCapability],
) -> LakehouseCreatePlan {
    let provider_id = CatalogProviderId(provider_id.into());
    let format = LakehouseFormat::from_format_name(&options.format);
    let lifecycle = table_lifecycle(options.is_external);
    let mode = match requirement {
        CreateTableMetadataRequirement::None => None,
        CreateTableMetadataRequirement::TableFormat { mode } => Some(mode),
    };
    let authority = match mode {
        Some(TableFormatCreateMetadataMode::CatalogCoordinated) => {
            catalog_coordinated_create_authority(&format, lifecycle, capabilities)
        }
        None if matches!(format, LakehouseFormat::Iceberg)
            && capabilities.contains(&LakehouseCapability::IcebergRestCommit) =>
        {
            catalog_coordinated_create_authority(&format, lifecycle, capabilities)
        }
        None if matches!(format, LakehouseFormat::Delta)
            && !options.is_external
            && (capabilities.contains(&LakehouseCapability::CatalogCommit)
                || capabilities.contains(&LakehouseCapability::DeltaRatifiedCommits)) =>
        {
            catalog_coordinated_create_authority(&format, lifecycle, capabilities)
        }
        _ => LakehouseAuthority::PathManaged,
    };
    let scan = resolved_scan_authority(&authority, capabilities);
    let execution = LakehouseExecutionContext::catalog_table_context(
        provider_id,
        catalog_table,
        CatalogTableIdentity {
            table_id: None,
            table_uri: options.location.clone(),
        },
        LakehouseOperation::Create,
        format.clone(),
        authority,
        scan,
    );
    let materialization = match mode {
        None => LakehouseCreateMaterialization::None,
        Some(TableFormatCreateMetadataMode::PathManaged) => {
            LakehouseCreateMaterialization::BeforeCatalogTableFormat {
                mode: TableFormatCreateMetadataMode::PathManaged,
                context: None,
            }
        }
        Some(TableFormatCreateMetadataMode::CatalogCoordinated) => {
            LakehouseCreateMaterialization::BeforeCatalogTableFormat {
                mode: TableFormatCreateMetadataMode::CatalogCoordinated,
                context: Some(Box::new(execution.clone())),
            }
        }
    };

    LakehouseCreatePlan {
        table: LakehouseResolvedTable {
            status: LakehouseStatusHints {
                lifecycle,
                format,
                location: options.location.clone(),
                display_properties: options.properties.clone(),
                cross_format_summary: None,
            },
            execution,
            runtime: None,
        },
        materialization,
    }
}

pub fn catalog_coordinated_create_authority(
    format: &LakehouseFormat,
    lifecycle: TableLifecycle,
    capabilities: &[LakehouseCapability],
) -> LakehouseAuthority {
    match format {
        LakehouseFormat::Delta
            if capabilities.contains(&LakehouseCapability::CatalogCommit)
                || capabilities.contains(&LakehouseCapability::DeltaRatifiedCommits) =>
        {
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle,
                pointer: MetadataPointerAuthority::DeltaRatifiedCommits,
                commit: CommitAuthority::DeltaRatifiedCommit,
            }
        }
        LakehouseFormat::Iceberg
            if capabilities.contains(&LakehouseCapability::IcebergRestCommit) =>
        {
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            }
        }
        LakehouseFormat::Iceberg => LakehouseAuthority::CatalogAuthoritative {
            lifecycle,
            pointer: MetadataPointerAuthority::CatalogPropertyCas,
            commit: CommitAuthority::IcebergMetadataLocationCas,
        },
        _ => LakehouseAuthority::CatalogRegistered {
            lifecycle,
            pointer: MetadataPointerAuthority::StorageDiscovery,
            commit: CommitAuthority::Filesystem,
        },
    }
}

fn resolved_lakehouse_authority(
    format: &LakehouseFormat,
    lifecycle: TableLifecycle,
    properties: &[(String, String)],
    capabilities: &[LakehouseCapability],
) -> LakehouseAuthority {
    match format {
        LakehouseFormat::Delta
            if is_unity_delta_table(properties)
                && (capabilities.contains(&LakehouseCapability::CatalogCommit)
                    || capabilities.contains(&LakehouseCapability::DeltaRatifiedCommits)) =>
        {
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle,
                pointer: MetadataPointerAuthority::DeltaRatifiedCommits,
                commit: CommitAuthority::DeltaRatifiedCommit,
            }
        }
        LakehouseFormat::Iceberg
            if capabilities.contains(&LakehouseCapability::IcebergRestCommit) =>
        {
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            }
        }
        LakehouseFormat::Iceberg if has_iceberg_catalog_marker(properties) => {
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle,
                pointer: MetadataPointerAuthority::CatalogPropertyCas,
                commit: CommitAuthority::IcebergMetadataLocationCas,
            }
        }
        _ => LakehouseAuthority::CatalogRegistered {
            lifecycle,
            pointer: MetadataPointerAuthority::StorageDiscovery,
            commit: CommitAuthority::Filesystem,
        },
    }
}

fn resolved_scan_authority(
    authority: &LakehouseAuthority,
    capabilities: &[LakehouseCapability],
) -> ScanAuthority {
    if capabilities.contains(&LakehouseCapability::IcebergRestScanPlanning) {
        ScanAuthority::IcebergRestServerSide
    } else if matches!(authority, LakehouseAuthority::ReadOnlyVirtualized { .. }) {
        ScanAuthority::ReadOnlyVirtual
    } else {
        ScanAuthority::ClientTableFormat
    }
}

fn table_lifecycle(is_external: bool) -> TableLifecycle {
    if is_external {
        TableLifecycle::External
    } else {
        TableLifecycle::Managed
    }
}

fn property_value<'a>(properties: &'a [(String, String)], key: &str) -> Option<&'a str> {
    properties
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

fn resolved_table_id(format: &LakehouseFormat, properties: &[(String, String)]) -> Option<String> {
    match format {
        LakehouseFormat::Delta => unity_table_id_value(
            properties
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .map(ToString::to_string),
        LakehouseFormat::Iceberg => property_value(properties, "metadata.table-uuid")
            .or_else(|| property_value(properties, "table-uuid"))
            .map(ToString::to_string),
        _ => None,
    }
}

fn is_unity_delta_table(properties: &[(String, String)]) -> bool {
    property_value(properties, DELTA_UNITY_TABLE_ID_KEY).is_some()
        || property_value(properties, DELTA_UNITY_TABLE_ID_LEGACY_KEY).is_some()
}

fn has_iceberg_catalog_marker(properties: &[(String, String)]) -> bool {
    properties.iter().any(|(key, value)| {
        let key = key.trim();
        (key.eq_ignore_ascii_case("table_type") && value.eq_ignore_ascii_case("ICEBERG"))
            || key.starts_with("metadata.")
    })
}
