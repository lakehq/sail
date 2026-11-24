// SPDX-License-Identifier: Apache-2.0

pub use sail_common_datafusion::catalog::delta::DeltaRatifiedCommit;
use sail_common_datafusion::catalog::delta::{
    DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY, DeltaRatifiedCommits,
    unity_table_id_value,
};
use sail_common_datafusion::catalog::{
    CapabilityFingerprint, CatalogProviderId, CatalogTableIdentity, CommitAuthority,
    LakehouseAuthority, LakehouseExecutionContext, LakehouseFormat, LakehouseOperation,
    LakehouseRuntimeAccess, LakehouseStatusHints, MetadataPointerAuthority, ScanAuthority,
    StorageAccess, TableAccessSessionRef, TableKind, TableLifecycle, TableStatus,
};
use serde::{Deserialize, Serialize};

use crate::provider::{
    CreateTableMetadataRequirement, CreateTableOptions, LakeSourceCreateMetadataMode,
    StagedTableCreate,
};

// TODO: Complete the remaining lakehouse catalog capability work:
// staged create/register/abort state machines; typed Delta, Iceberg REST,
// metadata-location CAS, and provider-native commit recovery; operation-scoped
// credentials and remote signing; REST server-side scan planning; Delta
// ratified-version bounds and protocol profiles; separate classic Glue CAS from
// Glue OpenTableFormat; versioned catalog authority; cross-format virtual
// metadata and governance; and migration shim removal after conformance coverage.
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

#[derive(Debug, Clone)]
pub struct LakehouseCreateRequest {
    pub catalog_table: Vec<String>,
    pub options: CreateTableOptions,
    /// The table as the catalog returned it to the statement that asks for the plan,
    /// when the statement has looked the table up and the table exists. The provider
    /// looks the table up itself when it needs the table and this is `None`.
    pub existing: Option<Box<TableStatus>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LakehouseCreatePlan {
    pub table: LakehouseResolvedTable,
    pub materialization: LakehouseCreateMaterialization,
    /// The state of the table when the catalog stages it before it is created.
    pub staged: Option<Box<StagedTableCreate>>,
    /// Whether the plan replaces an existing table that keeps its entry in the catalog.
    /// The storage metadata of the plan, or the commit of the write, is then all that
    /// replaces the table, and the catalog has no table to create.
    pub replaces_in_place: bool,
}

// TODO: Replace this compatibility materialization shape with protocol-specific
// staged create/register/commit/abort state machines for UC Delta, Iceberg REST,
// Spark CTAS/RTAS staging, and provider-native Glue OpenTableFormat flows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LakehouseCreateMaterialization {
    None,
    BeforeCatalogLakeSource {
        mode: LakeSourceCreateMetadataMode,
        context: Option<Box<LakehouseExecutionContext>>,
    },
    AfterCatalogLakeSource {
        mode: LakeSourceCreateMetadataMode,
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

/// A request for the storage credentials that the catalog issues for a table.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TableStorageCredentialsRequest {
    /// The storage location of the table.
    pub location: String,
    pub access: StorageAccess,
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
// cleanup policy. Keep REST requirements/updates, metadata-location CAS,
// Delta ratification, and provider-native update outcomes distinct.
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

/// What a catalog answers to a staged commit of a catalog-managed Delta table. An error
/// instead of an outcome means that the catalog did not ratify the commit: the catalog
/// refused the request, or the request was not sent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DeltaCommitOutcome {
    /// The catalog ratified the commit.
    Committed,
    /// The version of the commit is taken by another commit.
    Conflict(String),
    /// The catalog accepts no commit until the ratified commits of the table are published
    /// and reported to it.
    BackfillRequired(String),
    /// The catalog throttled the request, which can be sent again.
    Throttled(String),
    /// The catalog did not tell whether it ratified the commit (a server error, a timeout, or
    /// a network error).
    Unknown(String),
}

/// A request for the ratified commits of the table of the context, with the versions from
/// the start version to the end version, or to the latest version of the table.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct DeltaRatifiedCommitRequest {
    pub context: LakehouseExecutionContext,
    pub start_version: i64,
    pub end_version: Option<i64>,
}

pub type DeltaRatifiedCommitResponse = DeltaRatifiedCommits;

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
        CreateTableMetadataRequirement::LakeSource { mode } => Some(mode),
    };
    let authority = match mode {
        Some(LakeSourceCreateMetadataMode::CatalogCoordinated) => {
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
        Some(LakeSourceCreateMetadataMode::PathManaged) => {
            LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::PathManaged,
                context: None,
            }
        }
        Some(LakeSourceCreateMetadataMode::CatalogCoordinated) => {
            LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
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
        staged: options.staged.clone(),
        replaces_in_place: false,
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
            if lifecycle == TableLifecycle::Managed
                && is_unity_delta_table(properties)
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
        ScanAuthority::ClientLakeSource
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
