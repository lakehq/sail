use datafusion_common::{DataFusionError, Result};
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::{LakehouseCommitOutcome, LakehouseCommitRequest};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::AlterTableOptions;
use sail_common_datafusion::catalog::managed::{
    METADATA_LOCATION_UNDERSCORE_KEY, PREVIOUS_METADATA_LOCATION_KEY,
    existing_metadata_location_key,
};
use sail_common_datafusion::catalog::{
    CommitAuthority, LakehouseExecutionContext, TableKind, TableStatus,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use url::Url;

use crate::spec::catalog::TableUpdate;
use crate::spec::{TableMetadata, TableRequirement};
use crate::table_format::{
    catalog_managed_iceberg_from_properties, metadata_location_from_properties,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum CatalogCommitOutcome {
    Committed(CatalogCommittedTable),
    NotSupported,
    Conflict,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct CatalogCommittedTable {
    metadata_location: Option<String>,
    payload: Option<serde_json::Value>,
}

impl CatalogCommittedTable {
    fn from_payload(payload: Option<serde_json::Value>) -> Self {
        let metadata_location = payload
            .as_ref()
            .and_then(|payload| payload.get("metadata-location"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);
        Self {
            metadata_location,
            payload,
        }
    }

    pub(crate) fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    pub(crate) fn payload(&self) -> Option<&serde_json::Value> {
        self.payload.as_ref()
    }
}

#[derive(Debug, Default)]
pub(crate) struct CatalogTableInfo {
    pub(crate) metadata_location: Option<String>,
    pub(crate) is_catalog_managed_iceberg_table: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum IcebergCatalogCommitMode {
    Filesystem,
    MetadataLocationCas,
    CatalogCommit,
    CompatibilityCatalogCommit,
}

impl IcebergCatalogCommitMode {
    pub(crate) fn resolve(
        context: Option<&LakehouseExecutionContext>,
        catalog_table_info: &CatalogTableInfo,
        table_properties: &[(String, String)],
    ) -> Self {
        if let Some(context) = context {
            if matches!(context.commit, CommitAuthority::Filesystem)
                && catalog_table_info.is_catalog_managed_iceberg_table
            {
                return Self::MetadataLocationCas;
            }
            return match context.commit {
                CommitAuthority::IcebergMetadataLocationCas => Self::MetadataLocationCas,
                CommitAuthority::IcebergRestCommit | CommitAuthority::VersionedCatalogCommit => {
                    Self::CatalogCommit
                }
                CommitAuthority::Filesystem
                | CommitAuthority::DeltaRatifiedCommit
                | CommitAuthority::ReadOnly => Self::Filesystem,
            };
        }

        if catalog_table_info.is_catalog_managed_iceberg_table
            || catalog_managed_iceberg_from_properties(table_properties)
        {
            Self::CompatibilityCatalogCommit
        } else {
            Self::Filesystem
        }
    }

    pub(crate) fn uses_catalog_metadata(self) -> bool {
        !matches!(self, Self::Filesystem)
    }

    pub(crate) fn uses_catalog_commit(self) -> bool {
        matches!(self, Self::CatalogCommit | Self::CompatibilityCatalogCommit)
    }

    pub(crate) fn uses_metadata_location_update(self) -> bool {
        matches!(
            self,
            Self::MetadataLocationCas | Self::CompatibilityCatalogCommit
        )
    }
}

pub(crate) struct IcebergCatalogCommitCoordinator<'a, C: SessionExtensionAccessor + ?Sized> {
    context: &'a C,
    catalog_table: &'a [String],
}

impl<'a, C: SessionExtensionAccessor + ?Sized> IcebergCatalogCommitCoordinator<'a, C> {
    pub(crate) fn new(context: &'a C, catalog_table: &'a [String]) -> Self {
        Self {
            context,
            catalog_table,
        }
    }

    pub(crate) async fn load_table_info(
        context: &C,
        catalog_table: &[String],
    ) -> Result<CatalogTableInfo> {
        let manager = match context.extension::<CatalogManager>() {
            Ok(manager) => manager,
            Err(err) => {
                log::debug!(
                    "Catalog manager unavailable while resolving Iceberg metadata location: {err}"
                );
                return Ok(CatalogTableInfo::default());
            }
        };
        match manager.get_table(catalog_table).await {
            Ok(status) => Ok(catalog_table_info_from_status(&status)),
            Err(CatalogError::NotFound(_, _)) | Err(CatalogError::NotSupported(_)) => {
                Ok(CatalogTableInfo::default())
            }
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }

    pub(crate) async fn load_metadata_location(
        context: &C,
        catalog_table: &[String],
    ) -> Result<Option<String>> {
        Ok(Self::load_table_info(context, catalog_table)
            .await?
            .metadata_location)
    }

    pub(crate) async fn commit(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<CatalogCommitOutcome> {
        let manager = self.context.extension::<CatalogManager>()?;
        let requirements = requirements
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let updates = updates
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        match manager
            .commit_lakehouse_table(
                self.catalog_table,
                LakehouseCommitRequest {
                    context: lakehouse_table.clone(),
                    format: "iceberg".to_string(),
                    requirements,
                    updates,
                    payload: None,
                },
            )
            .await
        {
            Ok(outcome) => match outcome {
                LakehouseCommitOutcome::Committed { payload, .. } => Ok(
                    CatalogCommitOutcome::Committed(CatalogCommittedTable::from_payload(payload)),
                ),
                LakehouseCommitOutcome::Noop { .. } => Ok(CatalogCommitOutcome::Committed(
                    CatalogCommittedTable::from_payload(None),
                )),
                LakehouseCommitOutcome::RetryableConflict { message } => {
                    log::warn!("Iceberg catalog commit conflict: {message}");
                    Ok(CatalogCommitOutcome::Conflict)
                }
                LakehouseCommitOutcome::StateUnknown { message } => {
                    // TODO: Preserve recovery and cleanup policy for Iceberg
                    // commit-state-unknown instead of reducing it to an execution error.
                    // REST requirements/updates, metadata-location CAS, and provider-native
                    // updates need distinct reconciliation paths.
                    Err(DataFusionError::Execution(format!(
                        "Iceberg catalog commit state is unknown: {message}"
                    )))
                }
                LakehouseCommitOutcome::Rejected { message } => Err(DataFusionError::Execution(
                    format!("Iceberg catalog commit was rejected: {message}"),
                )),
            },
            Err(CatalogError::NotSupported(err) | CatalogError::UnsupportedCapability(err)) => {
                log::debug!("Iceberg catalog commit is not supported: {err}");
                Ok(CatalogCommitOutcome::NotSupported)
            }
            Err(CatalogError::Conflict(err)) => {
                log::warn!("Iceberg catalog commit conflict: {err}");
                Ok(CatalogCommitOutcome::Conflict)
            }
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }

    pub(crate) async fn update_metadata_location(
        &self,
        existing_properties: &[(String, String)],
        previous_metadata_location: Option<&str>,
        new_metadata_location: &str,
    ) -> Result<()> {
        let manager = self.context.extension::<CatalogManager>()?;
        let metadata_location_key = existing_metadata_location_key(existing_properties)
            .map(ToString::to_string)
            .unwrap_or_else(|| METADATA_LOCATION_UNDERSCORE_KEY.to_string());
        let mut properties = vec![(metadata_location_key, new_metadata_location.to_string())];
        if let Some(previous_metadata_location) = previous_metadata_location {
            properties.push((
                PREVIOUS_METADATA_LOCATION_KEY.to_string(),
                previous_metadata_location.to_string(),
            ));
        }
        manager
            .alter_table(
                self.catalog_table,
                AlterTableOptions::SetTableProperties { properties },
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

pub(crate) fn catalog_table_info_from_status(status: &TableStatus) -> CatalogTableInfo {
    match &status.kind {
        TableKind::Table {
            format: _,
            properties,
            ..
        } => CatalogTableInfo {
            metadata_location: metadata_location_from_properties(properties),
            is_catalog_managed_iceberg_table: catalog_managed_iceberg_from_properties(properties),
        },
        _ => CatalogTableInfo::default(),
    }
}

pub(crate) fn catalog_requirements(
    table_meta: &TableMetadata,
    commit_requirements: &[TableRequirement],
    action_requirements: &[TableRequirement],
) -> Vec<TableRequirement> {
    let mut requirements = Vec::with_capacity(
        commit_requirements.len()
            + action_requirements.len()
            + usize::from(table_meta.table_uuid.is_some()),
    );
    requirements.extend_from_slice(commit_requirements);
    requirements.extend_from_slice(action_requirements);
    if let Some(uuid) = table_meta.table_uuid
        && !requirements
            .iter()
            .any(|requirement| matches!(requirement, TableRequirement::UuidMatch { uuid: u } if *u == uuid))
        {
            requirements.push(TableRequirement::UuidMatch { uuid });
        }
    requirements
}

pub(crate) fn table_metadata_location(table_url: &Url, metadata_file: &str) -> Result<String> {
    if crate::utils::parse_absolute_url(metadata_file).is_some() {
        return Ok(metadata_file.to_string());
    }
    Ok(table_url
        .join(metadata_file)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_table_preserves_rest_commit_payload_metadata_location() {
        let payload = serde_json::json!({
            "metadata-location": "s3://bucket/table/metadata/00002-uuid.metadata.json",
            "metadata": {
                "table-uuid": "table-uuid"
            }
        });

        let committed = CatalogCommittedTable::from_payload(Some(payload.clone()));

        assert_eq!(
            committed.metadata_location(),
            Some("s3://bucket/table/metadata/00002-uuid.metadata.json")
        );
        assert_eq!(committed.payload(), Some(&payload));
    }
}
