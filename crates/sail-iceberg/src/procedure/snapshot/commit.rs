use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::TaskContext;
use object_store::ObjectStoreExt;

use super::operation::{PreparedSnapshotOperation, SnapshotOperation};
use crate::catalog_support::commit::{
    CatalogCommitOutcome, CatalogTableInfo, IcebergCatalogCommitCoordinator,
    IcebergCatalogCommitMode, catalog_requirements,
};
use crate::io::StoreContext;
use crate::lake_source::{
    catalog_managed_iceberg_from_properties, metadata_location_from_properties,
    resolve_iceberg_metadata_location,
};
use crate::operations::metadata_commit::{
    MetadataFileCommit, MetadataFileStyle, MetadataWriteOutcome, PreviousMetadataFile,
    apply_snapshot_updates,
};
use crate::procedure::rewrite_manifests::RewriteManifests;
use crate::spec::TableMetadata;
use crate::table::metadata_loader::{
    load_metadata_file_bytes, metadata_file_version_from_path,
    metadata_location_to_object_path_string,
};
use crate::utils::metadata::metadata_files_for_version;

const MAX_PROCEDURE_COMMIT_RETRIES: usize = 5;

pub(crate) enum ProcedureCommitOperation {
    Snapshot(SnapshotOperation),
    RewriteManifests(RewriteManifests),
}

impl From<SnapshotOperation> for ProcedureCommitOperation {
    fn from(operation: SnapshotOperation) -> Self {
        Self::Snapshot(operation)
    }
}

impl From<RewriteManifests> for ProcedureCommitOperation {
    fn from(operation: RewriteManifests) -> Self {
        Self::RewriteManifests(operation)
    }
}

pub(crate) async fn commit_snapshot_operation(
    ctx: &TaskContext,
    table_url: &url::Url,
    table_properties: &[(String, String)],
    lakehouse_table: Option<&sail_common_datafusion::catalog::LakehouseExecutionContext>,
    operation: impl Into<ProcedureCommitOperation>,
    output_schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let operation = operation.into();
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let store_context = StoreContext::new(object_store.clone(), table_url)?;
    let catalog_table = lakehouse_table.map(|context| context.catalog_table().to_vec());

    for attempt in 1..=MAX_PROCEDURE_COMMIT_RETRIES {
        let catalog_info = match catalog_table.as_ref() {
            Some(table) => IcebergCatalogCommitCoordinator::load_table_info(ctx, table).await?,
            None => CatalogTableInfo::default(),
        };
        let commit_mode =
            IcebergCatalogCommitMode::resolve(lakehouse_table, &catalog_info, table_properties)?;
        let recorded_metadata_location = catalog_info
            .metadata_location
            .clone()
            .or_else(|| metadata_location_from_properties(table_properties));
        let metadata_location = if commit_mode.uses_catalog_metadata() {
            resolve_iceberg_metadata_location(
                lakehouse_table,
                recorded_metadata_location.clone(),
                catalog_info.is_catalog_managed_iceberg_table
                    || catalog_managed_iceberg_from_properties(table_properties),
            )?
        } else {
            // A filesystem catalog's pointer is a hint, not the publication authority.
            // In particular, retries must discover the winning metadata version.
            None
        };
        let metadata_file = match metadata_location.as_deref() {
            Some(location) => metadata_location_to_object_path_string(location)?,
            None => crate::table::find_latest_metadata_file(&object_store, table_url).await?,
        };
        let bytes = load_metadata_file_bytes(&object_store, &metadata_file).await?;
        let mut metadata = TableMetadata::from_json(&bytes)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let prepared = match &operation {
            ProcedureCommitOperation::Snapshot(operation) => {
                operation.prepare(&metadata, output_schema.clone())?
            }
            ProcedureCommitOperation::RewriteManifests(operation) => {
                let prepared = operation
                    .prepare(&store_context, table_url, &metadata, output_schema.clone())
                    .await?;
                PreparedSnapshotOperation {
                    changed: !prepared.updates.is_empty(),
                    requirements: prepared.requirements,
                    updates: prepared.updates,
                    manifests: prepared.manifests,
                    output: prepared.output,
                }
            }
        };
        if !prepared.changed {
            return Ok(prepared.output);
        }

        let requirements = catalog_requirements(&metadata, &prepared.requirements, &[]);
        let updates = prepared.updates.clone();
        for (path, bytes) in &prepared.manifests {
            store_context
                .prefixed
                .put_opts(
                    &object_store::path::Path::from(path.as_str()),
                    bytes.clone().into(),
                    object_store::PutOptions {
                        mode: object_store::PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
        }
        let mut use_metadata_location_fallback = commit_mode.uses_metadata_location_update();
        if commit_mode.uses_catalog_commit() {
            let table = catalog_table.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "missing catalog table for Iceberg procedure commit".to_string(),
                )
            })?;
            let context = lakehouse_table.ok_or_else(|| {
                DataFusionError::Internal(
                    "missing lakehouse context for Iceberg procedure commit".to_string(),
                )
            })?;
            match IcebergCatalogCommitCoordinator::new(ctx, table)
                .commit(context, requirements, updates)
                .await?
            {
                CatalogCommitOutcome::Committed(_) => return Ok(prepared.output),
                CatalogCommitOutcome::Conflict => {
                    remove_uncommitted_manifests(&store_context, &prepared.manifests).await?;
                    if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                        continue;
                    }
                    return Err(procedure_commit_conflict());
                }
                CatalogCommitOutcome::NotSupported
                    if matches!(
                        commit_mode,
                        IcebergCatalogCommitMode::CompatibilityCatalogCommit
                    ) =>
                {
                    use_metadata_location_fallback = true;
                }
                CatalogCommitOutcome::NotSupported => {
                    remove_uncommitted_manifests(&store_context, &prepared.manifests).await?;
                    return plan_err!(
                        "Iceberg catalog commit is not supported by the resolved catalog authority"
                    );
                }
            }
        }

        let current_version = metadata_file_version_from_path(&metadata_file).unwrap_or(0);
        let next_version = current_version + 1;
        if !use_metadata_location_fallback {
            let existing = metadata_files_for_version(&store_context, next_version).await?;
            if !existing.is_empty() {
                remove_uncommitted_manifests(&store_context, &prepared.manifests).await?;
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
        }
        apply_snapshot_updates(
            table_url,
            &mut metadata,
            &prepared.updates,
            metadata_location.as_deref().map_or(
                PreviousMetadataFile::ObjectPath(&metadata_file),
                PreviousMetadataFile::CatalogLocation,
            ),
        )?;
        let style = if use_metadata_location_fallback {
            MetadataFileStyle::Unique
        } else {
            MetadataFileStyle::Versioned
        };
        let prepared_metadata =
            MetadataFileCommit::prepare(table_url, &metadata, next_version, style)?;
        match prepared_metadata.write(&store_context).await? {
            MetadataWriteOutcome::Written => {}
            MetadataWriteOutcome::Conflict => {
                remove_uncommitted_manifests(&store_context, &prepared.manifests).await?;
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
        }
        if let Some(table) = catalog_table.as_ref()
            && (use_metadata_location_fallback
                || matches!(commit_mode, IcebergCatalogCommitMode::Filesystem))
        {
            if let Err(error) = IcebergCatalogCommitCoordinator::new(ctx, table)
                .update_metadata_location(
                    table_properties,
                    recorded_metadata_location.as_deref(),
                    prepared_metadata.location(),
                )
                .await
            {
                // A unique metadata file is not published until its pointer CAS succeeds.
                // All other errors (including unknown commit state) preserve every artifact.
                if use_metadata_location_fallback && is_catalog_conflict(&error) {
                    let (store, path) = store_context.resolve(prepared_metadata.location())?;
                    store.delete(&path).await?;
                    remove_uncommitted_manifests(&store_context, &prepared.manifests).await?;
                    if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                        continue;
                    }
                    return Err(procedure_commit_conflict());
                }
                return Err(error);
            }
        }
        prepared_metadata.write_hint(&store_context).await;
        return Ok(prepared.output);
    }
    Err(procedure_commit_conflict())
}

fn is_catalog_conflict(error: &DataFusionError) -> bool {
    matches!(error, DataFusionError::External(error)
        if matches!(error.downcast_ref::<sail_catalog::error::CatalogError>(),
            Some(sail_catalog::error::CatalogError::Conflict(_))))
}

fn procedure_commit_conflict() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg procedure commit failed after {MAX_PROCEDURE_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}

async fn remove_uncommitted_manifests(
    store: &StoreContext,
    manifests: &[(String, Vec<u8>)],
) -> Result<()> {
    for (path, _) in manifests {
        match store
            .prefixed
            .delete(&object_store::path::Path::from(path.as_str()))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
