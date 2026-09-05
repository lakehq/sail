use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::TaskContext;
use object_store::ObjectStoreExt;

use super::operation::SnapshotOperation;
use crate::catalog_support::commit::{
    CatalogCommitOutcome, CatalogTableInfo, IcebergCatalogCommitCoordinator,
    IcebergCatalogCommitMode, catalog_requirements, table_metadata_location,
};
use crate::io::StoreContext;
use crate::lake_source::{
    catalog_managed_iceberg_from_properties, metadata_location_from_properties,
    resolve_iceberg_metadata_location,
};
use crate::spec::metadata::table_metadata::{MetadataLog, SnapshotLog};
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{TableMetadata, TableUpdate};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path, metadata_location_to_object_path_string, write_version_hint,
};
use crate::utils::metadata::metadata_files_for_version;

const MAX_PROCEDURE_COMMIT_RETRIES: usize = 5;

pub(in crate::procedure) async fn commit_snapshot_operation(
    ctx: &TaskContext,
    table_url: &url::Url,
    table_properties: &[(String, String)],
    lakehouse_table: Option<&sail_common_datafusion::catalog::LakehouseExecutionContext>,
    operation: SnapshotOperation,
    output_schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
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
        let metadata_location = resolve_iceberg_metadata_location(
            lakehouse_table,
            recorded_metadata_location.clone(),
            catalog_info.is_catalog_managed_iceberg_table
                || catalog_managed_iceberg_from_properties(table_properties),
        )?;
        let metadata_file = match metadata_location.as_deref() {
            Some(location) => metadata_location_to_object_path_string(location)?,
            None => crate::table::find_latest_metadata_file(&object_store, table_url).await?,
        };
        let bytes = load_metadata_file_bytes(&object_store, &metadata_file).await?;
        let mut metadata = TableMetadata::from_json(&bytes)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let prepared = operation.prepare(&metadata, output_schema.clone())?;
        if !prepared.changed {
            return Ok(prepared.output);
        }

        let requirements =
            catalog_requirements(&metadata, std::slice::from_ref(&prepared.requirement), &[]);
        let updates = vec![prepared.update.clone()];
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
                CatalogCommitOutcome::Conflict if attempt < MAX_PROCEDURE_COMMIT_RETRIES => {
                    continue;
                }
                CatalogCommitOutcome::Conflict => return Err(procedure_commit_conflict()),
                CatalogCommitOutcome::NotSupported
                    if matches!(
                        commit_mode,
                        IcebergCatalogCommitMode::CompatibilityCatalogCommit
                    ) =>
                {
                    use_metadata_location_fallback = true;
                }
                CatalogCommitOutcome::NotSupported => {
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
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
        }
        apply_snapshot_update(
            &mut metadata,
            &prepared.update,
            metadata_location.as_deref().unwrap_or(&metadata_file),
        )?;
        let metadata_json = metadata
            .to_json()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let extension = metadata_file_extension_from_properties(&metadata.properties)?;
        let new_metadata_file = if use_metadata_location_fallback {
            format!(
                "metadata/{next_version:05}-{}{extension}",
                uuid::Uuid::new_v4()
            )
        } else {
            format!("metadata/v{next_version}{extension}")
        };
        let new_metadata_location = table_metadata_location(table_url, &new_metadata_file)?;
        let encoded = encode_metadata_file(&new_metadata_file, &metadata_json)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let metadata_path = object_store::path::Path::from(new_metadata_file.as_str());
        let result = store_context
            .prefixed
            .put_opts(
                &metadata_path,
                object_store::PutPayload::from(Bytes::from(encoded)),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. })
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES =>
            {
                continue;
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                return Err(procedure_commit_conflict());
            }
            Err(error) => return Err(DataFusionError::External(Box::new(error))),
        }
        if !use_metadata_location_fallback {
            let version_files = metadata_files_for_version(&store_context, next_version).await?;
            if version_files
                .iter()
                .any(|candidate| candidate != &new_metadata_file)
            {
                let _ = store_context.prefixed.delete(&metadata_path).await;
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
            IcebergCatalogCommitCoordinator::new(ctx, table)
                .update_metadata_location(
                    table_properties,
                    recorded_metadata_location.as_deref(),
                    &new_metadata_location,
                )
                .await?;
        }
        let version_hint = if use_metadata_location_fallback {
            new_metadata_file
                .rsplit('/')
                .next()
                .unwrap_or(new_metadata_file.as_str())
                .to_string()
        } else {
            next_version.to_string()
        };
        write_version_hint(&store_context.prefixed, &version_hint).await;
        return Ok(prepared.output);
    }
    Err(procedure_commit_conflict())
}

fn apply_snapshot_update(
    metadata: &mut TableMetadata,
    update: &TableUpdate,
    previous_metadata_file: &str,
) -> Result<()> {
    let TableUpdate::SetSnapshotRef {
        ref_name,
        reference,
    } = update
    else {
        return Err(DataFusionError::Internal(
            "Iceberg snapshot procedure produced a non-reference update".to_string(),
        ));
    };
    let previous_timestamp = metadata.last_updated_ms;
    let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    if ref_name == MAIN_BRANCH {
        metadata.current_snapshot_id = Some(reference.snapshot_id);
        metadata.snapshot_log.push(SnapshotLog {
            timestamp_ms,
            snapshot_id: reference.snapshot_id,
        });
    }
    metadata.refs.insert(ref_name.clone(), reference.clone());
    metadata.last_updated_ms = timestamp_ms;
    metadata.metadata_log.push(MetadataLog {
        timestamp_ms: previous_timestamp,
        metadata_file: previous_metadata_file.to_string(),
    });
    Ok(())
}

fn procedure_commit_conflict() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg procedure commit failed after {MAX_PROCEDURE_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}
