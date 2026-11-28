// SPDX-License-Identifier: Apache-2.0

use datafusion::arrow::datatypes::SchemaRef;
use sail_catalog::provider::{AlterTableOptions, DropTableOptions};
use sail_common_datafusion::catalog::iceberg::{
    ICEBERG_METADATA_LOCATION_UNDERSCORE_KEY, MIGRATION_BACKUP_PROPERTY,
};

use super::*;

/// A metastore publishes by registering a pointer to a unique metadata file.
pub(super) struct Import {
    pub destination: Vec<String>,
    pub source: Vec<String>,
    pub source_kind: TableKind,
    pub backup: Option<String>,
    pub drop_backup: bool,
    pub options: CreateTableOptions,
    pub url: url::Url,
    pub metadata: TableMetadata,
    pub parallelism: i32,
    pub ignore_missing: bool,
    pub output_schema: SchemaRef,
}

pub(super) async fn import(ctx: &TaskContext, mut request: Import) -> Result<RecordBatch> {
    let manager = ctx.extension::<CatalogManager>()?;
    request.options.mode = CreateTableMode::Create;
    manager
        .create_table_metadata_requirement(&request.destination, &request.options)
        .map_err(catalog_error)?;
    let mut source = request.source.clone();
    if let Some(backup) = &request.backup {
        *source
            .last_mut()
            .ok_or_else(|| DataFusionError::Plan("Empty source name".into()))? = backup.clone();
        // Hive may relocate a managed table on rename. Always import the renamed table's files.
        if manager
            .get_table(&request.source)
            .await
            .map_err(catalog_error)?
            .kind
            != request.source_kind
        {
            return plan_err!("Migration source changed before rename");
        }
        manager
            .rename_table(&request.source, &source)
            .await
            .map_err(catalog_error)?;
    }
    let outcome = register(ctx, &manager, &source, &mut request).await;
    if request.backup.is_some() {
        match &outcome {
            Ok(_) if request.drop_backup => {
                if let Err(error) = manager
                    .drop_table(
                        &source,
                        DropTableOptions {
                            if_exists: false,
                            purge: false,
                        },
                    )
                    .await
                {
                    log::warn!(
                        "Migrated {}, but could not drop backup {}: {error}",
                        request.destination.join("."),
                        source.join(".")
                    );
                }
            }
            Ok(_) => {
                if let Err(error) = manager
                    .alter_table(
                        &source,
                        AlterTableOptions::SetTableProperties {
                            properties: vec![(MIGRATION_BACKUP_PROPERTY.into(), "true".into())],
                        },
                    )
                    .await
                {
                    log::warn!(
                        "Migrated {}, but could not mark {} as a migration backup: {error}",
                        request.destination.join("."),
                        source.join(".")
                    );
                }
            }
            Err(error) if is_unknown_commit(error) => {}
            Err(error) => {
                let reason = match manager.get_table_or_view(&request.destination).await {
                    Err(CatalogError::NotFound(_, _)) => {
                        match manager.rename_table(&source, &request.destination).await {
                            Ok(()) => return outcome,
                            Err(restore) => format!("backup rename failed: {restore}"),
                        }
                    }
                    Err(lookup) => format!("destination lookup failed: {lookup}"),
                    Ok(_) => "destination is occupied".to_owned(),
                };
                let message = format!(
                    "Migration failed ({error}); could not confirm restoration of {} from {}: {reason}",
                    request.destination.join("."),
                    source.join(".")
                );
                return Err(catalog_error(CatalogError::CommitStateUnknown(
                    message.into(),
                )));
            }
        }
    }
    outcome
}

async fn register(
    ctx: &TaskContext,
    manager: &CatalogManager,
    source: &[String],
    request: &mut Import,
) -> Result<RecordBatch> {
    match manager.get_table_or_view(&request.destination).await {
        Ok(_) => {
            return plan_err!(
                "Destination already exists: {}",
                request.destination.join(".")
            );
        }
        Err(CatalogError::NotFound(_, _)) => {}
        Err(error) => return Err(catalog_error(error)),
    }
    let (store, prepared) = prepare_import(
        ctx,
        &request.url,
        &request.metadata,
        source,
        request.parallelism,
        request.ignore_missing,
        request.output_schema.clone(),
    )
    .await?;
    apply_snapshot_actions(&mut request.metadata, &prepared.updates)?;
    let batch = prepared.output;
    ensure_new_table_location(&store, &request.url).await?;
    let publication = MetadataFileCommit::prepare(
        &request.url,
        &request.metadata,
        0,
        MetadataFileStyle::Unique,
    )?;
    let mut cleanup = stage_import_metadata(&store, &publication, &prepared.staged_files).await?;
    let mut options = request.options.clone();
    options.properties = request
        .metadata
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    options.properties.push((
        ICEBERG_METADATA_LOCATION_UNDERSCORE_KEY.into(),
        publication.location().to_owned(),
    ));
    let created = manager.create_table(&request.destination, options).await;
    let registered = match created {
        Ok(status) => status,
        Err(error) => match manager.get_table(&request.destination).await {
            Ok(status) => status,
            Err(CatalogError::NotFound(_, _))
                if matches!(
                    error,
                    CatalogError::AlreadyExists(_, _)
                        | CatalogError::InvalidArgument(_)
                        | CatalogError::NotSupported(_)
                ) =>
            {
                cleanup.cleanup().await;
                return Err(catalog_error(error));
            }
            _ => {
                return Err(catalog_error(CatalogError::CommitStateUnknown(
                    format!(
                        "Could not confirm registration at {}: {error}",
                        publication.location()
                    )
                    .into(),
                )));
            }
        },
    };
    if !registered
        .kind
        .properties()
        .iter()
        .any(|(k, v)| k == ICEBERG_METADATA_LOCATION_UNDERSCORE_KEY && v == publication.location())
    {
        return plan_err!("Destination changed during import registration");
    }
    Ok(batch)
}

async fn stage_import_metadata(
    store: &StoreContext,
    publication: &MetadataFileCommit,
    manifests: &[(String, Vec<u8>)],
) -> Result<crate::operations::snapshot::CreatedPathCleanup> {
    let mut cleanup = stage_manifests(store, manifests).await?;
    cleanup.publication_started();
    match publication.write(store).await {
        Ok(MetadataWriteOutcome::Written) => {}
        Ok(MetadataWriteOutcome::Conflict) => {
            cleanup.cleanup().await;
            return plan_err!("Import metadata file already exists");
        }
        Err(error) => {
            return Err(catalog_error(CatalogError::CommitStateUnknown(
                format!(
                    "Could not confirm metadata write at {}: {error}",
                    publication.location()
                )
                .into(),
            )));
        }
    }
    cleanup.register(publication.path().clone());
    Ok(cleanup)
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt;

    use super::*;
    use crate::io::test_store::{PutFault, TestStore};

    #[tokio::test]
    async fn uncertain_metadata_put_preserves_staged_manifests() -> Result<()> {
        let url = url::Url::parse("memory://bucket/table/")
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut fault = TestStore::new(Arc::new(object_store::memory::InMemory::new()));
        fault.fault = Some((".metadata.json".into(), PutFault::LostAckUnreadable));
        let fault = Arc::new(fault);
        let store = StoreContext::new(fault.clone(), &url)?;
        let schema = crate::spec::Schema::builder()
            .build()
            .map_err(DataFusionError::Execution)?;
        let metadata = empty_table_metadata(
            &url,
            schema,
            crate::spec::PartitionSpec::unpartitioned_spec(),
            &[],
        )?;
        let publication =
            MetadataFileCommit::prepare(&url, &metadata, 0, MetadataFileStyle::Unique)?;
        let manifest = "metadata/test.avro";
        let Err(error) =
            stage_import_metadata(&store, &publication, &[(manifest.into(), vec![1])]).await
        else {
            return datafusion_common::exec_err!("Expected a lost acknowledgement");
        };
        assert!(is_unknown_commit(&error));
        tokio::task::yield_now().await;
        let durable = StoreContext::new(fault.inner.clone(), &url)?;
        durable.prefixed.head(publication.path()).await?;
        durable.prefixed.head(&manifest.into()).await?;
        Ok(())
    }
}
