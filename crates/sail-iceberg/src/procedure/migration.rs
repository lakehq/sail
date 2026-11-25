// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result, plan_err};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CreateTableColumnOptions, CreateTableMode, CreateTableOptions, StageTableOptions,
};
use sail_common_datafusion::catalog::{LakehouseExecutionContext, TableKind, TableReplacement};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakeprocedure::LakeProcedureCall;

use super::add_files::{AddFiles, ImportOutput, supports_import_format};
use super::arguments::{
    optional_bool, optional_i32, optional_string, optional_string_map, required_string,
};
use super::descriptor::IcebergProcedureType;
use super::snapshot::operation::PreparedSnapshotOperation;
use super::snapshot::stage_manifests;
use crate::datasource::type_converter::arrow_schema_to_iceberg;
use crate::io::StoreContext;
use crate::lake_source::create_table_partition_spec;
use crate::operations::bootstrap::empty_table_metadata;
use crate::operations::metadata_commit::{
    BaseMetadataCodec, MetadataFileCommit, MetadataFileStyle, MetadataWriteOutcome,
    apply_snapshot_actions,
};
use crate::schema_evolution::SchemaEvolver;
use crate::spec::TableMetadata;

mod registration;

fn catalog_error(error: CatalogError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn table_name(name: &str) -> Result<Vec<String>> {
    sail_sql_analyzer::parser::parse_object_name(name)
        .and_then(sail_sql_analyzer::expression::from_ast_object_name)
        .map(Into::into)
        .map_err(|error| DataFusionError::Plan(error.to_string()))
}

pub(crate) async fn execute(
    ctx: &Arc<TaskContext>,
    call: &LakeProcedureCall,
) -> Result<RecordBatch> {
    let invocation = &call.invocation;
    let migrate = match IcebergProcedureType::parse(&invocation.procedure.name) {
        Some(IcebergProcedureType::Migrate) => true,
        Some(IcebergProcedureType::Snapshot) => false,
        _ => return plan_err!("Expected a table migration procedure"),
    };
    let manager = ctx.extension::<CatalogManager>()?;
    let destination = table_name(&required_string(invocation, "table")?)?;
    let destination_catalog = destination
        .first()
        .ok_or_else(|| DataFusionError::Plan("Missing destination catalog".into()))?;
    let source = if migrate {
        destination.clone()
    } else {
        table_name(&required_string(invocation, "source_table")?)?
    };
    let parallelism = optional_i32(invocation, "parallelism")?.unwrap_or(1);
    if parallelism <= 0 {
        return plan_err!("Parallelism should be larger than 0");
    }
    let status = manager.get_table(&source).await.map_err(catalog_error)?;
    let TableKind::Table {
        columns,
        location: Some(source_location),
        format,
        bucket_by,
        properties,
        partition_by,
        comment,
        is_external,
        ..
    } = &status.kind
    else {
        return plan_err!("Migration source must be a stored file table");
    };
    if !supports_import_format(format) {
        return plan_err!("Unsupported migration source format: {format}");
    }
    if let Some(bucket) = bucket_by {
        return plan_err!(
            "Cannot create an Iceberg table from a bucketed source table: {} buckets, bucket columns: [{}]",
            bucket.num_buckets,
            bucket.columns.join(", ")
        );
    }
    let mut properties = properties.iter().cloned().collect::<BTreeMap<_, _>>();
    for key in [
        "path",
        "transient_lastDdlTime",
        "serialization.format",
        "provider",
        "location",
    ] {
        properties.remove(key);
    }
    if !migrate {
        for key in [
            "write.metadata.path",
            "write.folder-storage.path",
            "write.object-storage.path",
            "write.data.path",
        ] {
            properties.remove(key);
        }
    }
    properties.extend(optional_string_map(invocation, "properties")?.unwrap_or_default());
    let property_location = properties.remove("location");
    let location = if migrate {
        properties.insert("migrated".into(), "true".into());
        Some(property_location.unwrap_or_else(|| source_location.clone()))
    } else {
        properties.insert("gc.enabled".into(), "false".into());
        properties.insert("snapshot".into(), "true".into());
        optional_string(invocation, "location")?.or(property_location)
    };
    let location = location
        .map(sail_data_source::resolve_table_url)
        .transpose()?;
    let source_url = sail_data_source::resolve_table_url(source_location.clone())?;
    // Spark file-table schemas place partition columns after the data columns, in spec order.
    let mut columns = columns.iter().collect::<Vec<_>>();
    columns.sort_by_cached_key(|column| partition_by.iter().position(|p| p.column == column.name));
    let iceberg_schema = SchemaEvolver::assign_schema_field_ids(&arrow_schema_to_iceberg(
        &Schema::new(columns.iter().map(|c| c.field()).collect::<Vec<_>>()),
    )?)?;
    for key in properties.keys() {
        if let Some(column) = key.strip_prefix("write.metadata.metrics.column.")
            && iceberg_schema.field_id_by_name(column).is_none()
        {
            return plan_err!("Invalid metrics config: column {column} does not exist");
        }
    }
    let spec = create_table_partition_spec(&iceberg_schema, partition_by)?;
    if let Some(comment) = comment {
        properties
            .entry("comment".into())
            .or_insert_with(|| comment.clone());
    }
    let properties = crate::lake_source::iceberg_table_properties_from_catalog_create(
        properties.into_iter().collect(),
    )?;

    let options = CreateTableOptions {
        columns: columns
            .iter()
            .map(|c| CreateTableColumnOptions {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                metadata: c.metadata.clone(),
                comment: c.comment.clone(),
                default: c.default.clone(),
                generated_always_as: c.generated_always_as.clone(),
                identity: c.identity.clone(),
            })
            .collect(),
        comment: comment.clone(),
        constraints: vec![],
        location: location.as_ref().map(ToString::to_string),
        format: "iceberg".into(),
        partition_by: partition_by.clone(),
        sort_by: vec![],
        bucket_by: None,
        mode: if migrate {
            CreateTableMode::Replace
        } else {
            CreateTableMode::Create
        },
        properties,
        is_external: !migrate || *is_external,
        is_write_precondition: false,
    };
    let backup = if migrate {
        let backup = optional_string(invocation, "backup_table_name")?
            .unwrap_or_else(|| format!("{}_BACKUP_", status.name));
        if backup.is_empty() || backup == status.name {
            return plan_err!("Invalid migration backup name: {backup}");
        }
        Some(backup)
    } else {
        None
    };
    let drop_backup = optional_bool(invocation, "drop_backup")?.unwrap_or(false);
    let ignore_missing = optional_bool(invocation, "ignore_missing_files")?.unwrap_or(false);
    if manager
        .catalog_capabilities(destination_catalog)
        .map_err(catalog_error)?
        .contains(&sail_catalog::lakehouse::LakehouseCapability::IcebergImportRegistration)
    {
        let url =
            location.ok_or_else(|| DataFusionError::Plan("Missing import location".into()))?;
        check_location(migrate, &source_url, &url)?;
        let metadata = empty_table_metadata(&url, iceberg_schema, spec, &options.properties)?;
        let ctx = Arc::clone(ctx);
        let output_schema = invocation.procedure.schema();
        return tokio::spawn(async move {
            registration::import(
                &ctx,
                registration::Import {
                    destination,
                    source,
                    source_kind: status.kind,
                    backup,
                    drop_backup,
                    options,
                    url,
                    metadata,
                    parallelism,
                    ignore_missing,
                    output_schema,
                },
            )
            .await
        })
        .await
        .map_err(|e| catalog_error(CatalogError::CommitStateUnknown(e.to_string().into())))?;
    }
    let stage = if let Some(name) = backup {
        StageTableOptions::IcebergMigration {
            definition: options,
            source: Box::new(status.kind),
            backup: sail_common_datafusion::catalog::MigrationBackup {
                name,
                retain: !drop_backup,
            },
        }
    } else {
        StageTableOptions::Definition(options)
    };
    let replacement = manager
        .stage_table_replacement(&destination, stage)
        .await
        .map_err(catalog_error)?;
    let url = sail_data_source::resolve_table_url(replacement.location.clone())?;
    check_location(migrate, &source_url, &url)?;
    let metadata = if let Some(json) = replacement.authority.rest_metadata() {
        serde_json::from_str(json).map_err(|e| DataFusionError::External(Box::new(e)))?
    } else {
        empty_table_metadata(&url, iceberg_schema, spec, &replacement.property_updates)?
    };
    let (store, prepared) = prepare_import(
        ctx,
        &url,
        &metadata,
        &source,
        parallelism,
        ignore_missing,
        invocation.procedure.schema(),
    )
    .await?;
    if let Some(execution) = replacement.authority.execution().cloned() {
        publish_rest_import(ctx, &store, replacement, execution, metadata, prepared).await
    } else {
        publish_path_import(&manager, store, url, replacement, metadata, prepared).await
    }
}

async fn publish_rest_import(
    ctx: &Arc<TaskContext>,
    store: &StoreContext,
    replacement: TableReplacement,
    execution: LakehouseExecutionContext,
    metadata: TableMetadata,
    prepared: PreparedSnapshotOperation,
) -> Result<RecordBatch> {
    use crate::catalog_support::commit::{CatalogCommitOutcome, IcebergCatalogCommitCoordinator};
    let (requirements, mut updates) =
        crate::operations::replacement::rest_table_changes(&replacement, &metadata, &metadata)?;
    updates.extend(prepared.updates);
    let mut cleanup = stage_manifests(store, &prepared.manifests).await?;
    let ctx = Arc::clone(ctx);
    let publish = async move {
        let coordinator =
            IcebergCatalogCommitCoordinator::new(ctx.as_ref(), &replacement.catalog_table);
        cleanup.publication_started();
        match coordinator.commit(&execution, requirements, updates).await {
            Ok(CatalogCommitOutcome::Committed(response))
                if response.metadata_location().is_some() =>
            {
                Ok(())
            }
            Ok(CatalogCommitOutcome::Committed(_)) => {
                Err(catalog_error(CatalogError::CommitStateUnknown(
                    "Missing snapshot table commit response".to_string().into(),
                )))
            }
            Ok(CatalogCommitOutcome::Conflict | CatalogCommitOutcome::NotSupported) => {
                cleanup.cleanup().await;
                plan_err!("Catalog rejected snapshot table creation")
            }
            Err(error) => {
                if !is_unknown_commit(&error) {
                    cleanup.cleanup().await;
                }
                Err(error)
            }
        }
    };
    tokio::spawn(publish).await.map_err(|error| {
        catalog_error(CatalogError::CommitStateUnknown(error.to_string().into()))
    })??;
    Ok(prepared.output)
}

async fn publish_path_import(
    manager: &CatalogManager,
    store: StoreContext,
    url: url::Url,
    mut replacement: TableReplacement,
    mut metadata: TableMetadata,
    prepared: PreparedSnapshotOperation,
) -> Result<RecordBatch> {
    let batch = prepared.output;
    apply_snapshot_actions(&mut metadata, &prepared.updates)?;
    replacement.properties = metadata
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let publication = MetadataFileCommit::prepare(
        &url,
        &metadata,
        1,
        MetadataFileStyle::Versioned(BaseMetadataCodec::capture(None)?),
    )?;
    manager
        .publish_table_replacement(
            replacement,
            Box::pin(async move {
                ensure_new_table_location(&store, &url).await?;
                let mut cleanup = stage_manifests(&store, &prepared.manifests).await?;
                cleanup.publication_started();
                match publication.write(&store).await {
                    Ok(MetadataWriteOutcome::Written) => publication.write_hint(&store).await,
                    Ok(MetadataWriteOutcome::Conflict) => {
                        cleanup.cleanup().await;
                        return Err(CatalogError::Conflict(
                            "Iceberg metadata already exists".into(),
                        ));
                    }
                    Err(error) => {
                        return Err(CatalogError::CommitStateUnknown(error.to_string().into()));
                    }
                }
                Ok(())
            }),
        )
        .await
        .map_err(catalog_error)?;
    Ok(batch)
}

async fn prepare_import(
    ctx: &TaskContext,
    url: &url::Url,
    metadata: &TableMetadata,
    source: &[String],
    parallelism: i32,
    ignore_missing: bool,
    output_schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<(StoreContext, PreparedSnapshotOperation)> {
    let store = StoreContext::new(ctx.runtime_env().object_store_registry.get_store(url)?, url)?;
    let prepared = AddFiles::for_migration(source.to_vec(), parallelism as usize, ignore_missing)
        .prepare(
            ctx,
            &store,
            url,
            metadata,
            ImportOutput::FileCount(output_schema),
        )
        .await?;
    Ok((store, prepared))
}

fn check_location(migrate: bool, source_url: &url::Url, url: &url::Url) -> Result<()> {
    if !migrate
        && (url.as_str().starts_with(source_url.as_str())
            || source_url.as_str().starts_with(url.as_str()))
    {
        return plan_err!("Snapshot location overlaps the source table location");
    }
    Ok(())
}

fn is_unknown_commit(error: &DataFusionError) -> bool {
    matches!(error, DataFusionError::External(cause) if cause.downcast_ref::<CatalogError>().is_some_and(|error|matches!(error,CatalogError::CommitStateUnknown(_))))
}

async fn ensure_new_table_location(store: &StoreContext, url: &url::Url) -> Result<()> {
    if crate::table::metadata_loader::latest_metadata_file(&store.base, url)
        .await?
        .is_some()
    {
        return plan_err!("Iceberg metadata already exists at {url}");
    }
    Ok(())
}
