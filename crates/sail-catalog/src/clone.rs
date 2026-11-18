use datafusion::execution::TaskContext;
use sail_common::spec::CreateTableMode;
use sail_common_datafusion::catalog::delta::is_delta_coordinated_property;
use sail_common_datafusion::catalog::{TableKind, TableStatus};
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::error::CommitStateUnknownError;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::CloneMetrics;

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{CreateTableColumnOptions, CreateTableOptions};

fn delta_path(name: &[String]) -> Option<&str> {
    match name {
        [format, path] if format.eq_ignore_ascii_case("delta") => Some(path),
        _ => None,
    }
}

async fn lookup_clone_table(
    manager: &CatalogManager,
    name: &[String],
) -> CatalogResult<Option<TableStatus>> {
    if delta_path(name)
        .is_some_and(|path| std::path::Path::new(path).is_absolute() || path.contains(":/"))
    {
        return Ok(None);
    }
    match manager.get_table_or_view(name).await {
        Ok(status) => Ok(Some(status)),
        Err(CatalogError::NotFound(_, _)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn table_location(kind: &TableKind) -> CatalogResult<(&str, bool)> {
    match kind {
        TableKind::Table {
            format,
            location: Some(location),
            is_external,
            properties,
            ..
        } if format.eq_ignore_ascii_case("delta") => {
            if properties
                .iter()
                .any(|(key, _)| is_delta_coordinated_property(key))
            {
                return Err(CatalogError::NotSupported(
                    "SHALLOW CLONE of catalog-coordinated Delta tables".into(),
                ));
            }
            Ok((location, *is_external))
        }
        _ => Err(CatalogError::NotSupported(
            "SHALLOW CLONE requires Delta tables".into(),
        )),
    }
}

pub(crate) async fn clone_table(
    ctx: &TaskContext,
    manager: &CatalogManager,
    table: Vec<String>,
    source: Vec<String>,
    location: Option<String>,
    default_location: String,
    mode: CreateTableMode,
    is_external: bool,
) -> CatalogResult<Option<CloneMetrics>> {
    let existing = lookup_clone_table(manager, &table).await?;
    let path_target = delta_path(&table).filter(|_| existing.is_none());
    if path_target.is_some() && location.is_some() {
        return Err(CatalogError::InvalidArgument(
            "LOCATION cannot be combined with a path clone target".into(),
        ));
    }
    if existing.is_some() && mode.ignore_if_exists() {
        return Ok(None);
    }
    if existing.is_some() && !mode.is_replace() {
        return Err(CatalogError::AlreadyExists(
            CatalogObject::Table,
            table.join("."),
        ));
    }
    if path_target.is_none() && existing.is_none() && mode.replace_requires_existing() {
        return Err(CatalogError::NotFound(
            CatalogObject::Table,
            table.join("."),
        ));
    }
    let (destination, external) = if let Some(status) = &existing {
        let (old_location, external) = table_location(&status.kind)?;
        if location.as_deref().is_some_and(|path| path != old_location) {
            return Err(CatalogError::NotSupported(
                "relocating a Delta table during SHALLOW CLONE".into(),
            ));
        }
        (old_location.to_string(), external)
    } else {
        (
            location.clone().unwrap_or(default_location),
            is_external || location.is_some() || path_target.is_some(),
        )
    };
    let source_location = if let Some(status) = lookup_clone_table(manager, &source).await? {
        table_location(&status.kind)?.0.to_string()
    } else if let Some(path) = delta_path(&source) {
        path.to_string()
    } else {
        return Err(CatalogError::NotFound(
            CatalogObject::Table,
            source.join("."),
        ));
    };
    let lake = ctx
        .extension::<DataSourceRegistry>()?
        .get_lake_source("delta")?;
    // An inferred directory may still belong to a renamed managed table.
    let storage_mode = if existing.is_none() && path_target.is_none() && location.is_none() {
        CreateTableMode::Create
    } else {
        mode
    };
    let prepared = lake
        .prepare_shallow_clone(ctx, &source_location, &destination, storage_mode)
        .await?;
    let publish = Box::pin(async move {
        prepared.publish.await.map_err(|error| {
            let mut cause: &(dyn std::error::Error + 'static) = &error;
            loop {
                if cause.is::<CommitStateUnknownError>() {
                    return CatalogError::CommitStateUnknown(error.to_string().into());
                }
                match cause.source() {
                    Some(source) => cause = source,
                    None => return CatalogError::from(error),
                }
            }
        })
    });
    if path_target.is_some() {
        publish.await?;
        return Ok(prepared.metrics);
    }
    let metadata = prepared.metadata;
    let options = CreateTableOptions {
        columns: metadata
            .schema
            .fields()
            .iter()
            .map(|field| CreateTableColumnOptions {
                metadata: field
                    .metadata()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
            })
            .collect(),
        comment: None,
        constraints: vec![],
        location: Some(destination),
        format: "delta".into(),
        partition_by: metadata.partition_by.unwrap_or_default(),
        sort_by: vec![],
        bucket_by: None,
        mode: if existing.is_some() {
            CreateTableMode::Replace
        } else {
            CreateTableMode::Create
        },
        properties: metadata.properties.clone(),
        is_external: external,
        is_write_precondition: false,
    };
    let mut replacement = manager.stage_table_replacement(&table, options).await?;
    replacement.properties = metadata.properties;
    manager
        .publish_table_replacement(replacement, publish)
        .await?;
    Ok(prepared.metrics)
}
