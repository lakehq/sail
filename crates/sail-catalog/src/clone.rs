// SPDX-License-Identifier: Apache-2.0

use datafusion::execution::TaskContext;
use sail_common::spec::CreateTableMode;
use sail_common_datafusion::catalog::delta::is_delta_coordinated_property;
use sail_common_datafusion::catalog::{TableKind, TableStatus};
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::error::CommitStateUnknownError;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::CloneMetrics;

use crate::command::CloneTarget;
use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{CreateTableOptions, StageTableOptions};

impl CatalogManager {
    /// In the delta namespace, an unqualified file name can also be a table name.
    pub async fn clone_path<'a, T: AsRef<str>>(
        &self,
        name: &'a [T],
    ) -> CatalogResult<Option<&'a str>> {
        let [format, path] = name else {
            return Ok(None);
        };
        if !format.as_ref().eq_ignore_ascii_case("delta") {
            return Ok(None);
        }
        let path = path.as_ref();
        if std::path::Path::new(path).is_absolute()
            || path.contains(":/")
            || path.starts_with("./")
            || path.starts_with("../")
        {
            return Ok(Some(path));
        }
        match self
            .get_database_by_qualifier(std::slice::from_ref(format))
            .await
        {
            Ok(_) => Ok(None),
            Err(CatalogError::NotFound(_, _)) => Ok(Some(path)),
            Err(error) => Err(error),
        }
    }
}

async fn resolve_source(manager: &CatalogManager, source: &[String]) -> CatalogResult<String> {
    if let Some(path) = manager.clone_path(source).await? {
        return Ok(path.to_string());
    }
    let status = manager.get_table(source).await?;
    Ok(table_location(&status.kind)?.0.to_string())
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

impl StageTableOptions {
    /// Resolve inherited clone options from the same catalog entry as the staged generation.
    pub fn resolve(self, existing: Option<&TableStatus>) -> CatalogResult<CreateTableOptions> {
        let (default_location, location, mode, is_external) = match self {
            Self::Definition(options) => return Ok(options),
            Self::IcebergMigration { .. } => {
                return Err(CatalogError::NotSupported(
                    "Iceberg migration staging".into(),
                ));
            }
            Self::ShallowClone {
                default_location,
                location,
                mode,
                is_external,
            } => (default_location, location, mode, is_external),
        };
        let (destination, external) = if let Some(status) = existing {
            let (old_location, external) = table_location(&status.kind)?;
            if location.as_deref().is_some_and(|path| path != old_location) {
                return Err(CatalogError::NotSupported(
                    "relocating a Delta table during SHALLOW CLONE".into(),
                ));
            }
            (old_location.to_string(), external)
        } else {
            let external = is_external || location.is_some();
            (location.unwrap_or(default_location), external)
        };
        Ok(CreateTableOptions {
            columns: vec![],
            comment: None,
            constraints: vec![],
            location: Some(destination),
            format: "delta".into(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            mode,
            properties: vec![],
            is_external: external,
            is_write_precondition: false,
        })
    }
}

pub(crate) async fn clone_table(
    ctx: &TaskContext,
    manager: &CatalogManager,
    target: CloneTarget,
    source: Vec<String>,
    location: Option<String>,
    mode: CreateTableMode,
    is_external: bool,
) -> CatalogResult<Option<CloneMetrics>> {
    let (replacement, destination, storage_mode) = match target {
        CloneTarget::Path(path) => {
            if location.is_some() {
                return Err(CatalogError::InvalidArgument(
                    "LOCATION cannot be combined with a path clone target".into(),
                ));
            }
            (None, path, mode)
        }
        CloneTarget::Table {
            name,
            default_location,
        } => {
            let existing = match manager.get_table_or_view(&name).await {
                Ok(_) if mode.ignore_if_exists() => return Ok(None),
                Ok(_) if !mode.is_replace() => {
                    return Err(CatalogError::AlreadyExists(
                        CatalogObject::Table,
                        name.join("."),
                    ));
                }
                Ok(status) => {
                    table_location(&status.kind)?;
                    true
                }
                Err(e @ CatalogError::NotFound(_, _)) if mode.replace_requires_existing() => {
                    return Err(e);
                }
                Err(CatalogError::NotFound(_, _)) => false,
                Err(error) => return Err(error),
            };
            let inferred_location = location.is_none();
            let replacement = manager
                .stage_table_replacement(
                    &name,
                    StageTableOptions::ShallowClone {
                        default_location,
                        location,
                        mode: if existing {
                            CreateTableMode::Replace
                        } else {
                            CreateTableMode::Create
                        },
                        is_external,
                    },
                )
                .await?;
            // An inferred directory may still belong to a renamed managed table.
            let storage_mode = if !replacement.existing && inferred_location {
                CreateTableMode::Create
            } else {
                mode
            };
            let destination = replacement.location.clone();
            (Some(replacement), destination, storage_mode)
        }
    };
    let lake = ctx
        .extension::<DataSourceRegistry>()?
        .get_lake_source("delta")?;
    let source_location = async {
        resolve_source(manager, &source)
            .await
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))
    };
    let prepared = lake
        .prepare_shallow_clone(ctx, Box::pin(source_location), &destination, storage_mode)
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
    let Some(mut replacement) = replacement else {
        publish.await?;
        prepared.post_commit.await;
        return Ok(prepared.metrics);
    };
    let metadata = prepared.metadata;
    replacement.columns = metadata
        .schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
                field
                    .metadata()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            )
        })
        .collect();
    replacement.partition_by = metadata.partition_by.unwrap_or_default();
    replacement.properties = metadata.properties;
    manager
        .publish_table_replacement(replacement, publish)
        .await?;
    prepared.post_commit.await;
    Ok(prepared.metrics)
}
