// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStoreExt;
use sail_common_datafusion::catalog::iceberg::is_iceberg_table_marker;
use sail_common_datafusion::catalog::managed::metadata_location_value;
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::{
    find_path_in_options, OptionLayer, PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat,
    TableFormatAlterTableOperation, TableFormatRegistry, CATALOG_TABLE_OPTION,
};
use sail_data_source::options::gen::{IcebergReadOptions, IcebergWriteOptions};
use sail_data_source::options::ResolveOptions;
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
use crate::io::StoreContext;
use crate::logical::IcebergTableSource;
use crate::physical_plan::plan_builder::{IcebergPlanBuilder, IcebergTableConfig};
use crate::physical_plan::IcebergWriterExecOptions;
use crate::spec::{MetadataLog, PartitionSpec, Schema, Snapshot, TableMetadata};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path, metadata_location_to_object_path_string,
};
use crate::table::{find_latest_metadata_file, Table};
use crate::utils::metadata::metadata_files_for_version;
use crate::utils::partition_transform::{
    catalog_partition_field_from_iceberg, format_partition_exprs,
};

const MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES: usize = 5;

/// Iceberg implementation of [`TableFormat`].
#[derive(Debug, Default)]
pub struct IcebergTableFormat;

impl IcebergTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))
    }
}

#[async_trait]
impl TableFormat for IcebergTableFormat {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let provider = build_iceberg_provider(ctx, info).await?;
        Ok(Arc::new(IcebergTableSource::new(provider)))
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;

        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in Iceberg table options");
        };
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            logical_schema,
        } = info;

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Iceberg format");
        }

        let table_url = Self::parse_table_url(vec![path]).await?;
        let catalog_table = catalog_table_from_options(&options)?;
        let metadata_location = metadata_location_from_options(&options);
        let catalog_managed_table = catalog_managed_iceberg_from_options(&options);
        let (options, table_properties) = split_iceberg_write_options_and_table_properties(options);
        let variant_shredding_option_presence =
            IcebergWriterExecOptions::variant_shredding_option_presence(&options);
        let iceberg_options = IcebergWriteOptions::resolve(ctx, options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let exists_res = match metadata_location.as_deref() {
            Some(location) if catalog_managed_table => {
                metadata_location_to_object_path_string(location)
            }
            _ => find_latest_metadata_file(&store, &table_url).await,
        };
        let table_exists = exists_res.is_ok();

        match mode {
            PhysicalSinkMode::ErrorIfExists if table_exists => {
                return plan_err!("Iceberg table already exists at path: {table_url}");
            }
            PhysicalSinkMode::IgnoreIfExists if table_exists => {
                return Ok(Arc::new(EmptyExec::new(input.schema())));
            }
            PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("predicate or partition overwrite for Iceberg");
            }
            _ => {}
        }

        // Get existing partition spec (encoded as partition expressions) if table exists
        let existing_partition_columns = if table_exists {
            let metadata_location = catalog_managed_table.then_some(metadata_location).flatten();
            let table =
                Table::load_with_metadata_location(ctx, table_url.clone(), metadata_location)
                    .await?;
            Some(Self::partition_columns_from_metadata(&table)?)
        } else {
            None
        };

        // Validate partition column mismatch for append/overwrite operations
        if let Some(existing_partitions) = &existing_partition_columns {
            if !partition_by.is_empty() && partition_by != *existing_partitions {
                // For append mode, partition column changes are not allowed
                match mode {
                    PhysicalSinkMode::Append => {
                        return plan_err!(
                            "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                            Cannot change partitioning on append.",
                            format_partition_exprs(existing_partitions),
                            format_partition_exprs(&partition_by)
                        );
                    }
                    PhysicalSinkMode::Overwrite
                        // For overwrite mode, check if schema overwrite is allowed
                        if !iceberg_options.overwrite_schema => {
                            return plan_err!(
                                "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                                Set overwriteSchema=true to change partitioning.",
                                format_partition_exprs(existing_partitions),
                                format_partition_exprs(&partition_by)
                            );
                        }
                    _ => {}
                }
            }
        }

        let resolved_partition_columns = if !partition_by.is_empty() {
            partition_by
        } else {
            existing_partition_columns.unwrap_or_default()
        };

        let mut options = IcebergWriterExecOptions::from(iceberg_options);
        options.apply_variant_shredding_option_presence(variant_shredding_option_presence);
        options.table_properties = table_properties;
        if let Some(catalog_table) = catalog_table {
            options.table_properties.push((
                CATALOG_TABLE_OPTION.to_string(),
                serde_json::to_string(&catalog_table)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ));
        }
        let table_config = IcebergTableConfig {
            table_url,
            partition_columns: resolved_partition_columns,
            table_exists,
            options,
        };

        let physical_sort = sort_order.map(|req| {
            req.into_iter()
                .map(|r| datafusion::physical_expr::PhysicalSortExpr {
                    expr: r.expr,
                    options: r.options.unwrap_or_default(),
                })
                .collect::<Vec<_>>()
        });

        let logical_input_schema = logical_schema.map(|schema| Arc::new(schema.as_arrow().clone()));
        let builder = IcebergPlanBuilder::new(
            input,
            table_config,
            mode,
            physical_sort,
            logical_input_schema,
            ctx,
        );
        let exec = builder.build().await?;
        Ok(exec)
    }

    async fn alter_table(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        operation: TableFormatAlterTableOperation,
    ) -> Result<()> {
        match operation {
            TableFormatAlterTableOperation::SetTableProperties { changes, if_exists } => {
                self.alter_table_properties(runtime_env, path, changes, if_exists)
                    .await
            }
            TableFormatAlterTableOperation::AlterColumnType { .. } => {
                not_impl_err!("Column type alteration not supported for Iceberg format")
            }
            TableFormatAlterTableOperation::AlterColumnDefault { .. } => {
                not_impl_err!("Column default alteration not supported for Iceberg format")
            }
            TableFormatAlterTableOperation::AddCheckConstraint { .. } => {
                not_impl_err!("CHECK constraints not supported for Iceberg format")
            }
        }
    }
}

impl IcebergTableFormat {
    async fn alter_table_properties(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<()> {
        let table_url = Self::parse_table_url(vec![path.to_string()]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

        let initial_latest_meta = find_latest_metadata_file(&object_store, &table_url).await?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let latest_meta = if attempt == 1 {
                initial_latest_meta.clone()
            } else {
                find_latest_metadata_file(&object_store, &table_url).await?
            };

            let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
            let mut table_meta = TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            crate::properties::apply_table_property_changes(&mut table_meta, &changes, if_exists)?;

            let current_version = metadata_file_version_from_path(&latest_meta).unwrap_or(0);
            let next_version = current_version + 1;
            let existing_for_next = metadata_files_for_version(&store_ctx, next_version).await?;
            if !existing_for_next.is_empty() {
                log::warn!(
                    "Detected existing Iceberg metadata files for version {}: {:?}. Retrying attempt {}",
                    next_version,
                    existing_for_next,
                    attempt
                );
                if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                    return Err(alter_table_properties_conflict_error());
                }
                continue;
            }

            let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
            table_meta.last_updated_ms = timestamp_ms;
            table_meta.metadata_log.push(MetadataLog {
                timestamp_ms,
                metadata_file: latest_meta.clone(),
            });

            let new_meta_bytes = table_meta
                .to_json()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
            let new_meta_rel = format!("metadata/v{next_version}{file_extension}");
            let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let new_meta_path = object_store::path::Path::from(new_meta_rel.as_str());
            let put_opts = object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            };
            let payload = object_store::PutPayload::from(Bytes::from(new_meta_bytes));
            match store_ctx
                .prefixed
                .put_opts(&new_meta_path, payload, put_opts)
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    log::warn!(
                        "Iceberg metadata file {} already exists for version {}. Retrying attempt {}",
                        new_meta_rel,
                        next_version,
                        attempt
                    );
                    if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                        return Err(alter_table_properties_conflict_error());
                    }
                    continue;
                }
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            }

            let version_files = metadata_files_for_version(&store_ctx, next_version).await?;
            let conflict_after_write = version_files.iter().any(|path| path != &new_meta_rel);
            if conflict_after_write {
                log::warn!(
                    "Concurrent Iceberg metadata writes detected for version {}: {:?}. Retrying attempt {}",
                    next_version,
                    version_files,
                    attempt
                );
                if let Err(err) = store_ctx.prefixed.delete(&new_meta_path).await {
                    log::warn!(
                        "Failed to delete conflicted Iceberg metadata file {}: {:?}",
                        new_meta_rel,
                        err
                    );
                }
                if attempt >= MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES {
                    return Err(alter_table_properties_conflict_error());
                }
                continue;
            }

            let hint_path = object_store::path::Path::from("metadata/version-hint.text");
            store_ctx
                .prefixed
                .put(
                    &hint_path,
                    object_store::PutPayload::from(Bytes::from(next_version.to_string())),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(());
        }
    }

    // TODO: Implement row-level DELETE/UPDATE/MERGE for this format. Expanded
    // inputs should consume Sail row intent tags to decide which rows rewrite
    // data files and which rows produce low-level delete artifacts, then strip
    // all internal metadata before writing user data.
}

/// Create an Iceberg table provider for reading.
pub async fn create_iceberg_provider(
    ctx: &dyn Session,
    table_url: Url,
    options: IcebergReadOptions,
) -> Result<Arc<dyn TableProvider>> {
    Ok(create_iceberg_provider_concrete(ctx, table_url, options, None, false).await?)
}

pub(crate) async fn create_iceberg_provider_concrete(
    ctx: &dyn Session,
    table_url: Url,
    options: IcebergReadOptions,
    metadata_location: Option<String>,
    catalog_managed_table: bool,
) -> Result<Arc<IcebergTableProvider>> {
    let metadata_location = catalog_managed_table.then_some(metadata_location).flatten();
    let table = Table::load_with_metadata_location(ctx, table_url, metadata_location).await?;
    let provider = table.to_provider(&options)?;
    Ok(Arc::new(provider))
}

async fn build_iceberg_provider(
    ctx: &dyn Session,
    info: SourceInfo,
) -> Result<Arc<IcebergTableProvider>> {
    let SourceInfo {
        paths,
        schema: _,
        constraints: _,
        partition_by: _,
        bucket_by: _,
        sort_order: _,
        options,
    } = info;

    let table_url = IcebergTableFormat::parse_table_url(paths).await?;
    let metadata_location = metadata_location_from_options(&options);
    let catalog_managed_table = catalog_managed_iceberg_from_options(&options);
    let iceberg_options = IcebergReadOptions::resolve(ctx, options)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    create_iceberg_provider_concrete(
        ctx,
        table_url,
        iceberg_options,
        metadata_location,
        catalog_managed_table,
    )
    .await
}

/// Load metadata and pick snapshot per options (precedence: snapshot_id > ref > timestamp > current).
#[expect(dead_code)]
pub(crate) async fn load_table_metadata_with_options(
    ctx: &dyn Session,
    table_url: &Url,
    options: IcebergReadOptions,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!(
        "Loading table metadata (with options) from: {}, options: {:?}",
        table_url,
        options
    );
    let table = Table::load(ctx, table_url.clone()).await?;
    table.scan_state(&options)
}

impl IcebergTableFormat {
    async fn parse_table_url(paths: Vec<String>) -> Result<Url> {
        if paths.len() != 1 {
            return plan_err!(
                "Iceberg table requires exactly one path, got {}",
                paths.len()
            );
        }

        let path = &paths[0];
        let mut table_url = Url::parse(path).map_err(|e| DataFusionError::External(Box::new(e)))?;

        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }
        Ok(table_url)
    }

    fn partition_columns_from_metadata(table: &Table) -> Result<Vec<CatalogPartitionField>> {
        let metadata = table.metadata();
        let spec = match metadata.default_partition_spec() {
            Some(spec) => spec,
            None => return Ok(vec![]),
        };
        if spec.is_unpartitioned() {
            return Ok(vec![]);
        }

        let schema = metadata.current_schema().ok_or_else(|| {
            DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
        })?;

        let mut columns = Vec::with_capacity(spec.fields().len());
        for field in spec.fields() {
            let col_name = schema
                .field_by_id(field.source_id)
                .map(|f| f.name.clone())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Partition field references unknown source column id {}",
                        field.source_id
                    ))
                })?;
            columns.push(
                catalog_partition_field_from_iceberg(col_name, field.transform)
                    .map_err(DataFusionError::Plan)?,
            );
        }

        Ok(columns)
    }
}

pub(crate) fn metadata_location_from_properties(properties: &[(String, String)]) -> Option<String> {
    metadata_location_value(
        properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .map(ToString::to_string)
}

pub(crate) fn metadata_location_from_options(options: &[OptionLayer]) -> Option<String> {
    options.iter().rev().find_map(|layer| match layer {
        OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } => {
            metadata_location_from_properties(items)
        }
        _ => None,
    })
}

pub(crate) fn catalog_managed_iceberg_from_properties(properties: &[(String, String)]) -> bool {
    properties.iter().any(|(key, value)| {
        let key = key.trim();
        is_iceberg_table_marker(key, value.trim()) || key.starts_with("metadata.")
    })
}

pub(crate) fn catalog_managed_iceberg_from_options(options: &[OptionLayer]) -> bool {
    options.iter().any(|layer| match layer {
        OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } => {
            catalog_managed_iceberg_from_properties(items)
        }
        _ => false,
    })
}

pub(crate) fn catalog_table_from_properties(
    properties: &[(String, String)],
) -> Result<Option<Vec<String>>> {
    properties
        .iter()
        .rev()
        .find(|(key, _)| key == CATALOG_TABLE_OPTION)
        .map(|(_, value)| {
            serde_json::from_str::<Vec<String>>(value)
                .map_err(|e| DataFusionError::Plan(format!("invalid catalog table reference: {e}")))
        })
        .transpose()
}

fn catalog_table_from_options(options: &[OptionLayer]) -> Result<Option<Vec<String>>> {
    for layer in options.iter().rev() {
        let OptionLayer::OptionList { items } = layer else {
            continue;
        };
        if let Some(table) = catalog_table_from_properties(items)? {
            return Ok(Some(table));
        }
    }
    Ok(None)
}

fn split_iceberg_write_options_and_table_properties(
    options: Vec<OptionLayer>,
) -> (Vec<OptionLayer>, Vec<(String, String)>) {
    let mut table_properties = Vec::new();
    let clean_options = options
        .into_iter()
        .inspect(|layer| {
            if let OptionLayer::TablePropertyList { items } = layer {
                // Catalog-encoded OPTIONS are stored as `option.*` table properties.
                // Keep them for option resolution, but do not commit them to Iceberg metadata.
                table_properties.extend(
                    items
                        .iter()
                        .filter(|(key, _)| !key.starts_with("option."))
                        .cloned(),
                );
            }
        })
        .collect();
    (clean_options, table_properties)
}

fn alter_table_properties_conflict_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg ALTER TABLE SET/UNSET TBLPROPERTIES failed after {MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_iceberg_write_options_keeps_catalog_options_out_of_table_properties() {
        let options = vec![
            OptionLayer::TablePropertyList {
                items: vec![
                    ("option.metadataAsDataRead".to_string(), "true".to_string()),
                    ("write.data.path".to_string(), "custom_data".to_string()),
                    (
                        "write.folder-storage.path".to_string(),
                        "legacy_data".to_string(),
                    ),
                    ("custom.key".to_string(), "custom-value".to_string()),
                ],
            },
            OptionLayer::OptionList {
                items: vec![
                    ("mergeSchema".to_string(), "true".to_string()),
                    ("path".to_string(), "/tmp/table".to_string()),
                ],
            },
        ];

        let (clean_options, table_properties) =
            split_iceberg_write_options_and_table_properties(options);

        assert_eq!(
            table_properties,
            vec![
                ("write.data.path".to_string(), "custom_data".to_string()),
                (
                    "write.folder-storage.path".to_string(),
                    "legacy_data".to_string(),
                ),
                ("custom.key".to_string(), "custom-value".to_string()),
            ]
        );
        let ctx = datafusion::execution::context::SessionContext::default();
        let state = ctx.state();
        #[expect(clippy::unwrap_used)]
        let iceberg_options = IcebergWriteOptions::resolve(&state, clean_options).unwrap();
        assert!(iceberg_options.merge_schema);
        assert_eq!(
            iceberg_options.write_data_path.as_deref(),
            Some("custom_data")
        );
        assert_eq!(
            iceberg_options.write_folder_storage_path.as_deref(),
            Some("legacy_data")
        );
    }

    #[test]
    fn catalog_managed_iceberg_detection_requires_marker_or_metadata_summary() {
        assert!(!catalog_managed_iceberg_from_properties(&[(
            "metadata-location".to_string(),
            "file:///tmp/table/metadata/v1.metadata.json".to_string(),
        )]));
        assert!(catalog_managed_iceberg_from_properties(&[(
            "table_type".to_string(),
            "ICEBERG".to_string(),
        )]));
        assert!(catalog_managed_iceberg_from_properties(&[(
            "metadata.table-uuid".to_string(),
            "9f7c2fc5-2e7d-4a6a-b3f9-0f6a47a3522c".to_string(),
        )]));
    }
}
