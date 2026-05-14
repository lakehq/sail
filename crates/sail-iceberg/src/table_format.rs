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
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::{
    find_path_in_options, OptionLayer, PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat,
    TableFormatRegistry,
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
    metadata_file_version_from_path,
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

    async fn create_table(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        info: sail_common_datafusion::datasource::CreateTableInfo,
    ) -> Result<()> {
        let sail_common_datafusion::datasource::CreateTableInfo {
            path,
            schema,
            partition_by,
            mut properties,
            operation,
            generated_columns: _,
        } = info;

        let table_url = Self::parse_table_url(vec![path]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

        let existing_metadata = match find_latest_metadata_file(&object_store, &table_url).await {
            Ok(path) => Some(path),
            Err(DataFusionError::Plan(message))
                if message.contains("No metadata files found in table location") =>
            {
                None
            }
            Err(e) => return Err(e),
        };

        match (&existing_metadata, operation) {
            (Some(_), operation) if operation.is_if_not_exists() => return Ok(()),
            (Some(_), operation)
                if !operation.replaces_existing_storage() && schema.fields().is_empty() =>
            {
                return Ok(());
            }
            (Some(_), operation) if !operation.replaces_existing_storage() => {
                return plan_err!("Iceberg table already exists at path: {table_url}");
            }
            _ => {}
        }

        if schema.fields().is_empty() {
            return plan_err!("Iceberg CREATE TABLE requires a non-empty schema");
        }

        let mut iceberg_schema =
            crate::datasource::type_converter::arrow_schema_to_iceberg(schema.as_ref())?;
        iceberg_schema =
            crate::schema_evolution::SchemaEvolver::assign_schema_field_ids(&iceberg_schema)?;

        let mut partition_spec_builder = PartitionSpec::builder();
        for field in &partition_by {
            let source_id = iceberg_schema
                .field_id_by_name(&field.column)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Partition column mismatch: column '{}' not found in schema",
                        crate::utils::partition_transform::format_partition_expr(field)
                    ))
                })?;
            partition_spec_builder = partition_spec_builder.add_field(
                source_id,
                crate::utils::partition_transform::partition_field_name(field),
                crate::utils::partition_transform::iceberg_transform_from_partition_field(field),
            );
        }
        let partition_spec = partition_spec_builder.build();

        let mut table_properties = properties
            .drain()
            .filter(|(key, _)| !is_catalog_encoded_option(key))
            .collect::<Vec<_>>();
        table_properties.sort_by(|(a, _), (b, _)| a.cmp(b));
        let (requested_format_version, metadata_properties) =
            crate::properties::metadata_properties_from_table_properties(&table_properties)?;
        let format_version = requested_format_version.max(
            crate::operations::helpers::format_version_for_schema(&iceberg_schema),
        );

        let current_version = existing_metadata
            .as_deref()
            .and_then(metadata_file_version_from_path)
            .unwrap_or(0);
        let next_version = current_version + 1;
        let existing_for_next = metadata_files_for_version(&store_ctx, next_version).await?;
        if !existing_for_next.is_empty() {
            return plan_err!(
                "Iceberg metadata file for version {next_version} already exists at {table_url}"
            );
        }

        let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
        let mut table_metadata = TableMetadata {
            format_version,
            table_uuid: None,
            location: table_url.to_string(),
            last_sequence_number: 0,
            last_updated_ms: timestamp_ms,
            last_column_id: iceberg_schema.highest_field_id(),
            schemas: vec![iceberg_schema.clone()],
            current_schema_id: iceberg_schema.schema_id(),
            partition_specs: vec![partition_spec.clone()],
            default_spec_id: partition_spec.spec_id(),
            last_partition_id: partition_spec.highest_field_id().unwrap_or(0),
            properties: metadata_properties,
            current_snapshot_id: None,
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: existing_metadata
                .iter()
                .map(|metadata_file| MetadataLog {
                    timestamp_ms,
                    metadata_file: metadata_file.clone(),
                })
                .collect(),
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: Default::default(),
            statistics: vec![],
            partition_statistics: vec![],
        };
        table_metadata.ensure_required_format_fields();

        let metadata_json = table_metadata
            .to_json()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let file_extension = metadata_file_extension_from_properties(&table_metadata.properties)?;
        let metadata_rel = format!("metadata/v{next_version}{file_extension}");
        let metadata_bytes = encode_metadata_file(&metadata_rel, &metadata_json)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let metadata_path = object_store::path::Path::from(metadata_rel.as_str());
        let put_opts = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..Default::default()
        };
        store_ctx
            .prefixed
            .put_opts(
                &metadata_path,
                object_store::PutPayload::from(Bytes::from(metadata_bytes)),
                put_opts,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let hint_path = object_store::path::Path::from("metadata/version-hint.text");
        store_ctx
            .prefixed
            .put(
                &hint_path,
                object_store::PutPayload::from(Bytes::from(next_version.to_string())),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
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
            logical_schema: _,
        } = info;

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Iceberg format");
        }

        let table_url = Self::parse_table_url(vec![path]).await?;
        let (options, table_properties) = split_iceberg_write_options_and_table_properties(options);
        let iceberg_options = IcebergWriteOptions::resolve(ctx, options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let exists_res = find_latest_metadata_file(&store, &table_url).await;
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
            let table = Table::load(ctx, table_url.clone()).await?;
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
        options.table_properties = table_properties;
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

        let builder = IcebergPlanBuilder::new(input, table_config, mode, physical_sort, ctx);
        let exec = builder.build().await?;
        Ok(exec)
    }

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
    Ok(create_iceberg_provider_concrete(ctx, table_url, options).await?)
}

pub(crate) async fn create_iceberg_provider_concrete(
    ctx: &dyn Session,
    table_url: Url,
    options: IcebergReadOptions,
) -> Result<Arc<IcebergTableProvider>> {
    let table = Table::load(ctx, table_url).await?;
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
    let iceberg_options = IcebergReadOptions::resolve(ctx, options)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    create_iceberg_provider_concrete(ctx, table_url, iceberg_options).await
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
                        .filter(|(key, _)| !is_catalog_encoded_option(key))
                        .cloned(),
                );
            }
        })
        .collect();
    (clean_options, table_properties)
}

fn is_catalog_encoded_option(key: &str) -> bool {
    key.starts_with("option.")
}

fn alter_table_properties_conflict_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg ALTER TABLE SET/UNSET TBLPROPERTIES failed after {MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}

#[cfg(test)]
mod tests {
    use sail_common_datafusion::datasource::CreateTableOperation;

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
    fn create_table_materializes_metadata_only_iceberg_table() -> Result<()> {
        futures::executor::block_on(async {
            let unique = format!(
                "sail-iceberg-create-table-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .as_nanos()
            );
            let dir = std::env::temp_dir().join(unique);
            std::fs::create_dir_all(&dir).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let table_url = Url::from_directory_path(&dir).map_err(|()| {
                DataFusionError::Plan(format!("invalid temp directory path: {}", dir.display()))
            })?;

            let ctx = datafusion::execution::context::SessionContext::default();
            IcebergTableFormat
                .create_table(
                    ctx.runtime_env(),
                    sail_common_datafusion::datasource::CreateTableInfo {
                        path: table_url.to_string(),
                        schema: Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                            datafusion::arrow::datatypes::Field::new(
                                "id",
                                datafusion::arrow::datatypes::DataType::Int64,
                                false,
                            ),
                        ])),
                        partition_by: vec![CatalogPartitionField {
                            column: "id".to_string(),
                            transform: Some(
                                sail_common_datafusion::catalog::PartitionTransform::Bucket(8),
                            ),
                        }],
                        properties: std::collections::HashMap::from([
                            ("format-version".to_string(), "2".to_string()),
                            ("option.metadataAsDataRead".to_string(), "true".to_string()),
                            ("custom.key".to_string(), "custom-value".to_string()),
                        ]),
                        operation: CreateTableOperation::Create,
                        generated_columns: std::collections::HashMap::new(),
                    },
                )
                .await?;

            let state = ctx.state();
            let table = Table::load(&state, table_url.clone()).await?;
            let metadata = table.metadata();
            assert!(metadata.current_snapshot_id.is_none());
            assert!(metadata.snapshots.is_empty());
            assert_eq!(metadata.schemas.len(), 1);
            let schema = metadata.current_schema().ok_or_else(|| {
                DataFusionError::Plan("missing current schema after CREATE TABLE".to_string())
            })?;
            assert_eq!(schema.fields().len(), 1);
            let spec = metadata.default_partition_spec().ok_or_else(|| {
                DataFusionError::Plan(
                    "missing default partition spec after CREATE TABLE".to_string(),
                )
            })?;
            assert_eq!(spec.fields().len(), 1);
            assert_eq!(spec.fields()[0].name, "id_bucket");
            assert!(!metadata
                .properties
                .contains_key("option.metadataAsDataRead"));
            assert_eq!(
                metadata.properties.get("custom.key").map(String::as_str),
                Some("custom-value")
            );

            std::fs::remove_dir_all(&dir).map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(())
        })
    }

    #[test]
    fn create_table_if_not_exists_preserves_existing_iceberg_metadata() -> Result<()> {
        futures::executor::block_on(async {
            let unique = format!(
                "sail-iceberg-create-table-if-not-exists-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .as_nanos()
            );
            let dir = std::env::temp_dir().join(unique);
            std::fs::create_dir_all(&dir).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let table_url = Url::from_directory_path(&dir).map_err(|()| {
                DataFusionError::Plan(format!("invalid temp directory path: {}", dir.display()))
            })?;
            let ctx = datafusion::execution::context::SessionContext::default();
            let create_info = |schema: datafusion::arrow::datatypes::Schema| {
                sail_common_datafusion::datasource::CreateTableInfo {
                    path: table_url.to_string(),
                    schema: Arc::new(schema),
                    partition_by: vec![],
                    properties: std::collections::HashMap::new(),
                    operation: CreateTableOperation::Create,
                    generated_columns: std::collections::HashMap::new(),
                }
            };

            IcebergTableFormat
                .create_table(
                    ctx.runtime_env(),
                    create_info(datafusion::arrow::datatypes::Schema::new(vec![
                        datafusion::arrow::datatypes::Field::new(
                            "id",
                            datafusion::arrow::datatypes::DataType::Int64,
                            false,
                        ),
                    ])),
                )
                .await?;

            let object_store = ctx
                .runtime_env()
                .object_store_registry
                .get_store(&table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let first_metadata = find_latest_metadata_file(&object_store, &table_url).await?;

            IcebergTableFormat
                .create_table(
                    ctx.runtime_env(),
                    create_info(datafusion::arrow::datatypes::Schema::empty()),
                )
                .await?;
            let adopted_metadata = find_latest_metadata_file(&object_store, &table_url).await?;
            assert_eq!(first_metadata, adopted_metadata);

            let mut second_info = create_info(datafusion::arrow::datatypes::Schema::new(vec![
                datafusion::arrow::datatypes::Field::new(
                    "different",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    true,
                ),
            ]));
            second_info.operation = CreateTableOperation::CreateIfNotExists;
            IcebergTableFormat
                .create_table(ctx.runtime_env(), second_info)
                .await?;

            let second_metadata = find_latest_metadata_file(&object_store, &table_url).await?;
            assert_eq!(first_metadata, second_metadata);

            std::fs::remove_dir_all(&dir).map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(())
        })
    }
}
