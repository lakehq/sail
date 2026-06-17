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

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, Extension, UserDefinedLogicalNodeCore};
use educe::Educe;
use object_store::ObjectStoreExt;
use sail_common_datafusion::catalog::iceberg::is_iceberg_table_marker;
use sail_common_datafusion::catalog::managed::metadata_location_value;
use sail_common_datafusion::catalog::{
    CatalogPartitionField, CommitAuthority, LakehouseExecutionContext, ScanAuthority,
};
use sail_common_datafusion::datasource::{
    create_sort_order, find_path_in_options, BucketBy, OptionLayer, PhysicalSinkMode, SinkInfo,
    SinkMode, SourceInfo, TableFormat, TableFormatAlterTableOperation,
    TableFormatCreateTableColumn, TableFormatCreateTableInfo, TableFormatCreateTableResult,
    TableFormatRegistry,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::with_variant_extension_if_marked_storage;
use sail_data_source::options::ResolveOptions;
use url::Url;

use crate::datasource::provider::IcebergTableProvider;
use crate::datasource::type_converter::{
    arrow_schema_to_iceberg, iceberg_schema_to_arrow, ICEBERG_ARROW_FIELD_DOC_KEY,
};
use crate::io::StoreContext;
use crate::logical::IcebergTableSource;
use crate::operations::bootstrap::{bootstrap_empty_table_metadata, NewTableMetadataStyle};
use crate::options::gen::{IcebergReadOptions, IcebergWriteOptions};
use crate::physical_plan::plan_builder::{IcebergPlanBuilder, IcebergTableConfig};
use crate::physical_plan::IcebergWriterExecOptions;
use crate::schema_evolution::SchemaEvolver;
use crate::spec::{MetadataLog, PartitionSpec, Schema, Snapshot, TableMetadata};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path, metadata_location_to_object_path_string,
};
use crate::table::{find_latest_metadata_file, Table};
use crate::utils::metadata::metadata_files_for_version;
use crate::utils::partition_transform::{
    catalog_partition_field_from_iceberg, format_partition_expr, format_partition_exprs,
    iceberg_transform_from_partition_field, partition_field_name,
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

    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<datafusion::arrow::datatypes::SchemaRef> {
        Ok(self.create_source(ctx, info).await?.schema())
    }

    async fn infer_metadata(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<sail_common_datafusion::datasource::TableFormatMetadata> {
        Ok(sail_common_datafusion::datasource::TableFormatMetadata {
            schema: self.infer_schema(ctx, info).await?,
            properties: vec![],
        })
    }

    async fn create_writer(&self, _ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
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
            lakehouse_table,
        } = info;
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Iceberg format");
        }

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(IcebergWriteNode::new(
                Arc::new(input),
                IcebergWriteNodeOptions {
                    path,
                    mode,
                    partition_by,
                    bucket_by,
                    sort_order,
                    options,
                    lakehouse_table,
                },
            )),
        }))
    }

    async fn create_table_metadata(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        info: TableFormatCreateTableInfo,
    ) -> Result<TableFormatCreateTableResult> {
        let TableFormatCreateTableInfo {
            path,
            columns,
            comment: _,
            partition_by,
            properties,
            replace,
            lakehouse_table,
        } = info;
        let catalog_table = lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table().to_vec());

        let table_url = Self::parse_table_url(vec![path]).await?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        match find_latest_metadata_file(&object_store, &table_url).await {
            Ok(metadata_file) if columns.is_empty() => {
                let metadata_location = table_metadata_location(&table_url, &metadata_file)?;
                return Ok(TableFormatCreateTableResult {
                    properties: vec![(
                        sail_common_datafusion::catalog::managed::METADATA_LOCATION_UNDERSCORE_KEY
                            .to_string(),
                        metadata_location,
                    )],
                });
            }
            Ok(metadata_file) if replace => {
                let declared_schema = create_table_arrow_schema(columns)?;
                let metadata_data = load_metadata_file_bytes(&object_store, &metadata_file).await?;
                let metadata = TableMetadata::from_json(&metadata_data)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                validate_existing_iceberg_create_table_metadata(
                    &metadata,
                    &declared_schema,
                    &partition_by,
                    table_url.as_str(),
                )?;
                let metadata_location = table_metadata_location(&table_url, &metadata_file)?;
                return Ok(TableFormatCreateTableResult {
                    properties: vec![(
                        sail_common_datafusion::catalog::managed::METADATA_LOCATION_UNDERSCORE_KEY
                            .to_string(),
                        metadata_location,
                    )],
                });
            }
            Ok(_) => {
                return plan_err!("Iceberg table metadata already exists at path: {table_url}");
            }
            Err(err)
                if err.to_string().contains("No metadata files found") && columns.is_empty() =>
            {
                return plan_err!("Iceberg CREATE TABLE requires at least one column");
            }
            Err(err) if err.to_string().contains("No metadata files found") => {}
            Err(err) => return Err(err),
        }

        let arrow_schema = create_table_arrow_schema(columns)?;
        let mut iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;
        iceberg_schema = SchemaEvolver::assign_schema_field_ids(&iceberg_schema)?;
        if iceberg_schema.fields().iter().any(|field| field.id == 0) {
            return plan_err!("Invalid Iceberg schema: field id 0 detected after assignment");
        }

        let mut partition_spec_builder = PartitionSpec::builder();
        for field in &partition_by {
            let source_id = iceberg_schema
                .field_id_by_name(&field.column)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Partition column mismatch: column '{}' not found in schema",
                        format_partition_expr(field)
                    ))
                })?;
            partition_spec_builder = partition_spec_builder.add_field(
                source_id,
                partition_field_name(field),
                iceberg_transform_from_partition_field(field),
            );
        }
        let partition_spec = partition_spec_builder.build();
        let table_properties = iceberg_table_properties_from_catalog_create(properties)?;
        let store_ctx = StoreContext::new(object_store, &table_url)?;
        let metadata_style = if catalog_table.is_some() {
            NewTableMetadataStyle::Uuid
        } else {
            NewTableMetadataStyle::Hadoop
        };
        let bootstrap = bootstrap_empty_table_metadata(
            &table_url,
            &store_ctx,
            iceberg_schema,
            partition_spec,
            &table_properties,
            metadata_style,
        )
        .await?;
        let metadata_location = table_url
            .join(&bootstrap.metadata_file)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .to_string();

        Ok(TableFormatCreateTableResult {
            properties: vec![(
                sail_common_datafusion::catalog::managed::METADATA_LOCATION_UNDERSCORE_KEY
                    .to_string(),
                metadata_location,
            )],
        })
    }

    async fn alter_table(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        operation: TableFormatAlterTableOperation,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<()> {
        reject_catalog_managed_iceberg_alter(lakehouse_table.as_ref())?;
        match operation {
            TableFormatAlterTableOperation::SetTableProperties { changes, if_exists } => {
                self.alter_table_properties(runtime_env, path, changes, if_exists)
                    .await
            }
            op => not_impl_err!("unsupported Iceberg ALTER TABLE operation: {op:?}"),
        }
    }
}

fn reject_catalog_managed_iceberg_alter(
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    if context.commit != CommitAuthority::Filesystem {
        return not_impl_err!(
            "ALTER TABLE is not yet supported for catalog-managed Iceberg tables: {}",
            context.catalog_table().join(".")
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct IcebergWriteNodeOptions {
    pub path: String,
    pub mode: SinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Vec<Sort>,
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct IcebergWriteNode {
    input: Arc<LogicalPlan>,
    options: IcebergWriteNodeOptions,
    #[educe(PartialOrd(ignore))]
    schema: datafusion_common::DFSchemaRef,
}

impl IcebergWriteNode {
    pub fn new(input: Arc<LogicalPlan>, options: IcebergWriteNodeOptions) -> Self {
        Self {
            input,
            options,
            schema: Arc::new(datafusion_common::DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &IcebergWriteNodeOptions {
        &self.options
    }
}

impl UserDefinedLogicalNodeCore for IcebergWriteNode {
    fn name(&self) -> &str {
        "IcebergWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IcebergWrite: options={:?}", self.options)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            options: self.options.clone(),
            schema: self.schema.clone(),
        })
    }
}

pub(crate) async fn plan_iceberg_write(
    ctx: &SessionState,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    node: &IcebergWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_plan::empty::EmptyExec;

    let IcebergWriteNodeOptions {
        path,
        mode,
        partition_by,
        bucket_by: _,
        sort_order,
        options,
        lakehouse_table,
    } = node.options().clone();

    let mode = match mode {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { .. } | SinkMode::OverwritePartitions => {
            return not_impl_err!("predicate or partition overwrite for Iceberg");
        }
    };
    validate_iceberg_lakehouse_storage_access(lakehouse_table.as_ref())?;
    let metadata_location = metadata_location_from_options(&options);
    let catalog_managed_table = catalog_managed_iceberg_from_options(&options);
    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(options)?;
    let variant_shredding_option_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let iceberg_options = IcebergWriteOptions::resolve(ctx, clean_options)?;

    let sort_order = create_sort_order(ctx, sort_order, logical_input.schema())?;
    let physical_sort = sort_order.map(|req| {
        req.into_iter()
            .map(|r| datafusion::physical_expr::PhysicalSortExpr {
                expr: r.expr,
                options: r.options.unwrap_or_default(),
            })
            .collect::<Vec<_>>()
    });

    let table_url = IcebergTableFormat::parse_table_url(vec![path]).await?;

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
            return Ok(Arc::new(EmptyExec::new(physical_input.schema())));
        }
        PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
            return not_impl_err!("predicate or partition overwrite for Iceberg");
        }
        _ => {}
    }

    let existing_partition_columns = if table_exists {
        let metadata_location = catalog_managed_table.then_some(metadata_location).flatten();
        let table =
            Table::load_with_metadata_location(ctx, table_url.clone(), metadata_location).await?;
        Some(IcebergTableFormat::partition_columns_from_metadata(&table)?)
    } else {
        None
    };

    if let Some(existing_partitions) = &existing_partition_columns {
        if !partition_by.is_empty() && partition_by != *existing_partitions {
            match mode {
                PhysicalSinkMode::Append => {
                    return plan_err!(
                        "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                        Cannot change partitioning on append.",
                        format_partition_exprs(existing_partitions),
                        format_partition_exprs(&partition_by)
                    );
                }
                PhysicalSinkMode::Overwrite if !iceberg_options.overwrite_schema => {
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
    options.lakehouse_table = lakehouse_table;
    let table_config = IcebergTableConfig {
        table_url,
        partition_columns: resolved_partition_columns,
        table_exists,
        options,
    };

    let logical_input_schema = Arc::new(logical_input.schema().as_arrow().clone());
    let builder = IcebergPlanBuilder::new(
        physical_input,
        table_config,
        mode,
        physical_sort,
        Some(logical_input_schema),
        ctx,
    );
    builder.build().await
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

pub async fn create_iceberg_provider_concrete(
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
        lakehouse_table,
        schema: _,
        constraints: _,
        partition_by: _,
        bucket_by: _,
        sort_order: _,
        options,
        read_case_sensitive: _,
    } = info;

    validate_iceberg_read_lakehouse_context(lakehouse_table.as_ref())?;
    let table_url = IcebergTableFormat::parse_table_url(paths).await?;
    let metadata_location = metadata_location_from_options(&options);
    let catalog_managed_table = catalog_managed_iceberg_from_options(&options);
    let iceberg_options = IcebergReadOptions::resolve(ctx, options)?;
    create_iceberg_provider_concrete(
        ctx,
        table_url,
        iceberg_options,
        metadata_location,
        catalog_managed_table,
    )
    .await
}

fn validate_iceberg_read_lakehouse_context(
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    validate_iceberg_lakehouse_storage_access(Some(context))?;
    if context.scan == ScanAuthority::IcebergRestServerSide {
        // TODO: Implement Iceberg REST server-side scan planning sessions before
        // allowing server-mode tables to fall through to client-side storage access.
        return not_impl_err!(
            "Iceberg REST catalog table {} requires server-side scan planning, which is not implemented yet",
            context.catalog_table().join(".")
        );
    }
    Ok(())
}

fn validate_iceberg_lakehouse_storage_access(
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    if context
        .rest_session
        .as_ref()
        .is_some_and(|session| session.remote_signing_enabled)
    {
        // TODO: Wire REST remote signing into Iceberg FileIO/object-store access.
        return not_impl_err!(
            "Iceberg REST catalog table {} requires remote signing, which is not implemented yet",
            context.catalog_table().join(".")
        );
    }
    if context
        .rest_session
        .as_ref()
        .is_some_and(|session| session.storage_credential_count > 0)
    {
        // TODO: Apply REST vended credentials to operation-scoped storage access.
        return not_impl_err!(
            "Iceberg REST catalog table {} requires vended storage credentials, which is not implemented yet",
            context.catalog_table().join(".")
        );
    }
    Ok(())
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
    pub async fn parse_table_url(paths: Vec<String>) -> Result<Url> {
        if paths.len() != 1 {
            return plan_err!(
                "Iceberg table requires exactly one path, got {}",
                paths.len()
            );
        }

        let path = &paths[0];
        let mut table_url = if let Ok(url) = Url::parse(path) {
            if url.scheme().len() > 1 {
                url
            } else {
                return plan_err!("Iceberg table location must be an absolute path or URL: {path}");
            }
        } else if Path::new(path).is_absolute() {
            Url::from_file_path(path)
                .map_err(|_| DataFusionError::Plan(format!("invalid file path: {path}")))?
        } else {
            return plan_err!("Iceberg table location must be an absolute path or URL: {path}");
        };

        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }
        Ok(table_url)
    }

    fn partition_columns_from_metadata(table: &Table) -> Result<Vec<CatalogPartitionField>> {
        partition_columns_from_table_metadata(table.metadata())
    }
}

fn partition_columns_from_table_metadata(
    metadata: &TableMetadata,
) -> Result<Vec<CatalogPartitionField>> {
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

fn create_table_arrow_schema(columns: Vec<TableFormatCreateTableColumn>) -> Result<ArrowSchema> {
    let fields = columns
        .into_iter()
        .map(
            |TableFormatCreateTableColumn {
                 name,
                 data_type,
                 nullable,
                 comment,
                 default,
                 generated_always_as,
                 identity,
             }| {
                if default.is_some() {
                    return not_impl_err!("column DEFAULT in Iceberg CREATE TABLE");
                }
                if generated_always_as.is_some() {
                    return not_impl_err!("generated columns in Iceberg CREATE TABLE");
                }
                if identity.is_some() {
                    return not_impl_err!("identity columns in Iceberg CREATE TABLE");
                }
                let mut field = ArrowField::new(name, data_type, nullable);
                if let Some(comment) = comment {
                    field = field.with_metadata(std::collections::HashMap::from([(
                        ICEBERG_ARROW_FIELD_DOC_KEY.to_string(),
                        comment,
                    )]));
                }
                field = with_variant_extension_if_marked_storage(field);
                Ok(field)
            },
        )
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrowSchema::new(fields))
}

fn validate_existing_iceberg_create_table_metadata(
    metadata: &TableMetadata,
    declared: &ArrowSchema,
    partition_by: &[CatalogPartitionField],
    path: &str,
) -> Result<()> {
    let existing = metadata.current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
    let existing = iceberg_schema_to_arrow(existing)?;
    if existing.fields().len() != declared.fields().len() {
        return plan_err!(
            "Iceberg table already exists at {path} with a different schema: \
             existing has {} fields, declared has {} fields",
            existing.fields().len(),
            declared.fields().len()
        );
    }
    for (existing_field, declared_field) in existing.fields().iter().zip(declared.fields().iter()) {
        if existing_field.name() != declared_field.name()
            || existing_field.data_type() != declared_field.data_type()
            || existing_field.is_nullable() != declared_field.is_nullable()
        {
            return plan_err!(
                "Iceberg table already exists at {path} with a different schema for field '{}': \
                 existing {:?} nullable={}, declared {:?} nullable={}",
                declared_field.name(),
                existing_field.data_type(),
                existing_field.is_nullable(),
                declared_field.data_type(),
                declared_field.is_nullable()
            );
        }
    }

    let existing_partitions = partition_columns_from_table_metadata(metadata)?;
    if !partition_by.is_empty() && partition_by != existing_partitions.as_slice() {
        return plan_err!(
            "Iceberg table already exists at {path} with different partition columns: \
             existing {:?}, declared {:?}",
            format_partition_exprs(&existing_partitions),
            format_partition_exprs(partition_by)
        );
    }
    Ok(())
}

fn table_metadata_location(table_url: &Url, metadata_file: &str) -> Result<String> {
    if Url::parse(metadata_file).is_ok() {
        return Ok(metadata_file.to_string());
    }

    let base_path = crate::utils::url_to_object_path(table_url)?.to_string();
    let metadata_file = metadata_file.trim_start_matches('/');
    let relative_metadata_file = metadata_file
        .strip_prefix(&base_path)
        .and_then(|path| path.strip_prefix('/'))
        .unwrap_or(metadata_file);

    Ok(table_url
        .join(relative_metadata_file)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string())
}

fn iceberg_table_properties_from_catalog_create(
    properties: Vec<(String, String)>,
) -> Result<Vec<(String, String)>> {
    let catalog_table_option = sail_common_datafusion::datasource::CATALOG_TABLE_OPTION;
    if properties
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case(catalog_table_option))
    {
        return plan_err!(
            "Iceberg table property `{catalog_table_option}` is reserved for internal use"
        );
    }
    Ok(properties
        .into_iter()
        .filter(|(key, _)| !key.starts_with("option."))
        .collect())
}

pub(crate) fn metadata_location_from_properties(properties: &[(String, String)]) -> Option<String> {
    metadata_location_value(
        properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .map(ToString::to_string)
}

pub fn metadata_location_from_options(options: &[OptionLayer]) -> Option<String> {
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

pub fn catalog_managed_iceberg_from_options(options: &[OptionLayer]) -> bool {
    options.iter().any(|layer| match layer {
        OptionLayer::TablePropertyList { items } | OptionLayer::OptionList { items } => {
            catalog_managed_iceberg_from_properties(items)
        }
        _ => false,
    })
}

#[expect(clippy::type_complexity)]
pub fn split_iceberg_write_options_and_table_properties(
    options: Vec<OptionLayer>,
) -> Result<(Vec<OptionLayer>, Vec<(String, String)>)> {
    let catalog_table_option = sail_common_datafusion::datasource::CATALOG_TABLE_OPTION;
    let mut table_properties = Vec::new();
    let clean_options = options
        .into_iter()
        .map(|layer| match layer {
            OptionLayer::OptionList { items } => {
                if items
                    .iter()
                    .any(|(key, _)| key.eq_ignore_ascii_case(catalog_table_option))
                {
                    return plan_err!(
                        "Iceberg write option `{catalog_table_option}` is reserved for internal use"
                    );
                }
                Ok(Some(OptionLayer::OptionList { items }))
            }
            OptionLayer::TablePropertyList { items } => {
                if items
                    .iter()
                    .any(|(key, _)| key.eq_ignore_ascii_case(catalog_table_option))
                {
                    return plan_err!(
                        "Iceberg table property `{catalog_table_option}` is reserved for internal use"
                    );
                }
                // Catalog-encoded OPTIONS are stored as `option.*` table properties.
                // Keep them for option resolution, but do not commit them to Iceberg metadata.
                table_properties.extend(
                    items
                        .iter()
                        .filter(|(key, _)| !key.starts_with("option."))
                        .cloned(),
                );
                Ok(Some(OptionLayer::TablePropertyList { items }))
            }
            other => Ok(Some(other)),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok((clean_options, table_properties))
}

fn alter_table_properties_conflict_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg ALTER TABLE SET/UNSET TBLPROPERTIES failed after {MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}

#[cfg(test)]
mod tests {
    use sail_common_datafusion::catalog::{
        CatalogProviderId, CatalogTableIdentity, CommitAuthority, IcebergRestTableSessionRef,
        LakehouseAuthority, LakehouseFormat, LakehouseOperation, MetadataPointerAuthority,
        TableLifecycle,
    };

    use super::*;

    #[test]
    fn split_iceberg_write_options_keeps_catalog_options_out_of_table_properties() -> Result<()> {
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
            split_iceberg_write_options_and_table_properties(options)?;

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
        Ok(())
    }

    #[test]
    fn catalog_table_option_is_reserved_for_iceberg_options() {
        let options = vec![OptionLayer::OptionList {
            items: vec![(
                sail_common_datafusion::datasource::CATALOG_TABLE_OPTION.to_string(),
                r#"["catalog","schema","table"]"#.to_string(),
            )],
        }];

        let result = split_iceberg_write_options_and_table_properties(options);
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("reserved for internal use")
        ));
    }

    #[test]
    fn catalog_table_option_is_reserved_for_iceberg_table_properties() {
        let options = vec![OptionLayer::TablePropertyList {
            items: vec![(
                sail_common_datafusion::datasource::CATALOG_TABLE_OPTION.to_string(),
                r#"["catalog","schema","table"]"#.to_string(),
            )],
        }];

        let result = split_iceberg_write_options_and_table_properties(options);
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("reserved for internal use")
        ));
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

    #[test]
    fn read_rejects_required_rest_server_side_scan_planning() {
        let context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("rest".to_string()),
            vec!["rest".to_string(), "db".to_string(), "tbl".to_string()],
            CatalogTableIdentity {
                table_id: Some("12345678-1234-1234-1234-123456789012".to_string()),
                table_uri: Some("s3://bucket/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::IcebergRestServerSide,
        );

        let result = validate_iceberg_read_lakehouse_context(Some(&context));
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("requires server-side scan planning")
        ));
    }

    #[test]
    fn storage_access_rejects_required_rest_remote_signing() {
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("rest".to_string()),
            vec!["rest".to_string(), "db".to_string(), "tbl".to_string()],
            CatalogTableIdentity {
                table_id: Some("12345678-1234-1234-1234-123456789012".to_string()),
                table_uri: Some("s3://bucket/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::ClientTableFormat,
        );
        context.rest_session = Some(IcebergRestTableSessionRef {
            fingerprint: "rest-session".to_string(),
            scan_planning_mode: Some("client".to_string()),
            storage_credential_count: 0,
            remote_signing_enabled: true,
        });

        let result = validate_iceberg_lakehouse_storage_access(Some(&context));
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("requires remote signing")
        ));
    }

    #[test]
    fn storage_access_rejects_required_rest_vended_credentials() {
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("rest".to_string()),
            vec!["rest".to_string(), "db".to_string(), "tbl".to_string()],
            CatalogTableIdentity {
                table_id: Some("12345678-1234-1234-1234-123456789012".to_string()),
                table_uri: Some("s3://bucket/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::ClientTableFormat,
        );
        context.rest_session = Some(IcebergRestTableSessionRef {
            fingerprint: "rest-session".to_string(),
            scan_planning_mode: Some("client".to_string()),
            storage_credential_count: 1,
            remote_signing_enabled: false,
        });

        let result = validate_iceberg_lakehouse_storage_access(Some(&context));
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("vended storage credentials")
        ));
    }
}
