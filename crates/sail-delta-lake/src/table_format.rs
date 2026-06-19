use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{not_impl_err, plan_err, DFSchema, DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, Extension, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::catalog::delta::{
    unity_table_id_value, DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY,
};
use sail_common_datafusion::catalog::{
    CatalogPartitionField, CatalogTableColumnIdentity, CommitAuthority, LakehouseExecutionContext,
    LakehouseOperation,
};
use sail_common_datafusion::column_features::{
    ColumnFeatureKey, ColumnFeatures, SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY,
};
use sail_common_datafusion::datasource::{
    create_sort_order, find_path_in_options, BucketBy, DeleteInfo, MergeInfo, OptionLayer,
    PhysicalSinkMode, SinkInfo, SinkMode, SourceInfo, TableFormat, TableFormatAlterTableOperation,
    TableFormatCreateTableColumn, TableFormatCreateTableInfo, TableFormatCreateTableResult,
    TableFormatMetadata, TableFormatRegistry, CATALOG_TABLE_OPTION,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::with_variant_extension_if_marked_storage;
use sail_data_source::options::ResolveOptions;
use sail_data_source::resolve_listing_urls;
use sail_logical_plan::merge::RowLevelWriteNode;
use url::Url;

use crate::catalog_managed::{metadata_with_catalog_managed, protocol_with_catalog_managed};
use crate::kernel::transaction::CommitBuilder;
use crate::kernel::{DeltaSnapshotConfig, SaveMode};
use crate::options::gen::{DeltaReadOptions, DeltaWriteOptions};
use crate::physical_plan::planner::{DeltaPhysicalPlanner, DeltaPlannerConfig, PlannerContext};
use crate::schema::type_widening::alter_column_type as alter_delta_column_type;
use crate::schema::{
    add_type_widening_metadata, annotate_for_column_mapping, collect_type_changes,
    compute_max_column_id, evolve_schema, format_type_change_path,
    is_supported_type_change_for_write, metadata_for_create_with_struct_type,
    normalize_delta_schema, protocol_can_write_type_widening, protocol_for_create,
    schema_has_column_defaults, schema_has_generated_columns, schema_has_identity_columns,
};
use crate::spec::{
    canonicalize_and_validate_table_properties, contains_timestampntz_arrow,
    contains_variant_arrow, route_table_property_key, ColumnMappingMode, ColumnMetadataKey,
    CommitAction, DataType as DeltaDataType, DeltaOperation, MetadataValue, Protocol, StructField,
    StructType, TableFeature, TableProperties,
};
use crate::storage::StorageConfig;
use crate::table::{
    create_delta_table_with_object_store, create_logstore_with_object_store,
    infer_delta_logical_metadata, infer_delta_logical_schema,
    load_catalog_managed_commits_for_snapshot, open_table_with_object_store_and_table_config,
    DeltaTable,
};
use crate::{create_delta_source, DeltaTableError};

/// Delta Lake implementation of [`TableFormat`].
#[derive(Debug)]
pub struct DeltaTableFormat;

impl DeltaTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))?;
        Ok(())
    }
}

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let SourceInfo {
            paths,
            lakehouse_table,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
            read_case_sensitive: _,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = DeltaReadOptions::resolve(ctx, options)?;
        create_delta_source(ctx, table_url, schema, options, lakehouse_table).await
    }

    async fn infer_schema(&self, ctx: &dyn Session, info: SourceInfo) -> Result<SchemaRef> {
        let SourceInfo {
            paths,
            lakehouse_table,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
            read_case_sensitive: _,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = DeltaReadOptions::resolve(ctx, options)?;
        infer_delta_logical_schema(ctx, table_url, schema, options, lakehouse_table).await
    }

    async fn infer_metadata(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<TableFormatMetadata> {
        let SourceInfo {
            paths,
            lakehouse_table,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
            read_case_sensitive: _,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = DeltaReadOptions::resolve(ctx, options)?;
        let (schema, properties) =
            infer_delta_logical_metadata(ctx, table_url, schema, options, lakehouse_table).await?;
        Ok(TableFormatMetadata { schema, properties })
    }

    async fn create_table_metadata(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        info: TableFormatCreateTableInfo,
    ) -> Result<TableFormatCreateTableResult> {
        let TableFormatCreateTableInfo {
            path,
            columns,
            comment,
            partition_by,
            properties,
            replace: _,
            lakehouse_table,
        } = info;
        let catalog_table = lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table().to_vec());

        let catalog_managed_table_id = if catalog_table.is_some() {
            Some(
                unity_table_id_value(
                    properties
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str())),
                )
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "catalog-managed Delta CREATE TABLE requires a Unity table id".to_string(),
                    )
                })?
                .to_string(),
            )
        } else {
            None
        };

        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for Delta CREATE TABLE");
        }

        let table_url = parse_location_to_url(&path)?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let existing_table = match open_table_with_object_store_and_table_config(
            table_url.clone(),
            object_store.clone(),
            Default::default(),
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        )
        .await
        {
            Ok(table) => Some(table),
            Err(DeltaTableError::InvalidTableLocation(_))
            | Err(DeltaTableError::FileNotFound(_)) => None,
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };

        let declared_schema = delta_create_table_arrow_schema(&columns)?;
        if let Some(table) = existing_table {
            if !declared_schema.fields().is_empty() {
                let snapshot = table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let existing_schema = snapshot
                    .arrow_schema()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                validate_existing_delta_create_table_schema(
                    existing_schema.as_ref(),
                    declared_schema.as_ref(),
                    &path,
                )?;
                let declared_partitions = partition_by
                    .iter()
                    .map(|field| field.column.clone())
                    .collect::<Vec<_>>();
                let existing_partitions = snapshot.metadata().partition_columns();
                if existing_partitions.as_slice() != declared_partitions.as_slice() {
                    return plan_err!(
                        "Delta table already exists at {path} with different partition columns: \
                         existing {existing_partitions:?}, declared {declared_partitions:?}"
                    );
                }
            }
            return Ok(TableFormatCreateTableResult::default());
        }

        if declared_schema.fields().is_empty() {
            return plan_err!(
                "Delta CREATE TABLE requires at least one column when no existing Delta metadata is present"
            );
        }

        let properties = if catalog_managed_table_id.is_some() {
            delta_catalog_managed_create_table_properties(properties)
        } else {
            properties
        };
        let metadata_configuration = delta_create_table_metadata_configuration(properties)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let normalized_schema = normalize_delta_schema(&declared_schema);
        let mut kernel_schema = StructType::try_from(normalized_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        kernel_schema = inject_generation_expressions(
            kernel_schema,
            &delta_create_table_generation_expressions(&columns),
        );
        kernel_schema = inject_default_expressions(
            kernel_schema,
            &delta_create_table_default_expressions(&columns),
        );
        kernel_schema = inject_identity_columns(
            kernel_schema,
            &delta_create_table_identity_columns(&columns),
        );

        let effective_mode = metadata_configuration
            .get("delta.columnMapping.mode")
            .and_then(|v| ColumnMappingMode::try_from(v.as_str()).ok())
            .unwrap_or_default();

        let mut configuration = metadata_configuration;
        let metadata_schema = if !matches!(effective_mode, ColumnMappingMode::None) {
            let annotated = annotate_for_column_mapping(&kernel_schema);
            configuration.insert(
                "delta.columnMapping.mode".to_string(),
                effective_mode.as_ref().to_string(),
            );
            configuration.insert(
                "delta.columnMapping.maxColumnId".to_string(),
                compute_max_column_id(&annotated).to_string(),
            );
            annotated
        } else {
            kernel_schema
        };

        let mut protocol = protocol_for_create(
            !matches!(effective_mode, ColumnMappingMode::None),
            contains_timestampntz_arrow(normalized_schema.as_ref()),
            TableProperties::from(configuration.iter()).enable_in_commit_timestamps(),
            schema_has_generated_columns(&metadata_schema),
            schema_has_column_defaults(&metadata_schema),
            schema_has_identity_columns(&metadata_schema),
            contains_variant_arrow(normalized_schema.as_ref()),
            &configuration,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if catalog_managed_table_id.is_some() {
            protocol = protocol_with_catalog_managed(&protocol);
        }

        let partition_columns = partition_by
            .iter()
            .map(|field| field.column.clone())
            .collect::<Vec<_>>();
        let mut metadata = metadata_for_create_with_struct_type(
            metadata_schema,
            partition_columns,
            Utc::now().timestamp_millis(),
            configuration,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if let Some(comment) = comment {
            metadata = metadata.with_description(comment);
        }
        if let Some(table_id) = &catalog_managed_table_id {
            metadata = metadata_with_catalog_managed(metadata, table_id);
        }

        let table = create_delta_table_with_object_store(
            table_url.clone(),
            object_store,
            Default::default(),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let operation = DeltaOperation::Create {
            mode: SaveMode::ErrorIfExists,
            location: table_url.to_string(),
            protocol: Box::new(protocol.clone()),
            metadata: Box::new(metadata.clone()),
        };

        CommitBuilder::default()
            .with_actions(vec![
                CommitAction::Protocol(protocol),
                CommitAction::Metadata(metadata),
            ])
            .build(None, table.log_store(), operation)
            .await
            .map(|_| TableFormatCreateTableResult::default())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn create_writer(&self, _ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in Delta table options");
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
            return not_impl_err!("bucketing for Delta format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for Delta format");
        }
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(DeltaWriteNode::new(
                Arc::new(input),
                DeltaWriteNodeOptions {
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

    async fn create_deleter(&self, _ctx: &dyn Session, info: DeleteInfo) -> Result<LogicalPlan> {
        let DeleteInfo {
            table_name,
            path,
            condition,
            lakehouse_table,
            options,
        } = info;
        let write_node = RowLevelWriteNode::new_delete(
            Arc::new(LogicalPlan::EmptyRelation(
                datafusion_expr::logical_plan::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::empty()),
                },
            )),
            Arc::new(DFSchema::empty()),
            condition,
            self.name().to_string(),
            path,
            table_name,
            options,
            lakehouse_table,
        );

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(write_node),
        }))
    }

    async fn create_merger(&self, _ctx: &dyn Session, info: MergeInfo) -> Result<LogicalPlan> {
        crate::logical::merge::expand_merge_node(info)
    }

    async fn alter_table(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        operation: TableFormatAlterTableOperation,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<()> {
        reject_catalog_managed_delta_alter(lakehouse_table.as_ref(), &operation)?;
        match operation {
            TableFormatAlterTableOperation::SetTableProperties { changes, if_exists } => {
                self.alter_table_properties(runtime_env, path, changes, if_exists)
                    .await
            }
            TableFormatAlterTableOperation::AlterColumnType {
                column_path,
                data_type,
            } => {
                self.alter_table_column_type(runtime_env, path, column_path, data_type)
                    .await
            }
            TableFormatAlterTableOperation::AlterColumnDefault {
                column_path,
                default,
            } => {
                self.alter_table_column_default(runtime_env, path, column_path, default)
                    .await
            }
            TableFormatAlterTableOperation::AddCheckConstraint { name, expression } => {
                self.add_check_constraint(runtime_env, path, &name, &expression)
                    .await
            }
        }
    }
}

fn reject_catalog_managed_delta_alter(
    lakehouse_table: Option<&LakehouseExecutionContext>,
    operation: &TableFormatAlterTableOperation,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    if context.commit == CommitAuthority::DeltaRatifiedCommit {
        return not_impl_err!(
            "{} is not yet supported for catalog-managed Delta tables",
            delta_alter_operation_name(operation)
        );
    }
    Ok(())
}

fn delta_alter_operation_name(operation: &TableFormatAlterTableOperation) -> &'static str {
    match operation {
        TableFormatAlterTableOperation::SetTableProperties { .. } => {
            "ALTER TABLE SET/UNSET TBLPROPERTIES"
        }
        TableFormatAlterTableOperation::AlterColumnType { .. } => "ALTER TABLE ALTER COLUMN TYPE",
        TableFormatAlterTableOperation::AlterColumnDefault { .. } => {
            "ALTER TABLE ALTER COLUMN DEFAULT"
        }
        TableFormatAlterTableOperation::AddCheckConstraint { .. } => "ALTER TABLE ADD CONSTRAINT",
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct DeltaWriteNodeOptions {
    pub path: String,
    pub mode: SinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Vec<Sort>,
    #[educe(PartialEq(ignore), Hash(ignore), PartialOrd(ignore))]
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct DeltaWriteNode {
    input: Arc<LogicalPlan>,
    options: DeltaWriteNodeOptions,
    #[educe(PartialOrd(ignore))]
    schema: datafusion_common::DFSchemaRef,
}

impl DeltaWriteNode {
    pub fn new(input: Arc<LogicalPlan>, options: DeltaWriteNodeOptions) -> Self {
        Self {
            input,
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &DeltaWriteNodeOptions {
        &self.options
    }
}

impl UserDefinedLogicalNodeCore for DeltaWriteNode {
    fn name(&self) -> &str {
        "DeltaWrite"
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
        write!(f, "DeltaWrite: options={:?}", self.options)
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

pub(crate) async fn plan_delta_write(
    ctx: &SessionState,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    node: &DeltaWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let DeltaWriteNodeOptions {
        path,
        mode,
        partition_by,
        bucket_by: _,
        sort_order,
        options,
        lakehouse_table,
    } = node.options().clone();

    if is_flow_event_schema(logical_input.schema().as_arrow()) {
        return not_impl_err!("writing streaming data to Delta table");
    }

    let mode = match mode {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { condition } => {
            let source = condition.source.clone();
            PhysicalSinkMode::OverwriteIf {
                condition: Some(condition),
                source,
            }
        }
        SinkMode::OverwritePartitions => {
            return not_impl_err!("unsupported sink mode for Delta: OverwritePartitions");
        }
    };
    let physical_sort = create_sort_order(ctx, sort_order, logical_input.schema())?;
    let partition_by = partition_by
        .into_iter()
        .map(|field| field.column)
        .collect::<Vec<_>>();

    let table_url = DeltaTableFormat::parse_table_url(ctx, vec![path]).await?;
    let (options, table_properties) = split_delta_write_options_and_table_properties(options)?;
    let delta_options = DeltaWriteOptions::resolve(ctx, options)?;

    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let lakehouse_planning_context = lakehouse_table.as_ref().filter(|context| {
        context.table_identity.table_id.is_some()
            && !matches!(
                context.operation,
                LakehouseOperation::Create | LakehouseOperation::Register
            )
    });
    let table = match open_delta_write_planning_table(
        ctx,
        table_url.clone(),
        object_store,
        lakehouse_planning_context,
    )
    .await
    {
        Ok(table) => Some(table),
        Err(DeltaTableError::InvalidTableLocation(_)) | Err(DeltaTableError::FileNotFound(_)) => {
            None
        }
        Err(err) => return Err(DataFusionError::External(Box::new(err))),
    };
    let table_exists = table.is_some();
    let table_snapshot = table
        .as_ref()
        .map(|table| {
            table
                .snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))
                .cloned()
        })
        .transpose()?;
    let table_properties = table_properties.into_iter().collect::<HashMap<_, _>>();
    let metadata_configuration = resolve_delta_metadata_configuration(&table_properties)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    match mode {
        PhysicalSinkMode::ErrorIfExists if table_exists => {
            return plan_err!("Delta table already exists at path: {table_url}");
        }
        PhysicalSinkMode::IgnoreIfExists if table_exists => {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                physical_input.schema(),
            )));
        }
        PhysicalSinkMode::OverwritePartitions => {
            return not_impl_err!("unsupported sink mode for Delta: {mode:?}");
        }
        _ => {}
    }

    let existing_partition_columns = table_snapshot
        .as_ref()
        .map(|snapshot| snapshot.metadata().partition_columns().clone());

    if let Some(existing_partitions) = &existing_partition_columns {
        if !partition_by.is_empty() && partition_by != *existing_partitions {
            match mode {
                PhysicalSinkMode::Append => {
                    return plan_err!(
                        "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                        Cannot change partitioning on append.",
                        existing_partitions,
                        partition_by
                    );
                }
                PhysicalSinkMode::OverwriteIf { .. } => {
                    return plan_err!(
                        "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                        Cannot change partitioning with replaceWhere or conditional overwrite.",
                        existing_partitions,
                        partition_by
                    );
                }
                PhysicalSinkMode::Overwrite if !delta_options.overwrite_schema => {
                    return plan_err!(
                        "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                        Set overwriteSchema=true to change partitioning.",
                        existing_partitions,
                        partition_by
                    );
                }
                _ => {}
            }
        }
    }

    let partition_columns = match (&existing_partition_columns, &mode) {
        (
            Some(existing_partitions),
            PhysicalSinkMode::Append | PhysicalSinkMode::OverwriteIf { .. },
        ) if partition_by.is_empty() => existing_partitions.clone(),
        (Some(existing_partitions), PhysicalSinkMode::Overwrite)
            if partition_by.is_empty() && !delta_options.overwrite_schema =>
        {
            existing_partitions.clone()
        }
        _ => partition_by,
    };

    let logical_schema = logical_input.schema();
    let table_config = DeltaPlannerConfig::new(
        table_url,
        delta_options,
        metadata_configuration,
        partition_columns,
        None,
        table_exists,
    )
    .with_generation_expressions(extract_generation_expressions(Some(logical_schema)))
    .with_default_expressions(extract_default_expressions(Some(logical_schema)))
    .with_target_nullability(extract_target_nullability(Some(logical_schema)))
    .with_metadata_schema(extract_metadata_schema(Some(logical_schema)))
    .with_identity_columns(extract_identity_columns(Some(logical_schema)))
    .with_table_snapshot(table_snapshot)
    .with_lakehouse_table(lakehouse_table);
    let planner_ctx = PlannerContext::new(ctx, table_config);
    let planner = DeltaPhysicalPlanner::new(planner_ctx);
    planner
        .create_plan(physical_input, mode, physical_sort)
        .await
}

async fn open_delta_write_planning_table(
    ctx: &SessionState,
    table_url: Url,
    object_store: Arc<dyn object_store::ObjectStore>,
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> std::result::Result<DeltaTable, DeltaTableError> {
    // Only partition columns and table existence are needed at planning time;
    // skip replaying Add/Remove file actions unless a later physical plan needs them.
    let mut table_config = DeltaSnapshotConfig {
        require_files: false,
        ..Default::default()
    };

    if let Some(lakehouse_table) = lakehouse_table {
        let log_store = create_logstore_with_object_store(
            Arc::clone(&object_store),
            table_url.clone(),
            StorageConfig,
        )?;
        table_config.catalog_managed_commits = load_catalog_managed_commits_for_snapshot(
            ctx,
            lakehouse_table,
            &table_url,
            log_store.clone(),
            None,
        )
        .await
        .map_err(DeltaTableError::from)?;
        let mut table = DeltaTable::new(log_store, table_config);
        table.load().await?;
        Ok(table)
    } else {
        open_table_with_object_store_and_table_config(
            table_url,
            object_store,
            Default::default(),
            table_config,
        )
        .await
    }
}

impl DeltaTableFormat {
    async fn alter_table_properties(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<()> {
        use crate::kernel::transaction::CommitBuilder;
        use crate::schema::protocol_for_metadata;

        if let Some((key, _)) = changes
            .iter()
            .find(|(key, _)| is_delta_constraint_property(key))
        {
            return plan_err!(
                "[DELTA_ADD_CONSTRAINTS] Please use ALTER TABLE ADD CONSTRAINT to add CHECK constraints. Invalid property: {key}"
            );
        }

        // Parse the location into a URL. Handles both absolute filesystem paths
        // (e.g. `/tmp/table`) and fully-qualified URLs (`file://`, `s3://`, ...).
        let url = parse_location_to_url(path)?;

        // The `DynamicObjectStoreRegistry` lazily registers schemes such as S3/GCS/ABFS,
        // so fetching the store from the registry doubles as the registration entry point.
        let object_store = runtime_env
            .object_store_registry
            .get_store(&url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Only protocol and metadata are needed for ALTER TABLE; skip loading file-level actions.
        let table = match open_table_with_object_store_and_table_config(
            url,
            object_store,
            Default::default(),
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        )
        .await
        {
            Ok(table) => table,
            Err(DeltaTableError::InvalidTableLocation(message))
                if message.contains("No commit files found in _delta_log") =>
            {
                // FIXME: This string match is brittle. Replace it with a typed
                // missing-log/table-not-found error.
                return Ok(());
            }
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE SET/UNSET TBLPROPERTIES")?;

        // Split `SET` and `UNSET` changes.
        let (set_changes, unset_changes): (Vec<_>, Vec<_>) =
            changes.into_iter().partition(|(_, v)| v.is_some());
        let set_pairs: Vec<(&str, &str)> = set_changes
            .iter()
            .filter_map(|(k, v)| v.as_deref().map(|val| (k.as_str(), val)))
            .collect();
        let validated_sets = canonicalize_and_validate_table_properties(set_pairs.iter().copied())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let canonical_unsets: Vec<String> = unset_changes
            .iter()
            .map(|(k, _)| {
                route_table_property_key(k)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| k.clone())
            })
            .collect();

        let existing_config = snapshot.metadata().configuration().clone();

        // Enforce existence of UNSET keys unless `IF EXISTS` was specified.
        if !if_exists {
            for key in &canonical_unsets {
                if !existing_config.contains_key(key) {
                    return plan_err!(
                        "cannot remove property '{key}' because it is not set on the table"
                    );
                }
            }
        }

        // Build new metadata by applying changes.
        let mut new_metadata = snapshot.metadata().clone();
        for (key, value) in &validated_sets {
            new_metadata = new_metadata.add_config_key(key.clone(), value.clone());
        }
        for key in &canonical_unsets {
            new_metadata = new_metadata.remove_config_key(key);
        }

        // Derive the desired protocol from the new configuration and merge it with the
        // existing protocol. We only ever upgrade: features already present on the table
        // are preserved, and new feature requirements are added.
        let desired_protocol = protocol_for_metadata(&new_metadata)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let desired_protocol = avoid_stable_type_widening_auto_upgrade_for_preview_tables(
            snapshot.protocol(),
            &desired_protocol,
            new_metadata.configuration(),
        );

        let existing_protocol = snapshot.protocol();
        let (merged_protocol, protocol_upgraded) =
            merge_protocol_for_upgrade(existing_protocol, &desired_protocol);

        let mut actions: Vec<CommitAction> = Vec::new();
        if protocol_upgraded {
            actions.push(CommitAction::Protocol(merged_protocol));
        }
        actions.push(CommitAction::Metadata(new_metadata));

        let operation = match (validated_sets.is_empty(), canonical_unsets.is_empty()) {
            (false, true) => DeltaOperation::SetTableProperties {
                properties: validated_sets,
            },
            (true, false) => DeltaOperation::UnsetTableProperties {
                properties: canonical_unsets,
            },
            _ => DeltaOperation::SetTableProperties {
                properties: validated_sets,
            },
        };

        CommitBuilder::default()
            .with_actions(actions)
            .build(Some(snapshot), table.log_store(), operation)
            .await
            .map(|_| ())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    async fn add_check_constraint(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        name: &str,
        expression: &str,
    ) -> Result<()> {
        use crate::kernel::transaction::CommitBuilder;
        use crate::schema::protocol_for_metadata;

        let url = parse_location_to_url(path)?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = open_table_with_object_store_and_table_config(
            url,
            object_store,
            Default::default(),
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE ADD CONSTRAINT")?;
        let key = format!("delta.constraints.{name}");
        if snapshot
            .metadata()
            .configuration()
            .keys()
            .any(|existing| existing.eq_ignore_ascii_case(&key))
        {
            return plan_err!("Delta constraint '{name}' already exists");
        }

        let new_metadata = snapshot
            .metadata()
            .clone()
            .add_config_key(key, expression.to_string());
        let desired_protocol = protocol_for_metadata(&new_metadata)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let desired_protocol = avoid_stable_type_widening_auto_upgrade_for_preview_tables(
            snapshot.protocol(),
            &desired_protocol,
            new_metadata.configuration(),
        );
        let (merged_protocol, protocol_upgraded) =
            merge_protocol_for_upgrade(snapshot.protocol(), &desired_protocol);

        let mut actions = Vec::new();
        if protocol_upgraded {
            actions.push(CommitAction::Protocol(merged_protocol));
        }
        actions.push(CommitAction::Metadata(new_metadata));

        CommitBuilder::default()
            .with_actions(actions)
            .build(
                Some(snapshot),
                table.log_store(),
                DeltaOperation::AddConstraint {
                    name: name.to_string(),
                    expr: expression.to_string(),
                },
            )
            .await
            .map(|_| ())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn alter_table_column_type(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        column_path: Vec<String>,
        data_type: ArrowDataType,
    ) -> Result<()> {
        use crate::kernel::transaction::CommitBuilder;

        let url = parse_location_to_url(path)?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = match open_table_with_object_store_and_table_config(
            url,
            object_store,
            Default::default(),
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        )
        .await
        {
            Ok(table) => table,
            Err(DeltaTableError::InvalidTableLocation(message))
                if message.contains("No commit files found in _delta_log") =>
            {
                // FIXME: This string match is brittle. Replace it with a typed
                // missing-log/table-not-found error when the Delta table API exposes one.
                return Ok(());
            }
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE ALTER COLUMN TYPE")?;
        let current_metadata = snapshot.metadata();
        let current_kernel = StructType::try_from(snapshot.schema())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let target_type = DeltaDataType::try_from(&data_type)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let candidate_kernel =
            alter_delta_column_type(&current_kernel, &column_path, &column_path, target_type)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let type_changes = collect_type_changes(&current_kernel, &candidate_kernel)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if type_changes.is_empty() {
            return Ok(());
        }
        if !snapshot.table_properties().enable_type_widening() {
            return plan_err!(
                "Delta ALTER COLUMN TYPE requires table property delta.enableTypeWidening=true"
            );
        }
        if !protocol_can_write_type_widening(snapshot.protocol()) {
            return plan_err!(
                "Delta ALTER COLUMN TYPE requires the typeWidening reader and writer table features"
            );
        }
        for (field_path, change) in &type_changes {
            if !is_supported_type_change_for_write(
                snapshot.protocol(),
                &change.from_type,
                &change.to_type,
            ) {
                return plan_err!(
                    "Delta ALTER COLUMN TYPE change at {} is not supported by Iceberg-compatible Delta tables: {} -> {}",
                    format_type_change_path(field_path, &change.field_path),
                    change.from_type,
                    change.to_type
                );
            }
        }

        let operation_column = operation_column_json(&candidate_kernel, &column_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let candidate_kernel = add_type_widening_metadata(&current_kernel, &candidate_kernel)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let kmode = snapshot.effective_column_mapping_mode();
        let (_final_kernel, updated_metadata) =
            evolve_schema(&current_kernel, &candidate_kernel, current_metadata, kmode)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let actions = vec![CommitAction::Metadata(updated_metadata)];
        CommitBuilder::default()
            .with_actions(actions)
            .build(
                Some(snapshot),
                table.log_store(),
                DeltaOperation::AlterColumn {
                    column: operation_column,
                },
            )
            .await
            .map(|_| ())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn alter_table_column_default(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        column_path: Vec<String>,
        default: Option<String>,
    ) -> Result<()> {
        use crate::kernel::transaction::CommitBuilder;
        use crate::schema::protocol_for_metadata;

        let url = parse_location_to_url(path)?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if column_path.len() != 1 {
            return plan_err!("ALTER COLUMN DEFAULT only supports top-level columns");
        }
        let table = match open_table_with_object_store_and_table_config(
            url,
            object_store,
            Default::default(),
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        )
        .await
        {
            Ok(table) => table,
            Err(DeltaTableError::InvalidTableLocation(message))
                if message.contains("No commit files found in _delta_log") =>
            {
                // FIXME: This string match is brittle. Replace it with a typed
                // missing-log/table-not-found error when the Delta table API exposes one.
                return Ok(());
            }
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE ALTER COLUMN DEFAULT")?;
        let current_kernel = StructType::try_from(snapshot.schema())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let candidate_kernel =
            alter_delta_column_default(&current_kernel, &column_path, default.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if candidate_kernel == current_kernel {
            return Ok(());
        }

        let updated_metadata = snapshot
            .metadata()
            .clone()
            .with_schema(&candidate_kernel)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let desired_protocol = protocol_for_metadata(&updated_metadata)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let desired_protocol = avoid_stable_type_widening_auto_upgrade_for_preview_tables(
            snapshot.protocol(),
            &desired_protocol,
            updated_metadata.configuration(),
        );
        let (merged_protocol, protocol_upgraded) =
            merge_protocol_for_upgrade(snapshot.protocol(), &desired_protocol);

        let operation_column = operation_column_json(&candidate_kernel, &column_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut actions = Vec::new();
        if protocol_upgraded {
            actions.push(CommitAction::Protocol(merged_protocol));
        }
        actions.push(CommitAction::Metadata(updated_metadata));

        CommitBuilder::default()
            .with_actions(actions)
            .build(
                Some(snapshot),
                table.log_store(),
                DeltaOperation::AlterColumn {
                    column: operation_column,
                },
            )
            .await
            .map(|_| ())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

fn alter_delta_column_default(
    schema: &StructType,
    column_path: &[String],
    default: Option<String>,
) -> crate::spec::DeltaResult<StructType> {
    let Some((name, nested_path)) = column_path.split_first() else {
        return Err(DeltaTableError::schema(
            "ALTER COLUMN DEFAULT requires a column name",
        ));
    };
    let mut found = false;
    StructType::try_from_results(schema.fields().map(|field| {
        if field.name() == name {
            found = true;
            if nested_path.is_empty() {
                Ok(apply_default_to_field(field, default.clone()))
            } else {
                alter_nested_delta_column_default(field, nested_path, column_path, default.clone())
            }
        } else {
            Ok(field.clone())
        }
    }))
    .and_then(|schema| {
        if found {
            Ok(schema)
        } else {
            Err(DeltaTableError::missing_column(column_path.join(".")))
        }
    })
}

fn alter_nested_delta_column_default(
    field: &StructField,
    nested_path: &[String],
    full_path: &[String],
    default: Option<String>,
) -> crate::spec::DeltaResult<StructField> {
    match field.data_type() {
        DeltaDataType::Struct(struct_type) => {
            let data_type = DeltaDataType::from(alter_delta_column_default(
                struct_type,
                nested_path,
                default,
            )?);
            Ok(StructField {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                metadata: field.metadata().clone(),
            })
        }
        other => Err(DeltaTableError::schema(format!(
            "cannot resolve ALTER COLUMN DEFAULT path '{}' through {other}",
            full_path.join(".")
        ))),
    }
}

fn apply_default_to_field(field: &StructField, default: Option<String>) -> StructField {
    let mut metadata = field.metadata().clone();
    match default {
        Some(expr) => {
            metadata.insert(
                ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
                MetadataValue::String(expr),
            );
        }
        None => {
            metadata.remove(ColumnMetadataKey::CurrentDefault.as_ref());
        }
    }
    StructField {
        name: field.name().clone(),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        metadata,
    }
}

fn operation_column_json(
    schema: &StructType,
    column_path: &[String],
) -> crate::spec::DeltaResult<serde_json::Value> {
    let field = resolve_operation_column_field(schema, column_path)?;
    let metadata = serde_json::to_value(field.metadata()).map_err(DeltaTableError::generic_err)?;
    Ok(serde_json::json!({
        "name": column_path.join("."),
        "type": field.data_type().to_string(),
        "nullable": field.is_nullable(),
        "metadata": metadata,
    }))
}

fn ensure_not_catalog_managed_delta(
    snapshot: &crate::table::DeltaSnapshot,
    operation: &str,
) -> Result<()> {
    let protocol = snapshot.protocol();
    if protocol.has_reader_feature(&TableFeature::CatalogManaged)
        || protocol.has_writer_feature(&TableFeature::CatalogManaged)
    {
        return not_impl_err!("{operation} is not yet supported for catalog-managed Delta tables");
    }
    Ok(())
}

fn resolve_operation_column_field(
    schema: &StructType,
    column_path: &[String],
) -> crate::spec::DeltaResult<StructField> {
    let Some((name, nested_path)) = column_path.split_first() else {
        return Err(DeltaTableError::schema(
            "ALTER COLUMN TYPE requires a column name",
        ));
    };
    let field = schema
        .field(name)
        .ok_or_else(|| DeltaTableError::missing_column(column_path.join(".")))?;
    if nested_path.is_empty() {
        Ok(field.clone())
    } else {
        resolve_operation_nested_field(field.data_type(), nested_path, column_path)
    }
}

fn resolve_operation_nested_field(
    data_type: &DeltaDataType,
    path: &[String],
    full_path: &[String],
) -> crate::spec::DeltaResult<StructField> {
    let Some((name, nested_path)) = path.split_first() else {
        return Err(DeltaTableError::schema(
            "ALTER COLUMN TYPE requires a column name",
        ));
    };

    match data_type {
        DeltaDataType::Struct(struct_type) => {
            let field = struct_type
                .field(name)
                .ok_or_else(|| DeltaTableError::missing_column(full_path.join(".")))?;
            if nested_path.is_empty() {
                Ok(field.clone())
            } else {
                resolve_operation_nested_field(field.data_type(), nested_path, full_path)
            }
        }
        DeltaDataType::Array(array) if name == "element" => synthetic_operation_field(
            name,
            array.element_type(),
            array.contains_null(),
            nested_path,
            full_path,
        ),
        DeltaDataType::Map(map) if name == "key" => {
            synthetic_operation_field(name, map.key_type(), false, nested_path, full_path)
        }
        DeltaDataType::Map(map) if name == "value" => synthetic_operation_field(
            name,
            map.value_type(),
            map.value_contains_null(),
            nested_path,
            full_path,
        ),
        DeltaDataType::Array(_) => Err(DeltaTableError::schema(format!(
            "expected 'element' for array column path, found '{name}'"
        ))),
        DeltaDataType::Map(_) => Err(DeltaTableError::schema(format!(
            "expected 'key' or 'value' for map column path, found '{name}'"
        ))),
        other => Err(DeltaTableError::schema(format!(
            "cannot resolve ALTER COLUMN TYPE path segment '{name}' through {other}"
        ))),
    }
}

fn synthetic_operation_field(
    name: &str,
    data_type: &DeltaDataType,
    nullable: bool,
    nested_path: &[String],
    full_path: &[String],
) -> crate::spec::DeltaResult<StructField> {
    if nested_path.is_empty() {
        Ok(StructField::new(name, data_type.clone(), nullable))
    } else {
        resolve_operation_nested_field(data_type, nested_path, full_path)
    }
}

fn protocol_without_feature(protocol: &Protocol, feature: TableFeature) -> Protocol {
    let reader_features = protocol.reader_features().map(|features| {
        features
            .iter()
            .filter(|item| **item != feature)
            .cloned()
            .collect::<Vec<_>>()
    });
    let writer_features = protocol.writer_features().map(|features| {
        features
            .iter()
            .filter(|item| **item != feature)
            .cloned()
            .collect::<Vec<_>>()
    });
    Protocol::new(
        protocol.min_reader_version(),
        protocol.min_writer_version(),
        reader_features,
        writer_features,
    )
}

fn avoid_stable_type_widening_auto_upgrade_for_preview_tables(
    existing: &Protocol,
    desired: &Protocol,
    configuration: &HashMap<String, String>,
) -> Protocol {
    let existing_supports_preview = existing.has_reader_feature(&TableFeature::TypeWideningPreview)
        || existing.has_writer_feature(&TableFeature::TypeWideningPreview);
    let stable_explicitly_requested = configuration
        .keys()
        .any(|key| key.eq_ignore_ascii_case("delta.feature.typeWidening"));

    if existing_supports_preview && !stable_explicitly_requested {
        protocol_without_feature(desired, TableFeature::TypeWidening)
    } else {
        desired.clone()
    }
}

/// Merge an existing protocol with a desired one. The result preserves every feature and
/// version already present on the table, and adds anything additionally required by
/// `desired`. Returns `(merged, upgraded)` where `upgraded` indicates whether the merged
/// protocol differs from `existing` and therefore needs to be written as a new action.
fn merge_protocol_for_upgrade(
    existing: &crate::spec::Protocol,
    desired: &crate::spec::Protocol,
) -> (crate::spec::Protocol, bool) {
    use crate::spec::{Protocol, TableFeature};

    let new_min_reader = existing
        .min_reader_version()
        .max(desired.min_reader_version());
    let new_min_writer = existing
        .min_writer_version()
        .max(desired.min_writer_version());

    fn merge_features(
        existing: Option<&[TableFeature]>,
        desired: Option<&[TableFeature]>,
    ) -> Option<Vec<TableFeature>> {
        match (existing, desired) {
            (None, None) => None,
            (Some(a), None) => Some(a.to_vec()),
            (None, Some(b)) => {
                if b.is_empty() {
                    Some(Vec::new())
                } else {
                    Some(b.to_vec())
                }
            }
            (Some(a), Some(b)) => {
                let mut out = a.to_vec();
                for f in b {
                    if !out.contains(f) {
                        out.push(f.clone());
                    }
                }
                Some(out)
            }
        }
    }

    // Only attach explicit reader/writer feature lists if the corresponding version
    // requires them (>=3 for readers, >=7 for writers) -- otherwise older clients may
    // mis-interpret the table as being on the table-features protocol.
    let reader_features = if new_min_reader >= 3 {
        merge_features(existing.reader_features(), desired.reader_features())
    } else {
        existing.reader_features().map(|s| s.to_vec())
    };
    let writer_features = if new_min_writer >= 7 {
        merge_features(existing.writer_features(), desired.writer_features())
    } else {
        existing.writer_features().map(|s| s.to_vec())
    };

    let merged = Protocol::new(
        new_min_reader,
        new_min_writer,
        reader_features,
        writer_features,
    );

    let upgraded = merged != *existing;
    (merged, upgraded)
}

/// Parse a location string into a [`Url`]. Accepts both fully-qualified URLs and
/// local absolute file system paths.
pub(crate) fn parse_location_to_url(path: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(path) {
        // Reject "scheme-like" strings on Windows such as `c:/foo` that `Url::parse`
        // accepts as opaque URLs.
        if url.scheme().len() > 1 {
            return Ok(url);
        }
    }
    if std::path::Path::new(path).is_absolute() {
        return Url::from_file_path(path)
            .map_err(|_| DataFusionError::Plan(format!("invalid file path: {path}")));
    }
    Err(DataFusionError::Plan(format!(
        "table location must be an absolute path or URL: {path}"
    )))
}

impl DeltaTableFormat {
    pub async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        let mut urls = resolve_listing_urls(ctx, paths.clone()).await?;
        match (urls.pop(), urls.is_empty()) {
            (Some(path), true) => Ok(<ListingTableUrl as AsRef<Url>>::as_ref(&path).clone()),
            _ => plan_err!("expected a single path for Delta table sink: {paths:?}"),
        }
    }
}

fn delta_create_table_arrow_schema(columns: &[TableFormatCreateTableColumn]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| {
            let mut metadata = HashMap::new();
            if let Some(comment) = &column.comment {
                metadata.insert("comment".to_string(), comment.clone());
            }
            let mut field = Field::new(
                column.name.clone(),
                column.data_type.clone(),
                column.nullable,
            );
            if !metadata.is_empty() {
                field = field.with_metadata(metadata);
            }
            field = with_variant_extension_if_marked_storage(field);
            Ok(field)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn delta_create_table_generation_expressions(
    columns: &[TableFormatCreateTableColumn],
) -> HashMap<String, String> {
    columns
        .iter()
        .filter_map(|column| {
            column
                .generated_always_as
                .as_ref()
                .map(|expr| (column.name.clone(), expr.clone()))
        })
        .collect()
}

fn delta_create_table_default_expressions(
    columns: &[TableFormatCreateTableColumn],
) -> HashMap<String, String> {
    columns
        .iter()
        .filter_map(|column| {
            column
                .default
                .as_ref()
                .map(|expr| (column.name.clone(), expr.clone()))
        })
        .collect()
}

fn delta_create_table_identity_columns(
    columns: &[TableFormatCreateTableColumn],
) -> HashMap<String, CatalogTableColumnIdentity> {
    columns
        .iter()
        .filter_map(|column| {
            column
                .identity
                .as_ref()
                .map(|identity| (column.name.clone(), identity.clone()))
        })
        .collect()
}

fn delta_create_table_metadata_configuration(
    properties: Vec<(String, String)>,
) -> crate::spec::DeltaResult<HashMap<String, String>> {
    let table_properties = properties
        .into_iter()
        .filter(|(key, _)| !key.starts_with("option."))
        .collect::<HashMap<_, _>>();
    resolve_delta_metadata_configuration(&table_properties)
}

fn delta_catalog_managed_create_table_properties(
    properties: Vec<(String, String)>,
) -> Vec<(String, String)> {
    properties
        .into_iter()
        .filter(|(key, _)| {
            let lower = key.to_ascii_lowercase();
            !matches!(
                lower.as_str(),
                "comment"
                    | "created_at"
                    | "created_by"
                    | "owner"
                    | "table_type"
                    | "updated_at"
                    | "updated_by"
            ) && !key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_KEY)
                && !key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_LEGACY_KEY)
        })
        .collect()
}

fn validate_existing_delta_create_table_schema(
    existing: &Schema,
    declared: &Schema,
    path: &str,
) -> Result<()> {
    let existing_fields = existing.fields();
    let declared_fields = declared.fields();
    if existing_fields.len() != declared_fields.len() {
        return plan_err!(
            "Delta table already exists at {path} with a different schema: \
             existing has {} fields, declared has {} fields",
            existing_fields.len(),
            declared_fields.len()
        );
    }
    for (existing_field, declared_field) in existing_fields.iter().zip(declared_fields.iter()) {
        if existing_field.name() != declared_field.name()
            || existing_field.data_type() != declared_field.data_type()
            || existing_field.is_nullable() != declared_field.is_nullable()
        {
            return plan_err!(
                "Delta table already exists at {path} with a different schema for field '{}': \
                 existing {:?} nullable={}, declared {:?} nullable={}",
                declared_field.name(),
                existing_field.data_type(),
                existing_field.is_nullable(),
                declared_field.data_type(),
                declared_field.is_nullable()
            );
        }
    }
    Ok(())
}

fn extract_generation_expressions(logical_schema: Option<&DFSchema>) -> HashMap<String, String> {
    let Some(schema) = logical_schema else {
        return HashMap::new();
    };
    schema
        .fields()
        .iter()
        .filter_map(|field| {
            ColumnFeatures::from_map(field.metadata())
                .generation_expression()
                .map(|expr| (field.name().clone(), expr))
        })
        .collect()
}

fn extract_default_expressions(logical_schema: Option<&DFSchema>) -> HashMap<String, String> {
    let Some(schema) = logical_schema else {
        return HashMap::new();
    };
    schema
        .fields()
        .iter()
        .filter_map(|field| {
            ColumnFeatures::from_map(field.metadata())
                .current_default()
                .map(|expr| (field.name().clone(), expr))
        })
        .collect()
}

fn extract_target_nullability(logical_schema: Option<&DFSchema>) -> HashMap<String, bool> {
    let Some(schema) = logical_schema else {
        return HashMap::new();
    };
    schema
        .fields()
        .iter()
        .filter_map(|field| {
            field
                .metadata()
                .get(SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY)
                .and_then(|value| value.parse::<bool>().ok())
                .map(|nullable| (field.name().clone(), nullable))
        })
        .collect()
}

fn extract_metadata_schema(logical_schema: Option<&DFSchema>) -> Option<SchemaRef> {
    let schema = logical_schema?;
    let mut changed = false;
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let features = ColumnFeatures::from_map(field.metadata());
            let mut metadata = field.metadata().clone();
            if features.is_not_null_constraint() {
                changed = true;
                metadata.remove(ColumnFeatureKey::NotNullConstraint.as_str());
                let mut output = Field::new(field.name().clone(), field.data_type().clone(), false);
                if !metadata.is_empty() {
                    output = output.with_metadata(metadata);
                }
                output
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    changed.then(|| Arc::new(Schema::new(fields)))
}

fn extract_identity_columns(
    logical_schema: Option<&DFSchema>,
) -> HashMap<String, sail_common_datafusion::catalog::CatalogTableColumnIdentity> {
    let Some(schema) = logical_schema else {
        return HashMap::new();
    };
    schema
        .fields()
        .iter()
        .filter_map(|field| {
            ColumnFeatures::from_map(field.metadata())
                .identity()
                .map(|identity| (field.name().clone(), identity))
        })
        .collect()
}

fn inject_generation_expressions(
    schema: StructType,
    generation_expressions: &HashMap<String, String>,
) -> StructType {
    let fields = schema.into_fields().map(|field| {
        if let Some(expr) = generation_expressions.get(&field.name) {
            let existing_expr = field
                .metadata
                .get(ColumnMetadataKey::GenerationExpression.as_ref())
                .and_then(|v| match v {
                    MetadataValue::String(s) => Some(s.clone()),
                    _ => None,
                });
            if existing_expr.as_deref() == Some(expr.as_str()) {
                field
            } else {
                let StructField {
                    name,
                    data_type,
                    nullable,
                    mut metadata,
                } = field;
                metadata.insert(
                    ColumnMetadataKey::GenerationExpression.as_ref().to_string(),
                    MetadataValue::String(expr.clone()),
                );
                StructField {
                    name,
                    data_type,
                    nullable,
                    metadata,
                }
            }
        } else {
            field
        }
    });
    StructType::new_unchecked(fields)
}

fn inject_default_expressions(
    schema: StructType,
    default_expressions: &HashMap<String, String>,
) -> StructType {
    let fields = schema.into_fields().map(|field| {
        if let Some(expr) = default_expressions.get(&field.name) {
            let existing_expr = field
                .metadata
                .get(ColumnMetadataKey::CurrentDefault.as_ref())
                .and_then(|v| match v {
                    MetadataValue::String(s) => Some(s.clone()),
                    _ => None,
                });
            if existing_expr.as_deref() == Some(expr.as_str()) {
                field
            } else {
                let StructField {
                    name,
                    data_type,
                    nullable,
                    mut metadata,
                } = field;
                metadata.insert(
                    ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
                    MetadataValue::String(expr.clone()),
                );
                StructField {
                    name,
                    data_type,
                    nullable,
                    metadata,
                }
            }
        } else {
            field
        }
    });
    StructType::new_unchecked(fields)
}

fn inject_identity_columns(
    schema: StructType,
    identity_columns: &HashMap<String, CatalogTableColumnIdentity>,
) -> StructType {
    let fields = schema.into_fields().map(|field| {
        if let Some(identity) = identity_columns.get(&field.name) {
            let StructField {
                name,
                data_type,
                nullable,
                mut metadata,
            } = field;
            metadata.insert(
                ColumnMetadataKey::IdentityStart.as_ref().to_string(),
                MetadataValue::Number(identity.start),
            );
            metadata.insert(
                ColumnMetadataKey::IdentityStep.as_ref().to_string(),
                MetadataValue::Number(identity.step),
            );
            metadata.insert(
                ColumnMetadataKey::IdentityAllowExplicitInsert
                    .as_ref()
                    .to_string(),
                MetadataValue::Boolean(identity.allow_explicit_insert),
            );
            if let Some(high_water_mark) = identity.high_water_mark {
                metadata.insert(
                    ColumnMetadataKey::IdentityHighWaterMark
                        .as_ref()
                        .to_string(),
                    MetadataValue::Number(high_water_mark),
                );
            }
            StructField {
                name,
                data_type,
                nullable,
                metadata,
            }
        } else {
            field
        }
    });
    StructType::new_unchecked(fields)
}

fn resolve_delta_metadata_configuration(
    table_properties: &HashMap<String, String>,
) -> crate::spec::DeltaResult<HashMap<String, String>> {
    canonicalize_and_validate_table_properties(
        table_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )
}

fn is_delta_constraint_property(key: &str) -> bool {
    key.len() > "delta.constraints.".len()
        && key
            .get(.."delta.constraints.".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("delta.constraints."))
}

pub fn split_delta_write_options_and_table_properties(
    options: Vec<OptionLayer>,
) -> Result<(Vec<OptionLayer>, HashMap<String, String>)> {
    let mut table_properties = HashMap::new();
    let clean_options = options
        .into_iter()
        .map(|layer| match layer {
            OptionLayer::OptionList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if key.eq_ignore_ascii_case(CATALOG_TABLE_OPTION) {
                        return plan_err!(
                            "Delta write option `{CATALOG_TABLE_OPTION}` is reserved for internal use"
                        );
                    } else if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else {
                        clean_items.push((key, value));
                    }
                }
                Ok((!clean_items.is_empty()).then_some(OptionLayer::OptionList { items: clean_items }))
            }
            OptionLayer::TablePropertyList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if key.eq_ignore_ascii_case(CATALOG_TABLE_OPTION) {
                        return plan_err!(
                            "Delta table property `{CATALOG_TABLE_OPTION}` is reserved for internal use"
                        );
                    } else if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else if key.starts_with("option.") {
                        // Write option from the OPTIONS clause; keep in clean items
                        // so the caller can resolve it as a data-source write option.
                        clean_items.push((key, value));
                    } else {
                        // Custom user table property (e.g. from TBLPROPERTIES); include
                        // it in the Delta metadata configuration as-is.
                        table_properties.insert(key, value);
                    }
                }
                Ok((!clean_items.is_empty())
                    .then_some(OptionLayer::TablePropertyList { items: clean_items }))
            }
            other => Ok(Some(other)),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok((clean_options, table_properties))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_delta_write_options_and_table_properties() -> Result<()> {
        let options = vec![
            OptionLayer::OptionList {
                items: vec![
                    ("mergeSchema".to_string(), "true".to_string()),
                    ("column_mapping_mode".to_string(), "name".to_string()),
                ],
            },
            OptionLayer::OptionList {
                items: vec![
                    ("delta.appendOnly".to_string(), "true".to_string()),
                    ("targetFileSize".to_string(), "10".to_string()),
                ],
            },
        ];

        let (clean_options, table_properties) =
            split_delta_write_options_and_table_properties(options)?;

        assert_eq!(clean_options.len(), 2);
        match &clean_options[0] {
            OptionLayer::OptionList { items } => {
                assert_eq!(items, &[("mergeSchema".to_string(), "true".to_string())]);
            }
            _ => unreachable!("expected OptionList"),
        }
        match &clean_options[1] {
            OptionLayer::OptionList { items } => {
                assert_eq!(items, &[("targetFileSize".to_string(), "10".to_string())]);
            }
            _ => unreachable!("expected OptionList"),
        }
        assert_eq!(
            table_properties.get("delta.columnMapping.mode"),
            Some(&"name".to_string())
        );
        assert_eq!(
            table_properties.get("delta.appendOnly"),
            Some(&"true".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_split_delta_write_options_and_table_properties_from_table_property_list() -> Result<()>
    {
        // Simulate a TablePropertyList as produced for an existing catalog table.
        // It may contain:
        //   - known delta properties (e.g. delta.appendOnly) -> routed to table_properties
        //   - option.* write options stored from the OPTIONS clause -> kept in clean items
        //   - custom user TBLPROPERTIES (e.g. my.tag) -> forwarded to table_properties as-is
        let options = vec![
            OptionLayer::TablePropertyList {
                items: vec![
                    ("delta.appendOnly".to_string(), "true".to_string()),
                    ("option.target_file_size".to_string(), "50000".to_string()),
                    ("my.tag".to_string(), "custom-value".to_string()),
                    ("keep.me".to_string(), "yes".to_string()),
                ],
            },
            OptionLayer::OptionList {
                items: vec![("path".to_string(), "/tmp/table".to_string())],
            },
        ];

        let (clean_options, table_properties) =
            split_delta_write_options_and_table_properties(options)?;

        // delta.appendOnly routes to table_properties; option.* stays in clean items;
        // custom properties (my.tag, keep.me) also go to table_properties.
        assert_eq!(
            table_properties.get("delta.appendOnly"),
            Some(&"true".to_string())
        );
        assert_eq!(
            table_properties.get("my.tag"),
            Some(&"custom-value".to_string())
        );
        assert_eq!(table_properties.get("keep.me"), Some(&"yes".to_string()));
        // option.target_file_size should NOT be in table_properties
        assert_eq!(table_properties.get("option.target_file_size"), None);

        // option.target_file_size and path remain as clean items
        assert_eq!(clean_options.len(), 2);
        match &clean_options[0] {
            OptionLayer::TablePropertyList { items } => {
                assert_eq!(
                    items,
                    &[("option.target_file_size".to_string(), "50000".to_string())]
                );
            }
            _ => unreachable!("expected TablePropertyList"),
        }
        match &clean_options[1] {
            OptionLayer::OptionList { items } => {
                assert_eq!(items, &[("path".to_string(), "/tmp/table".to_string())]);
            }
            _ => unreachable!("expected OptionList"),
        }
        Ok(())
    }

    #[test]
    fn test_catalog_table_option_is_reserved_for_delta_options() {
        let options = vec![OptionLayer::OptionList {
            items: vec![
                (
                    CATALOG_TABLE_OPTION.to_string(),
                    r#"["catalog","schema","table"]"#.to_string(),
                ),
                ("path".to_string(), "/tmp/table".to_string()),
            ],
        }];

        let result = split_delta_write_options_and_table_properties(options);
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("reserved for internal use")
        ));
    }

    #[test]
    fn test_catalog_table_option_is_reserved_for_table_properties() {
        let options = vec![OptionLayer::TablePropertyList {
            items: vec![(
                CATALOG_TABLE_OPTION.to_string(),
                r#"["catalog","schema","table"]"#.to_string(),
            )],
        }];

        let result = split_delta_write_options_and_table_properties(options);
        assert!(matches!(
            &result,
            Err(err) if format!("{err}").contains("reserved for internal use")
        ));
    }

    #[test]
    fn preview_type_widening_guard_removes_implicit_stable_upgrade() {
        let existing = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::TypeWideningPreview]),
            Some(vec![TableFeature::TypeWideningPreview]),
        );
        let desired = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::TypeWidening]),
            Some(vec![
                TableFeature::AllowColumnDefaults,
                TableFeature::TypeWidening,
            ]),
        );

        let adjusted = avoid_stable_type_widening_auto_upgrade_for_preview_tables(
            &existing,
            &desired,
            &HashMap::new(),
        );

        assert!(!adjusted.has_reader_feature(&TableFeature::TypeWidening));
        assert!(!adjusted.has_writer_feature(&TableFeature::TypeWidening));
        assert!(adjusted.has_writer_feature(&TableFeature::AllowColumnDefaults));
    }

    #[test]
    fn preview_type_widening_guard_preserves_explicit_stable_request() {
        let existing = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::TypeWideningPreview]),
            Some(vec![TableFeature::TypeWideningPreview]),
        );
        let desired = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::TypeWidening]),
            Some(vec![TableFeature::TypeWidening]),
        );
        let configuration = HashMap::from([(
            "delta.feature.typeWidening".to_string(),
            "supported".to_string(),
        )]);

        let adjusted = avoid_stable_type_widening_auto_upgrade_for_preview_tables(
            &existing,
            &desired,
            &configuration,
        );

        assert!(adjusted.has_reader_feature(&TableFeature::TypeWidening));
        assert!(adjusted.has_writer_feature(&TableFeature::TypeWidening));
    }
}
