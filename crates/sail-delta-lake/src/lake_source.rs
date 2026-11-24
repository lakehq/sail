// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, DataFusionError, Result, not_impl_err, plan_err};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_expr::expr::Sort;
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{Expr, Extension, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::catalog::delta::{
    DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY, DeltaCreatedVersion,
    DeltaMetadataInfo, DeltaProtocolInfo, unity_table_id_value,
};
use sail_common_datafusion::catalog::{
    CatalogPartitionField, CatalogTableColumnIdentity, CommitAuthority, LakehouseExecutionContext,
    LakehouseOperation,
};
use sail_common_datafusion::column_features::{
    ColumnFeatureKey, ColumnFeatures, SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY,
};
use sail_common_datafusion::command::CommandOutput;
use sail_common_datafusion::datasource::{
    BucketBy, CATALOG_TABLE_OPTION, DataSource, DeltaCheckConstraintExpr, DeltaConstraintViolation,
    OptionLayer, PhysicalSinkMode, SinkInfo, SinkMode, SourceInfo, create_sort_order,
    find_path_in_options,
};
use sail_common_datafusion::lakesource::{
    LakeSource, LakeSourceAlterTableOperation, LakeSourceCreateTableColumn,
    LakeSourceCreateTableInfo, LakeSourceCreateTableResult, LakeSourceMetadata, RowLevelOperation,
};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::session::config::SessionConfigService;
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::with_variant_extension_if_marked_storage;
use sail_data_source::options::ResolveOptions;
use sail_data_source::resolve_table_url;
use sail_logical_plan::check_constraints::apply_delta_check_constraint_filter;
use url::Url;

use crate::catalog_managed::{metadata_with_catalog_managed, protocol_with_catalog_managed};
use crate::datasource::actions::adds_to_remove_actions;
use crate::delta_log::{StorageConfig, get_actions, in_commit_timestamp_from_actions};
use crate::options::r#gen::{DeltaReadOptions, DeltaWriteOptions};
use crate::options::session_replace_where_constraint_check;
use crate::physical_plan::DeltaTableReplacement;
use crate::physical_plan::planner::{DeltaPhysicalPlanner, DeltaPlannerConfig, PlannerContext};
use crate::schema::manager::{canonicalize_partition_columns, is_check_constraint_property};
use crate::schema::type_widening::alter_column_type as alter_delta_column_type;
use crate::schema::{
    add_type_widening_metadata, annotate_arrow_schema_for_column_mapping,
    annotate_for_column_mapping, annotate_new_fields_for_column_mapping, collect_type_changes,
    column_mapping_metadata_only, compute_max_column_id, evolve_schema, format_type_change_path,
    inject_default_expressions, inject_generation_expressions, inject_identity_columns,
    is_supported_type_change_for_write, metadata_for_create_with_struct_type,
    normalize_delta_schema, protocol_can_write_type_widening, protocol_for_metadata,
};
use crate::snapshot::DeltaSnapshotConfig;
use crate::spec::{
    Action, ColumnMappingMode, ColumnMetadataKey, CommitAction, DataType as DeltaDataType,
    DeltaOperation, Metadata, MetadataValue, Protocol, SaveMode, StructField, StructType,
    TableFeature, TableProperties, canonicalize_and_validate_table_properties,
    route_table_property_key,
};
use crate::table::{
    DeltaSnapshot, DeltaTable, SnapshotIntent, catalog_managed_commit_context,
    create_delta_table_with_object_store, create_logstore_with_object_store,
    infer_delta_logical_metadata, infer_delta_logical_schema,
    load_catalog_managed_commits_for_snapshot, open_table_with_object_store_and_table_config,
};
use crate::transaction::{CommitBuilder, CommitProperties};
use crate::{DeltaTableError, create_delta_source};

/// Delta Lake implementation of [`LakeSource`].
#[derive(Debug)]
pub struct DeltaLakeSource;

#[async_trait]
impl DataSource for DeltaLakeSource {
    fn name(&self) -> &str {
        "delta"
    }

    fn as_lake_source(self: Arc<Self>) -> Option<Arc<dyn LakeSource>> {
        Some(self)
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

    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in Delta table options");
        };
        let SinkInfo {
            input,
            identity_columns_to_track,
            sql_insert,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            lakehouse_table,
            create_table_schema,
            replace_table_metadata,
            create_table,
        } = info;
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for Delta format");
        }
        // Capture the session setting in the incoming plan, before the writer adds
        // retained old rows or CDF branches. Workers only execute the assertion.
        let input = match &mode {
            SinkMode::OverwriteIf { condition }
                if session_replace_where_constraint_check(ctx.config())? =>
            {
                apply_delta_check_constraint_filter(
                    input,
                    &replace_where_constraints(condition),
                    None,
                )?
            }
            _ => input,
        };
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(DeltaWriteNode::new(
                Arc::new(input),
                DeltaWriteNodeOptions {
                    identity_columns_to_track,
                    output: if sql_insert {
                        CommandOutput::Empty
                    } else {
                        CommandOutput::Count
                    },
                    path,
                    mode,
                    partition_by,
                    bucket_by,
                    sort_order,
                    options,
                    lakehouse_table,
                    create_table_schema,
                    replace_table_metadata,
                    create_table,
                },
            )),
        }))
    }
}

fn replace_where_constraints(condition: &ExprWithSource) -> Vec<DeltaCheckConstraintExpr> {
    // Delta's extractConstraints ignores constant-only top-level conjuncts, not
    // constants nested within column-bearing expressions (such as OR).
    split_conjunction(&condition.expr)
        .into_iter()
        .filter(|expr| expr.any_column_refs())
        .map(|expr| DeltaCheckConstraintExpr {
            name: "replaceWhere".to_string(),
            expression: condition
                .source
                .clone()
                .unwrap_or_else(|| condition.expr.to_string()),
            expr: expr.clone(),
            violation: DeltaConstraintViolation::ReplaceWhere,
        })
        .collect()
}

#[async_trait]
impl LakeSource for DeltaLakeSource {
    async fn prepare_shallow_clone(
        &self,
        ctx: &datafusion::execution::TaskContext,
        source: futures::future::BoxFuture<'_, Result<String>>,
        destination: &str,
        mode: sail_common::spec::CreateTableMode,
    ) -> Result<sail_common_datafusion::lakesource::LakeSourceClone> {
        crate::clone::prepare(ctx, source, destination, mode).await
    }

    fn procedure_provider(
        self: Arc<Self>,
    ) -> Option<Arc<dyn sail_common_datafusion::lakeprocedure::LakeProcedureProvider>> {
        Some(self)
    }

    async fn table_properties(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Vec<(String, String)>> {
        let table_url = Self::parse_table_url(ctx, info.paths).await?;
        let storage_options: Vec<_> = info
            .options
            .iter()
            .filter_map(|layer| match layer {
                OptionLayer::TablePropertyList { items } => Some(items),
                _ => None,
            })
            .flatten()
            .filter(|(key, _)| {
                key.strip_prefix("option.")
                    .is_some_and(|key| !key.to_ascii_lowercase().starts_with("fs."))
            })
            .cloned()
            .collect();
        let options = DeltaReadOptions::resolve(ctx, info.options)?;
        let mut properties =
            crate::table::delta_table_properties(ctx, table_url, options, info.lakehouse_table)
                .await?;
        properties.extend(storage_options);
        Ok(properties)
    }

    async fn infer_metadata(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<LakeSourceMetadata> {
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
        infer_delta_logical_metadata(ctx, table_url, schema, options, lakehouse_table).await
    }

    fn create_table_schema_for_write(
        &self,
        schema: &SchemaRef,
        properties: &[(String, String)],
    ) -> Result<Option<SchemaRef>> {
        let configuration = delta_create_table_metadata_configuration(properties.to_vec())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mode = configuration
            .get("delta.columnMapping.mode")
            .and_then(|mode| ColumnMappingMode::try_from(mode.as_str()).ok())
            .unwrap_or_default();
        if mode == ColumnMappingMode::None {
            return Ok(None);
        }
        let schema = annotate_arrow_schema_for_column_mapping(schema)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(schema)))
    }

    async fn create_table_metadata(
        &self,
        runtime_env: Arc<RuntimeEnv>,
        config: &SessionConfig,
        info: LakeSourceCreateTableInfo,
    ) -> Result<LakeSourceCreateTableResult> {
        let LakeSourceCreateTableInfo {
            path,
            columns,
            comment,
            partition_by,
            properties,
            replace,
            lakehouse_table,
            required_protocol,
        } = info;
        let catalog_table = lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table().to_vec());
        let unity_required_protocol = required_protocol
            .map(parse_unity_required_protocol)
            .transpose()?;

        // A new table gets its id from the properties of the staged table, and a
        // replaced table keeps the id that it has in the catalog.
        let catalog_managed_table_id = if catalog_table.is_some() {
            Some(
                unity_table_id_value(
                    properties
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str())),
                )
                .or_else(|| {
                    lakehouse_table
                        .as_ref()
                        .and_then(|context| context.table_identity.table_id.as_deref())
                })
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

        // The replacement of a table that exists in the catalog reads the commits that
        // the catalog ratified.
        let replace_context =
            catalog_managed_commit_context(lakehouse_table.as_ref()).filter(|context| {
                replace
                    && context.table_identity.table_id.is_some()
                    && !matches!(
                        context.operation,
                        LakehouseOperation::Create | LakehouseOperation::Register
                    )
            });
        let task_context = datafusion::execution::TaskContext::default()
            .with_session_config(config.clone())
            .with_runtime(runtime_env.clone());
        let existing_table = if replace_context.is_some() {
            Some(crate::ddl::open_table_with_files(&task_context, &path, replace_context).await?)
        } else {
            match open_table_with_object_store_and_table_config(
                table_url.clone(),
                object_store.clone(),
                Default::default(),
                DeltaSnapshotConfig {
                    require_files: replace,
                    ..Default::default()
                },
            )
            .await
            {
                Ok(table) => Some(table),
                Err(DeltaTableError::InvalidTableLocation(_))
                | Err(DeltaTableError::FileNotFound(_)) => None,
                Err(err) => return Err(DataFusionError::External(Box::new(err))),
            }
        };

        let declared_schema = delta_create_table_arrow_schema(&columns)?;
        if let Some(table) = existing_table.as_ref() {
            if replace {
                if declared_schema.fields().is_empty() {
                    return plan_err!("schema is not provided");
                }
            } else {
                if !declared_schema.fields().is_empty() {
                    let snapshot = table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let existing_schema = snapshot
                        .logical_arrow_schema()
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
                let snapshot = table
                    .snapshot()
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                return Ok(LakeSourceCreateTableResult {
                    schema: Some(
                        snapshot
                            .logical_arrow_schema()
                            .map_err(|error| DataFusionError::External(Box::new(error)))?,
                    ),
                    partition_by: Some(sail_common_datafusion::catalog::identity_partition_fields(
                        snapshot.metadata().partition_columns(),
                    )),
                    ..Default::default()
                });
            }
        }

        if declared_schema.fields().is_empty() {
            if replace {
                return plan_err!("schema is not provided");
            } else {
                return plan_err!(
                    "Delta CREATE TABLE requires at least one column when no existing Delta metadata is present"
                );
            }
        }

        let replace_snapshot = if let Some(table) = existing_table.as_ref() {
            Some(
                table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .clone(),
            )
        } else {
            None
        };

        // A catalog-managed table is replaced with a commit to the catalog. Any other
        // table is replaced with a commit to the file system, which is not allowed for
        // a table with the catalog-managed protocol.
        let replace_context = replace_context.filter(|_| {
            replace_snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.protocol().is_catalog_managed())
        });
        if let Some(snapshot) = replace_snapshot.as_ref()
            && replace_context.is_none()
        {
            ensure_not_catalog_managed_delta(snapshot, "REPLACE TABLE without the catalog")?;
        }

        let (protocol, metadata) = create_table_protocol_and_metadata(
            config,
            &declared_schema,
            &columns,
            comment,
            &partition_by,
            properties,
            catalog_managed_table_id.as_deref(),
            unity_required_protocol.as_ref(),
            replace_snapshot.as_deref(),
            replace_context.is_some(),
        )?;

        let report_create_commit = unity_required_protocol.is_some() && replace_snapshot.is_none();
        let log_store = if let Some(table) = existing_table.as_ref() {
            table.log_store()
        } else {
            create_delta_table_with_object_store(
                table_url.clone(),
                object_store,
                Default::default(),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .log_store()
        };
        let operation = DeltaOperation::Create {
            mode: if replace {
                SaveMode::Overwrite
            } else {
                SaveMode::ErrorIfExists
            },
            location: table_url.to_string(),
            protocol: Box::new(protocol.clone()),
            metadata: Box::new(metadata.clone()),
        };

        let mut actions = Vec::new();
        if let Some(snapshot) = replace_snapshot.as_ref() {
            let desired_protocol = avoid_stable_type_widening_auto_upgrade_for_preview_tables(
                snapshot.protocol(),
                &protocol,
                metadata.configuration(),
            );
            let (merged_protocol, protocol_upgraded) =
                merge_protocol_for_upgrade(snapshot.protocol(), &desired_protocol);
            if protocol_upgraded {
                actions.push(CommitAction::Protocol(merged_protocol));
            }
        } else {
            actions.push(CommitAction::Protocol(protocol));
        }
        actions.push(CommitAction::Metadata(metadata));
        if let Some(snapshot) = replace_snapshot.as_ref() {
            actions.extend(
                adds_to_remove_actions(snapshot.adds().to_vec())
                    .into_iter()
                    .map(CommitAction::Remove),
            );
        }

        if let (Some(context), Some(snapshot)) = (replace_context, replace_snapshot.as_ref()) {
            crate::ddl::commit(
                &task_context,
                Some(context),
                snapshot.clone(),
                log_store,
                actions,
                operation,
            )
            .await?;
            return Ok(LakeSourceCreateTableResult::default());
        }
        CommitBuilder::from(CommitProperties::from_session_config(config)?)
            .with_actions(actions)
            .build(replace_snapshot, log_store.clone(), operation)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut result = LakeSourceCreateTableResult::default();
        if report_create_commit {
            result.created_metadata = Some(unity_create_commit(log_store.as_ref()).await?);
        }
        Ok(result)
    }

    async fn plan_row_level_operation(
        &self,
        _ctx: &dyn Session,
        operation: RowLevelOperation,
    ) -> Result<LogicalPlan> {
        match operation {
            RowLevelOperation::Truncate {
                target_plan,
                target,
            } => {
                let mode = sail_common_datafusion::datasource::RowLevelWriteMode::CopyOnWrite;
                Ok(LogicalPlan::Extension(datafusion_expr::Extension {
                    node: Arc::new(
                        sail_logical_plan::row_level::RowLevelWriteNode::new_truncate(
                            target_plan,
                            mode,
                            *target,
                        )?,
                    ),
                }))
            }
            RowLevelOperation::Delete(info) => crate::logical::delete::expand_delete_node(*info),
            RowLevelOperation::Update(info) => crate::logical::update::expand_update_node(*info),
            RowLevelOperation::Merge(info) => crate::logical::merge::expand_merge_node(*info),
        }
    }

    async fn alter_table(
        &self,
        ctx: &datafusion::execution::TaskContext,
        path: &str,
        operation: LakeSourceAlterTableOperation,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<sail_common_datafusion::lakesource::LakeSourceAlterTableResult> {
        reject_catalog_managed_delta_alter(lakehouse_table.as_ref(), &operation)?;
        let runtime_env = ctx.runtime_env();
        let catalog_updated = match operation {
            LakeSourceAlterTableOperation::AlterSnapshotReference { .. } => {
                not_impl_err!("Snapshot references require an Iceberg table")
            }
            LakeSourceAlterTableOperation::AlterIdentifierFields { .. } => {
                not_impl_err!("Identifier fields require an Iceberg table")
            }
            LakeSourceAlterTableOperation::AlterPartitionSpec { .. }
            | LakeSourceAlterTableOperation::SetPartitioning { .. } => {
                return not_impl_err!("Partition spec evolution requires an Iceberg table");
            }
            LakeSourceAlterTableOperation::SetWriteOrder { .. } => {
                return not_impl_err!("WRITE ORDERED BY requires an Iceberg table");
            }
            LakeSourceAlterTableOperation::SetLocation { location } => {
                return crate::ddl::set_location(ctx, path, lakehouse_table.as_ref(), &location)
                    .await;
            }
            LakeSourceAlterTableOperation::ReplaceColumns {
                schema,
                case_sensitive,
            } => {
                return crate::ddl::replace_columns(
                    ctx,
                    path,
                    lakehouse_table.as_ref(),
                    &schema,
                    case_sensitive,
                )
                .await;
            }
            LakeSourceAlterTableOperation::AlterColumns {
                changes,
                case_sensitive,
            } => {
                return crate::ddl::alter_columns(
                    ctx,
                    path,
                    lakehouse_table.as_ref(),
                    &changes,
                    case_sensitive,
                )
                .await;
            }
            LakeSourceAlterTableOperation::SetTableProperties { changes, if_exists } => {
                self.alter_table_properties(ctx, path, lakehouse_table.as_ref(), changes, if_exists)
                    .await
            }
            LakeSourceAlterTableOperation::AlterColumnType {
                column_path,
                data_type,
                ..
            } => {
                self.alter_table_column_type(
                    ctx,
                    path,
                    lakehouse_table.as_ref(),
                    column_path,
                    data_type,
                )
                .await
            }
            LakeSourceAlterTableOperation::AlterColumnDefault {
                column_path,
                default,
            } => {
                self.alter_table_column_default(
                    ctx,
                    path,
                    lakehouse_table.as_ref(),
                    column_path,
                    default,
                )
                .await
            }
            LakeSourceAlterTableOperation::DropCheckConstraint { name, if_exists } => {
                crate::ddl::drop_check_constraint(
                    ctx,
                    path,
                    lakehouse_table.as_ref(),
                    name,
                    if_exists,
                )
                .await
            }
            LakeSourceAlterTableOperation::AddCheckConstraint { name, expression } => {
                let commit_properties =
                    CommitProperties::from_session_config(ctx.session_config())?;
                self.add_check_constraint(runtime_env, path, &name, &expression, commit_properties)
                    .await
                    .map(|()| false)
            }
        }?;
        Ok(
            sail_common_datafusion::lakesource::LakeSourceAlterTableResult {
                catalog_updated,
                ..Default::default()
            },
        )
    }
}

fn reject_catalog_managed_delta_alter(
    lakehouse_table: Option<&LakehouseExecutionContext>,
    operation: &LakeSourceAlterTableOperation,
) -> Result<()> {
    let Some(context) = lakehouse_table else {
        return Ok(());
    };
    if context.commit == CommitAuthority::DeltaRatifiedCommit
        && matches!(
            operation,
            LakeSourceAlterTableOperation::AddCheckConstraint { .. }
        )
    {
        return not_impl_err!(
            "{} is not yet supported for Unity Catalog managed Delta tables",
            delta_alter_operation_name(operation)
        );
    }
    Ok(())
}

fn delta_alter_operation_name(operation: &LakeSourceAlterTableOperation) -> &'static str {
    match operation {
        LakeSourceAlterTableOperation::AlterSnapshotReference { .. } => {
            "ALTER TABLE SNAPSHOT REFERENCE"
        }
        LakeSourceAlterTableOperation::AlterIdentifierFields { .. } => {
            "ALTER TABLE IDENTIFIER FIELDS"
        }
        LakeSourceAlterTableOperation::AlterPartitionSpec { .. }
        | LakeSourceAlterTableOperation::SetPartitioning { .. } => "ALTER TABLE PARTITION FIELD",
        LakeSourceAlterTableOperation::SetWriteOrder { .. } => "ALTER TABLE WRITE",
        LakeSourceAlterTableOperation::SetLocation { .. } => "ALTER TABLE SET LOCATION",
        LakeSourceAlterTableOperation::ReplaceColumns { .. } => "ALTER TABLE REPLACE COLUMNS",
        LakeSourceAlterTableOperation::AlterColumns { .. } => "ALTER TABLE COLUMNS",
        LakeSourceAlterTableOperation::SetTableProperties { .. } => {
            "ALTER TABLE SET/UNSET TBLPROPERTIES"
        }
        LakeSourceAlterTableOperation::AlterColumnType { .. } => "ALTER TABLE ALTER COLUMN TYPE",
        LakeSourceAlterTableOperation::AlterColumnDefault { .. } => {
            "ALTER TABLE ALTER COLUMN DEFAULT"
        }
        LakeSourceAlterTableOperation::AddCheckConstraint { .. } => "ALTER TABLE ADD CONSTRAINT",
        LakeSourceAlterTableOperation::DropCheckConstraint { .. } => "ALTER TABLE DROP CONSTRAINT",
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct DeltaWriteNodeOptions {
    pub identity_columns_to_track: Vec<String>,
    pub output: CommandOutput,
    pub path: String,
    pub mode: SinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Vec<Sort>,
    #[educe(PartialEq(ignore), Hash(ignore), PartialOrd(ignore))]
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    #[educe(PartialOrd(ignore))]
    pub create_table_schema: Option<SchemaRef>,
    pub replace_table_metadata: bool,
    /// The definition of a table that the catalog staged, which the first commit of the
    /// write creates.
    #[educe(Hash(ignore), PartialOrd(ignore))]
    pub create_table: Option<Box<LakeSourceCreateTableInfo>>,
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
            schema: options.output.df_schema(),
            options,
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
    ctx: &dyn Session,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    node: &DeltaWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let DeltaWriteNodeOptions {
        identity_columns_to_track,
        output,
        path,
        mode,
        partition_by,
        bucket_by: _,
        sort_order,
        options,
        lakehouse_table,
        create_table_schema,
        replace_table_metadata,
        create_table,
    } = node.options().clone();

    if is_flow_event_schema(logical_input.schema().as_arrow()) {
        return not_impl_err!("writing streaming data to Delta table");
    }

    let mut mode = match mode {
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
        SinkMode::OverwritePartitions => PhysicalSinkMode::OverwritePartitions,
        SinkMode::ReplaceTable(_) => return plan_err!("staged table replacement requires Iceberg"),
    };
    let physical_sort = create_sort_order(ctx, sort_order, logical_input.schema())?;
    let requested_partition_columns = partition_by
        .into_iter()
        .map(|field| field.column)
        .collect::<Vec<_>>();

    let table_url = DeltaLakeSource::parse_table_url(ctx, vec![path]).await?;
    let (options, table_properties) = split_delta_write_options_and_table_properties(options)?;
    let delta_options = DeltaWriteOptions::resolve(ctx, options)?;
    match delta_options
        .partition_overwrite_mode
        .to_ascii_lowercase()
        .as_str()
    {
        "static" => {}
        "dynamic" if matches!(mode, PhysicalSinkMode::Overwrite) => {
            mode = PhysicalSinkMode::OverwritePartitions
        }
        "dynamic" => {}
        other => return plan_err!("Invalid partitionOverwriteMode: {other}"),
    }
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let lakehouse_planning_context = catalog_managed_commit_context(lakehouse_table.as_ref())
        .filter(|context| {
            context.table_identity.table_id.is_some()
                && !matches!(
                    context.operation,
                    LakehouseOperation::Create | LakehouseOperation::Register
                )
        });
    let catalog_managed_write = lakehouse_planning_context.is_some();
    let table = match open_delta_write_planning_table(
        ctx,
        table_url.clone(),
        object_store,
        lakehouse_planning_context,
        delta_options.txn_app_id.is_some(),
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
    if let (Some(snapshot), Some(app_id), Some(version)) = (
        &table_snapshot,
        &delta_options.txn_app_id,
        delta_options.txn_version,
    ) && snapshot
        .transaction_version(app_id)?
        .is_some_and(|recorded| recorded >= version)
    {
        return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            output.schema(),
        )));
    }
    let table_properties = table_properties.into_iter().collect::<HashMap<_, _>>();
    // New table declarations retain Sail's option aliases; an existing table's custom
    // properties must not be reinterpreted when planning subsequent writes.
    let metadata_configuration = if table_exists {
        resolve_delta_metadata_configuration(&table_properties)
    } else {
        canonicalize_and_validate_table_properties(&table_properties)
    }
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if !table_exists {
        validate_delta_retention_properties(ctx.config(), &metadata_configuration)?;
    }
    // The first commit of the write creates the table that the catalog staged, with the
    // protocol and the metadata of the table definition.
    let create_actions = match create_table.as_deref() {
        Some(_) if table_exists => {
            return plan_err!(
                "Delta table already exists at the location that the catalog staged: {table_url}"
            );
        }
        Some(table) => Some(staged_table_protocol_and_metadata(ctx.config(), table)?),
        None => None,
    };
    let metadata_configuration = match &create_actions {
        Some((_, metadata)) => metadata.configuration().clone(),
        None => metadata_configuration,
    };

    match mode {
        PhysicalSinkMode::ErrorIfExists if table_exists => {
            return plan_err!("Delta table already exists at path: {table_url}");
        }
        PhysicalSinkMode::IgnoreIfExists if table_exists => {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                output.schema(),
            )));
        }
        _ => {}
    }
    // The table is rejected here, before the write produces any data file.
    if let Some(snapshot) = &table_snapshot {
        crate::transaction::PROTOCOL
            .can_write_to_protocol(snapshot.protocol())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    let partition_by = if requested_partition_columns.is_empty() {
        requested_partition_columns
    } else {
        let normalized_write_schema = normalize_delta_schema(&physical_input.schema());
        let delta_write_schema = StructType::try_from(normalized_write_schema.as_ref())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        canonicalize_partition_columns(&delta_write_schema, requested_partition_columns)
            .map_err(|error| DataFusionError::External(Box::new(error)))?
    };

    let existing_partition_columns = table_snapshot
        .as_ref()
        .map(|snapshot| snapshot.metadata().partition_columns().clone());

    if existing_partition_columns
        .as_ref()
        .is_none_or(Vec::is_empty)
    {
        if matches!(mode, PhysicalSinkMode::OverwritePartitions) {
            mode = PhysicalSinkMode::Overwrite;
        }
    } else if delta_options
        .partition_overwrite_mode
        .eq_ignore_ascii_case("dynamic")
        && matches!(mode, PhysicalSinkMode::OverwriteIf { .. })
    {
        return plan_err!("Cannot combine dynamic partition overwrite with replaceWhere");
    }
    if matches!(mode, PhysicalSinkMode::OverwritePartitions) && delta_options.overwrite_schema {
        return plan_err!("Cannot use overwriteSchema with dynamic partition overwrite");
    }

    if let Some(existing_partitions) = &existing_partition_columns
        && !partition_by.is_empty()
        && partition_by != *existing_partitions
    {
        match mode {
            PhysicalSinkMode::Append => {
                return plan_err!(
                    "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                        Cannot change partitioning on append.",
                    existing_partitions,
                    partition_by
                );
            }
            PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
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

    let partition_columns = match (&existing_partition_columns, &mode) {
        (
            Some(existing_partitions),
            PhysicalSinkMode::Append
            | PhysicalSinkMode::OverwriteIf { .. }
            | PhysicalSinkMode::OverwritePartitions,
        ) if partition_by.is_empty() => existing_partitions.clone(),
        (Some(existing_partitions), PhysicalSinkMode::Overwrite)
            if partition_by.is_empty() && !delta_options.overwrite_schema =>
        {
            existing_partitions.clone()
        }
        _ => partition_by,
    };

    if matches!(mode, PhysicalSinkMode::OverwritePartitions) && partition_columns.is_empty() {
        mode = PhysicalSinkMode::Overwrite;
    }
    // The replacement of an existing Unity managed table replaces its properties and its
    // comment with the ones of the statement, in the commit that overwrites the data.
    let replacement = match &table_snapshot {
        Some(snapshot)
            if replace_table_metadata
                && catalog_managed_write
                && matches!(mode, PhysicalSinkMode::Overwrite) =>
        {
            Some(delta_write_replacement(ctx, snapshot, &table_properties)?)
        }
        _ => None,
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
    .with_output(output)
    .with_generation_expressions(extract_generation_expressions(Some(logical_schema)))
    .with_default_expressions(extract_default_expressions(Some(logical_schema)))
    .with_target_nullability(extract_target_nullability(Some(logical_schema)))
    .with_metadata_schema(extract_metadata_schema(Some(logical_schema)))
    .with_identity_columns(extract_identity_columns(Some(logical_schema)))
    .with_identity_columns_to_track(identity_columns_to_track)
    .with_table_snapshot(table_snapshot)
    .with_lakehouse_table(lakehouse_table)
    .with_create_table_schema(create_table_schema)
    .with_replacement(replacement)
    .with_create_actions(create_actions);
    let planner_ctx = PlannerContext::new(ctx, table_config);
    let planner = DeltaPhysicalPlanner::new(planner_ctx);
    planner
        .create_plan(physical_input, mode, physical_sort)
        .await
}

async fn open_delta_write_planning_table(
    ctx: &dyn Session,
    table_url: Url,
    object_store: Arc<dyn object_store::ObjectStore>,
    lakehouse_table: Option<&LakehouseExecutionContext>,
    require_transactions: bool,
) -> std::result::Result<DeltaTable, DeltaTableError> {
    // Only partition columns and table existence are needed at planning time;
    // skip replaying Add/Remove file actions unless a later physical plan needs them.
    // Checksums may omit setTransactions. Replay the log for idempotent writes.
    let table_config = DeltaSnapshotConfig {
        require_files: false,
        require_transactions,
        ..Default::default()
    };

    if let Some(lakehouse_table) = catalog_managed_commit_context(lakehouse_table) {
        let log_store = create_logstore_with_object_store(
            Arc::clone(&object_store),
            table_url.clone(),
            StorageConfig,
        )?;
        let source = load_catalog_managed_commits_for_snapshot(
            &ctx,
            lakehouse_table,
            &table_url,
            log_store.clone(),
            None,
            SnapshotIntent::Write,
            Some(&table_config),
        )
        .await
        .map_err(DeltaTableError::from)?;
        let mut table = source.into_table(log_store, table_config);
        if table.state.is_none() {
            table.load().await?;
        }
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

impl DeltaLakeSource {
    async fn alter_table_properties(
        &self,
        ctx: &datafusion::execution::TaskContext,
        path: &str,
        lakehouse_table: Option<&LakehouseExecutionContext>,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<bool> {
        if let Some((key, _)) = changes
            .iter()
            .find(|(key, _)| is_check_constraint_property(key))
        {
            return plan_err!(
                "[DELTA_ADD_CONSTRAINTS] Please use ALTER TABLE ADD CONSTRAINT to add CHECK constraints. Invalid property: {key}"
            );
        }

        if catalog_managed_commit_context(lakehouse_table).is_some()
            && changes.iter().any(|(key, _)| {
                [
                    "io.unitycatalog.tableId",
                    "delta.feature.catalogManaged",
                    "delta.enableInCommitTimestamps",
                ]
                .iter()
                .any(|reserved| key.eq_ignore_ascii_case(reserved))
            })
        {
            return plan_err!("Cannot alter managed Delta coordination properties");
        }

        // Parse the location into a URL. Handles both absolute filesystem paths
        // (e.g. `/tmp/table`) and fully-qualified URLs (`file://`, `s3://`, ...).
        let table = crate::ddl::open_table(ctx, path, lakehouse_table).await?;

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        if catalog_managed_commit_context(lakehouse_table).is_none() {
            ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE SET/UNSET TBLPROPERTIES")?;
        }

        // Split `SET` and `UNSET` changes.
        let (set_changes, unset_changes): (Vec<_>, Vec<_>) =
            changes.into_iter().partition(|(_, v)| v.is_some());
        let set_pairs: Vec<(&str, &str)> = set_changes
            .iter()
            .filter_map(|(k, v)| v.as_deref().map(|val| (k.as_str(), val)))
            .collect();
        let allow_arbitrary_key = "spark.databricks.delta.allowArbitraryProperties.enabled";
        let allow_arbitrary = ctx
            .session_config()
            .get_extension::<SessionConfigService>()
            .map(|service| service.get_option(allow_arbitrary_key))
            .transpose()?
            .flatten()
            .map(|value| {
                value.to_ascii_lowercase().parse::<bool>().map_err(|_| {
                    DataFusionError::Plan(format!("{allow_arbitrary_key} must be a boolean"))
                })
            })
            .transpose()?
            .unwrap_or(false);
        let validated_sets =
            crate::spec::properties::canonicalize_and_validate_alter_table_properties(
                &set_pairs,
                allow_arbitrary,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let canonical_unsets: Vec<String> = unset_changes
            .iter()
            .map(|(key, _)| crate::spec::properties::canonicalize_alter_table_property_key(key))
            .collect();
        validate_delta_retention_properties(ctx.session_config(), &validated_sets)?;

        let existing_config = snapshot.metadata().configuration().clone();
        if catalog_managed_commit_context(lakehouse_table).is_some()
            && snapshot.protocol().is_catalog_managed()
        {
            ensure_unity_fixed_properties_kept(
                &existing_config,
                validated_sets
                    .iter()
                    .map(|(key, value)| (key.as_str(), Some(value.as_str())))
                    .chain(canonical_unsets.iter().map(|key| (key.as_str(), None))),
            )?;
        }

        // Enforce existence of UNSET keys unless `IF EXISTS` was specified.
        if !if_exists {
            for key in &canonical_unsets {
                if !existing_config.contains_key(key)
                    && !(key == "comment" && snapshot.metadata().description().is_some())
                {
                    return plan_err!(
                        "cannot remove property '{key}' because it is not set on the table"
                    );
                }
            }
        }

        // Build new metadata by applying changes.
        let mut new_metadata = snapshot.metadata().clone();
        for (key, value) in &validated_sets {
            if key == "comment" {
                new_metadata = new_metadata
                    .with_description(Some(value.clone()))
                    .remove_config_key(key);
            } else {
                new_metadata = new_metadata.add_config_key(key.clone(), value.clone());
            }
        }
        for key in &canonical_unsets {
            if key == "comment" {
                new_metadata = new_metadata.with_description(None);
            }
            new_metadata = new_metadata.remove_config_key(key);
        }

        // Derive the desired protocol from the new configuration and merge it with the
        // existing protocol. Features already supported by the table are preserved;
        // equivalent legacy protocol representations may use lower version numbers.
        let desired_protocol = protocol_for_metadata(&new_metadata)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let desired_protocol = avoid_stable_type_widening_auto_upgrade_for_preview_tables(
            snapshot.protocol(),
            &desired_protocol,
            new_metadata.configuration(),
        );

        let existing_protocol = snapshot.protocol();
        let desired_protocol = append_only_protocol_for_existing_table(
            existing_protocol,
            &desired_protocol,
            &new_metadata,
        );
        // Version properties request the corresponding implicit feature set, not metadata keys.
        let reader_version = validated_sets
            .get("delta.minReaderVersion")
            .map(|value| value.parse::<i32>())
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .unwrap_or(existing_protocol.min_reader_version());
        let writer_version = validated_sets
            .get("delta.minWriterVersion")
            .map(|value| value.parse::<i32>())
            .transpose()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .unwrap_or(existing_protocol.min_writer_version());
        // Validate the requested pair before merging can hide an invalid reader/writer dependency.
        if reader_version >= 3 && writer_version < 7 {
            return plan_err!("minReaderVersion 3 requires minWriterVersion 7");
        }
        let version_requirements = Protocol::new(
            reader_version,
            writer_version,
            (reader_version >= 3).then(Vec::new),
            (writer_version >= 7).then(Vec::new),
        );
        let merged_protocol = existing_protocol
            .merge_upgrade_requirements(&version_requirements)
            .merge_upgrade_requirements(&desired_protocol);
        let has_protocol_request = validated_sets.keys().any(|key| {
            matches!(
                key.as_str(),
                "delta.minReaderVersion" | "delta.minWriterVersion"
            ) || key.starts_with("delta.feature.")
        });
        let merged_protocol = if has_protocol_request {
            merged_protocol.normalize_for_alter()
        } else {
            merged_protocol
        };
        crate::transaction::PROTOCOL
            .can_write_to_protocol(&merged_protocol)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let protocol_upgraded = merged_protocol != *existing_protocol;
        let protocol_keys: Vec<_> = new_metadata
            .configuration()
            .keys()
            .filter(|key| {
                matches!(
                    key.as_str(),
                    "delta.minReaderVersion" | "delta.minWriterVersion"
                ) || key.to_ascii_lowercase().starts_with("delta.feature.")
            })
            .cloned()
            .collect();
        for key in protocol_keys {
            new_metadata = new_metadata.remove_config_key(&key);
        }

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

        crate::ddl::commit(
            ctx,
            lakehouse_table,
            snapshot,
            table.log_store(),
            actions,
            operation,
        )
        .await
    }

    async fn add_check_constraint(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        name: &str,
        expression: &str,
        commit_properties: CommitProperties,
    ) -> Result<()> {
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
        let key = format!("delta.constraints.{}", name.to_lowercase());
        if snapshot
            .metadata()
            .configuration()
            .keys()
            .any(|existing| existing.to_lowercase() == key)
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

        CommitBuilder::from(commit_properties)
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
        ctx: &datafusion::execution::TaskContext,
        path: &str,
        lakehouse_table: Option<&LakehouseExecutionContext>,
        column_path: Vec<String>,
        data_type: ArrowDataType,
    ) -> Result<bool> {
        let table = crate::ddl::open_table(ctx, path, lakehouse_table).await?;

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        if catalog_managed_commit_context(lakehouse_table).is_none() {
            ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE ALTER COLUMN TYPE")?;
        }
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
            return Ok(false);
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
        crate::ddl::commit(
            ctx,
            lakehouse_table,
            snapshot,
            table.log_store(),
            actions,
            DeltaOperation::AlterColumn {
                column: operation_column,
                position: None,
            },
        )
        .await
    }

    async fn alter_table_column_default(
        &self,
        ctx: &datafusion::execution::TaskContext,
        path: &str,
        lakehouse_table: Option<&LakehouseExecutionContext>,
        column_path: Vec<String>,
        default: Option<String>,
    ) -> Result<bool> {
        if column_path.len() != 1 {
            return plan_err!("ALTER COLUMN DEFAULT only supports top-level columns");
        }
        let table = crate::ddl::open_table(ctx, path, lakehouse_table).await?;

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        if catalog_managed_commit_context(lakehouse_table).is_none() {
            ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE ALTER COLUMN DEFAULT")?;
        }
        let current_kernel = StructType::try_from(snapshot.schema())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let candidate_kernel =
            alter_delta_column_default(&current_kernel, &column_path, default.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if candidate_kernel == current_kernel {
            return Ok(false);
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

        crate::ddl::commit(
            ctx,
            lakehouse_table,
            snapshot,
            table.log_store(),
            actions,
            DeltaOperation::AlterColumn {
                column: operation_column,
                position: None,
            },
        )
        .await
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

pub(crate) fn operation_column_json(
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

pub(crate) fn ensure_not_catalog_managed_delta(
    snapshot: &crate::table::DeltaSnapshot,
    operation: &str,
) -> Result<()> {
    if snapshot.protocol().is_catalog_managed() {
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
        .fields()
        .find(|field| field.name().eq_ignore_ascii_case(name))
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
                .fields()
                .find(|field| field.name().eq_ignore_ascii_case(name))
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

pub(crate) fn avoid_stable_type_widening_auto_upgrade_for_preview_tables(
    existing: &Protocol,
    desired: &Protocol,
    configuration: &HashMap<String, String>,
) -> Protocol {
    let existing_supports_preview = existing.has_reader_feature(&TableFeature::TypeWideningPreview)
        || existing.has_writer_feature(&TableFeature::TypeWideningPreview);
    let stable_explicitly_requested = configuration
        .keys()
        .any(|key| key.eq_ignore_ascii_case("delta.feature.typeWidening"));

    let desired = if existing_supports_preview && !stable_explicitly_requested {
        protocol_without_feature(desired, TableFeature::TypeWidening)
    } else {
        desired.clone()
    };
    preserve_existing_variant_features(existing, &desired, configuration)
}

fn preserve_existing_variant_features(
    existing: &Protocol,
    desired: &Protocol,
    configuration: &HashMap<String, String>,
) -> Protocol {
    let explicitly_requested = |feature: &str| {
        configuration
            .keys()
            .any(|key| key.eq_ignore_ascii_case(feature))
    };
    let mut desired = desired.clone();
    if existing.has_reader_feature(&TableFeature::VariantTypePreview)
        && !explicitly_requested("delta.feature.variantType")
        && !explicitly_requested("delta.feature.variantShredding")
    {
        desired = protocol_without_feature(&desired, TableFeature::VariantType);
    }
    // Existing stable shredding must not acquire the default preview feature on metadata updates.
    if existing.has_reader_feature(&TableFeature::VariantShredding)
        && !explicitly_requested("delta.feature.variantShredding-preview")
    {
        desired = protocol_without_feature(&desired, TableFeature::VariantShreddingPreview);
    }
    desired
}

fn append_only_protocol_for_existing_table(
    existing: &Protocol,
    desired: &Protocol,
    metadata: &crate::spec::Metadata,
) -> Protocol {
    let append_only = TableProperties::from(metadata.configuration().iter()).append_only();
    let needs_explicit_feature = existing.min_writer_version() < 2
        || existing.min_writer_version() >= 7
        || desired.min_writer_version() >= 7;
    if !append_only
        || !needs_explicit_feature
        || desired.has_writer_feature(&TableFeature::AppendOnly)
    {
        return desired.clone();
    }

    let mut writer_features = desired.writer_features().unwrap_or(&[]).to_vec();
    writer_features.push(TableFeature::AppendOnly);
    Protocol::new(
        desired.min_reader_version(),
        desired.min_writer_version().max(7),
        desired.reader_features().map(<[_]>::to_vec),
        Some(writer_features),
    )
}

/// Merge an existing protocol with a desired one. The result preserves every feature and
/// version already present on the table, and adds anything additionally required by
/// `desired`. Returns `(merged, upgraded)` where `upgraded` indicates whether the merged
/// protocol differs from `existing` and therefore needs to be written as a new action.
fn merge_protocol_for_upgrade(
    existing: &crate::spec::Protocol,
    desired: &crate::spec::Protocol,
) -> (crate::spec::Protocol, bool) {
    let merged = existing.merge_upgrade_requirements(desired);
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

impl DeltaLakeSource {
    pub async fn parse_table_url(_ctx: &dyn Session, mut paths: Vec<String>) -> Result<Url> {
        let Some(path) = paths.pop().filter(|_| paths.is_empty()) else {
            return plan_err!("expected a single path for Delta table sink");
        };
        resolve_table_url(path)
    }
}

fn delta_create_table_arrow_schema(columns: &[LakeSourceCreateTableColumn]) -> Result<SchemaRef> {
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

/// Returns the column mapping that `LakeSource::create_table_schema_for_write` assigned
/// to the columns of a new table, if any.
fn delta_create_table_column_mapping(
    columns: &[LakeSourceCreateTableColumn],
) -> Result<Option<StructType>> {
    let fields = columns
        .iter()
        .map(|column| {
            let metadata = column
                .metadata
                .iter()
                .filter(|(key, _)| {
                    *key == ColumnMetadataKey::ColumnMappingId.as_ref()
                        || *key == ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<HashMap<_, _>>();
            with_variant_extension_if_marked_storage(
                Field::new(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                )
                .with_metadata(metadata),
            )
        })
        .collect::<Vec<_>>();
    let schema = column_mapping_metadata_only(
        &StructType::try_from(normalize_delta_schema(&Arc::new(Schema::new(fields))))
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );
    Ok((compute_max_column_id(&schema) > 0).then_some(schema))
}

fn delta_create_table_generation_expressions(
    columns: &[LakeSourceCreateTableColumn],
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
    columns: &[LakeSourceCreateTableColumn],
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
    columns: &[LakeSourceCreateTableColumn],
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

fn validate_delta_retention_properties(
    config: &SessionConfig,
    properties: &HashMap<String, String>,
) -> Result<()> {
    if !properties.contains_key("delta.logRetentionDuration")
        && !properties.contains_key("delta.deletedFileRetentionDuration")
    {
        return Ok(());
    }
    let key =
        "spark.databricks.delta.vacuum.enforceDeletedFileAndLogRetentionDurationCompatibility";
    let enabled = config
        .get_extension::<SessionConfigService>()
        .map(|service| service.get_option(key))
        .transpose()?
        .flatten()
        .map(|value| {
            value
                .to_ascii_lowercase()
                .parse::<bool>()
                .map_err(|_| DataFusionError::Plan(format!("{key} must be a boolean")))
        })
        .transpose()?
        .unwrap_or(true);
    // Delta validates the supplied properties, using defaults for omitted keys.
    let properties = TableProperties::from(properties.iter());
    if enabled
        && properties.log_retention_duration().as_millis()
            < properties.deleted_file_retention_duration().as_millis()
    {
        return plan_err!(
            "delta.logRetentionDuration must be greater than or equal to delta.deletedFileRetentionDuration"
        );
    }
    Ok(())
}

fn delta_create_table_metadata_configuration(
    properties: Vec<(String, String)>,
) -> crate::spec::DeltaResult<HashMap<String, String>> {
    let table_properties = properties
        .into_iter()
        .filter(|(key, _)| !key.starts_with("option."))
        .collect::<HashMap<_, _>>();
    canonicalize_and_validate_table_properties(&table_properties)
}

/// Converts the Delta protocol required by Unity Catalog for a staged managed table.
/// The protocol must be valid as it is, since the table features that do not match the
/// protocol versions would be dropped when the protocol of the table is merged with it.
/// The table features are validated after the protocol of the table is merged with it.
fn parse_unity_required_protocol(protocol: DeltaProtocolInfo) -> Result<Protocol> {
    let invalid = |e: String| {
        DataFusionError::Plan(format!(
            "invalid Delta protocol required by Unity Catalog: {e}"
        ))
    };
    let protocol: Protocol = serde_json::to_value(protocol)
        .and_then(serde_json::from_value)
        .map_err(|e| invalid(e.to_string()))?;
    crate::transaction::PROTOCOL
        .validate_feature_lists(&protocol)
        .map_err(|e| invalid(e.to_string()))?;
    Ok(protocol)
}

/// Validates that the Delta protocol of a Unity Catalog managed table can be read and written.
fn validate_unity_required_protocol(protocol: &Protocol) -> Result<()> {
    let mut unsupported = crate::transaction::PROTOCOL
        .unsupported_reader_features(protocol)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    for feature in crate::transaction::PROTOCOL
        .unsupported_writer_features(protocol)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    {
        if !unsupported.contains(&feature) {
            unsupported.push(feature);
        }
    }
    if unsupported.is_empty() {
        return Ok(());
    }
    let mut unsupported = unsupported
        .iter()
        .map(|feature| feature.as_str().to_string())
        .collect::<Vec<_>>();
    unsupported.sort();
    not_impl_err!(
        "Delta table features required by Unity Catalog: {}",
        unsupported.join(", ")
    )
}

/// Builds the protocol and the metadata of version 0 of a table that the catalog staged,
/// which the first commit of a write creates.
fn staged_table_protocol_and_metadata(
    config: &SessionConfig,
    table: &LakeSourceCreateTableInfo,
) -> Result<(Protocol, Metadata)> {
    if table
        .partition_by
        .iter()
        .any(|field| field.transform.is_some())
    {
        return not_impl_err!("partition transforms for Delta CREATE TABLE");
    }
    let declared_schema = delta_create_table_arrow_schema(&table.columns)?;
    if declared_schema.fields().is_empty() {
        return plan_err!(
            "Delta CREATE TABLE requires at least one column when no existing Delta metadata is present"
        );
    }
    let table_id = unity_table_id_value(
        table
            .properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .ok_or_else(|| {
        DataFusionError::Plan(
            "catalog-managed Delta CREATE TABLE requires a Unity table id".to_string(),
        )
    })?;
    let required_protocol = table
        .required_protocol
        .clone()
        .map(parse_unity_required_protocol)
        .transpose()?;
    create_table_protocol_and_metadata(
        config,
        &declared_schema,
        &table.columns,
        table.comment.clone(),
        &table.partition_by,
        table.properties.clone(),
        Some(table_id),
        required_protocol.as_ref(),
        None,
        false,
    )
}

/// Builds the protocol and the metadata of a Delta table that is created, or that replaces
/// the table of the snapshot.
#[expect(clippy::too_many_arguments)]
fn create_table_protocol_and_metadata(
    config: &SessionConfig,
    declared_schema: &SchemaRef,
    columns: &[LakeSourceCreateTableColumn],
    comment: Option<String>,
    partition_by: &[CatalogPartitionField],
    properties: Vec<(String, String)>,
    catalog_managed_table_id: Option<&str>,
    unity_required_protocol: Option<&Protocol>,
    replace_snapshot: Option<&DeltaSnapshot>,
    catalog_replace: bool,
) -> Result<(Protocol, Metadata)> {
    let properties = if catalog_managed_table_id.is_some() {
        delta_catalog_managed_create_table_properties(properties)
    } else {
        properties
    };
    let mut metadata_configuration = delta_create_table_metadata_configuration(properties)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    validate_delta_retention_properties(config, &metadata_configuration)?;
    if let Some(snapshot) = replace_snapshot {
        metadata_configuration =
            delta_replace_configuration(snapshot, catalog_replace, metadata_configuration)?;
    }
    let normalized_schema = normalize_delta_schema(declared_schema);
    let mut kernel_schema = StructType::try_from(normalized_schema.as_ref())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    kernel_schema = inject_generation_expressions(
        kernel_schema,
        &delta_create_table_generation_expressions(columns),
    );
    kernel_schema = inject_default_expressions(
        kernel_schema,
        &delta_create_table_default_expressions(columns),
    );
    kernel_schema =
        inject_identity_columns(kernel_schema, &delta_create_table_identity_columns(columns));

    let effective_mode = metadata_configuration
        .get("delta.columnMapping.mode")
        .and_then(|v| ColumnMappingMode::try_from(v.as_str()).ok())
        .unwrap_or_default();

    let mut configuration = metadata_configuration;
    // A table that is created before the data of a write is written has the column
    // mapping that the data is written with.
    let assigned_column_mapping = delta_create_table_column_mapping(columns)?;
    let metadata_schema = if !matches!(effective_mode, ColumnMappingMode::None) {
        let (annotated, max_column_id) = if let Some(snapshot) = replace_snapshot
            .filter(|snapshot| snapshot.effective_column_mapping_mode() != ColumnMappingMode::None)
        {
            // A replaced table gets new physical names, and column IDs after the IDs
            // that the table has used.
            let current = StructType::try_from(snapshot.schema())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let used_column_id = snapshot
                .metadata()
                .configuration()
                .get("delta.columnMapping.maxColumnId")
                .and_then(|id| id.parse::<i64>().ok())
                .unwrap_or_default()
                .max(compute_max_column_id(&current));
            let (annotated, max_column_id) = annotate_new_fields_for_column_mapping(
                &StructType::try_new(Vec::<StructField>::new())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
                &kernel_schema,
                used_column_id + 1,
            );
            (annotated, max_column_id.to_string())
        } else if let Some(assigned) = &assigned_column_mapping {
            let (annotated, max_column_id) = annotate_new_fields_for_column_mapping(
                assigned,
                &kernel_schema,
                compute_max_column_id(assigned) + 1,
            );
            (annotated, max_column_id.to_string())
        } else {
            let annotated = annotate_for_column_mapping(&kernel_schema);
            let max_column_id = compute_max_column_id(&annotated).to_string();
            (annotated, max_column_id)
        };
        configuration.insert(
            "delta.columnMapping.mode".to_string(),
            effective_mode.as_ref().to_string(),
        );
        configuration.insert("delta.columnMapping.maxColumnId".to_string(), max_column_id);
        annotated
    } else {
        kernel_schema
    };

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
        metadata = metadata.with_description(Some(comment));
    }
    let mut protocol =
        protocol_for_metadata(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;
    // A replaced table keeps the protocol and the catalog properties that it has.
    if let Some(table_id) = catalog_managed_table_id.filter(|_| replace_snapshot.is_none()) {
        protocol = protocol_with_catalog_managed(&protocol);
        metadata = metadata_with_catalog_managed(metadata, table_id);
    }
    if let Some(required_protocol) = unity_required_protocol {
        protocol = protocol.merge_upgrade_requirements(required_protocol);
        validate_unity_required_protocol(&protocol)?;
        // The table features are creation-time directives that belong to the protocol,
        // so they are not written to the table configuration.
        let feature_keys = metadata
            .configuration()
            .keys()
            .filter(|key| key.to_ascii_lowercase().starts_with("delta.feature."))
            .cloned()
            .collect::<Vec<_>>();
        for key in feature_keys {
            metadata = metadata.remove_config_key(&key);
        }
    }
    Ok((protocol, metadata))
}

/// Reads version 0 of a staged Unity Catalog managed table to report it to the catalog.
pub(crate) async fn unity_create_commit(
    log_store: &dyn crate::delta_log::LogStore,
) -> Result<DeltaCreatedVersion> {
    let external = |e| DataFusionError::External(Box::new(e));
    let commit = log_store
        .read_commit_entry(0)
        .await
        .map_err(external)?
        .ok_or_else(|| {
            DataFusionError::Internal("Delta table version 0 is missing after CREATE TABLE".into())
        })?;
    let actions = get_actions(0, &commit).map_err(external)?;
    let protocol = actions.iter().find_map(|action| match action {
        Action::Protocol(protocol) => Some(protocol),
        _ => None,
    });
    let metadata = actions.iter().find_map(|action| match action {
        Action::Metadata(metadata) => Some(metadata),
        _ => None,
    });
    let (Some(protocol), Some(metadata), Some(in_commit_timestamp)) = (
        protocol,
        metadata,
        in_commit_timestamp_from_actions(&actions),
    ) else {
        return Err(DataFusionError::Internal(
            "Delta table version 0 is missing the protocol, metadata, or in-commit timestamp"
                .to_string(),
        ));
    };
    Ok(DeltaCreatedVersion {
        protocol: delta_protocol_info(protocol),
        metadata: delta_metadata_info(metadata),
        in_commit_timestamp,
        row_id_high_water_mark: row_id_high_water_mark(&actions)?,
    })
}

/// Returns the protocol in the form that a catalog keeps.
pub(crate) fn delta_protocol_info(protocol: &Protocol) -> DeltaProtocolInfo {
    DeltaProtocolInfo {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: delta_feature_names(protocol.reader_features()),
        writer_features: delta_feature_names(protocol.writer_features()),
    }
}

/// Returns the table metadata in the form that a catalog keeps.
pub(crate) fn delta_metadata_info(metadata: &Metadata) -> DeltaMetadataInfo {
    DeltaMetadataInfo {
        schema_string: metadata.schema_string().to_string(),
        partition_columns: metadata.partition_columns().clone(),
        configuration: metadata
            .configuration()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        description: metadata.description().map(str::to_string),
    }
}

/// The row ID high-water mark that the actions of a commit set.
fn row_id_high_water_mark(actions: &[Action]) -> Result<Option<i64>> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RowTrackingDomain {
        row_id_high_water_mark: i64,
    }

    actions
        .iter()
        .find_map(|action| match action {
            Action::DomainMetadata(domain)
                if domain.domain == crate::transaction::row_tracking::ROW_TRACKING_DOMAIN
                    && !domain.removed =>
            {
                Some(&domain.configuration)
            }
            _ => None,
        })
        .map(|configuration| {
            serde_json::from_str::<RowTrackingDomain>(configuration)
                .map(|domain| domain.row_id_high_water_mark)
                .map_err(|e| DataFusionError::External(Box::new(e)))
        })
        .transpose()
}

fn delta_feature_names(features: Option<&[TableFeature]>) -> Option<Vec<String>> {
    features.map(|features| {
        features
            .iter()
            .map(|feature| feature.as_str().to_string())
            .collect()
    })
}

/// The properties with fixed values that Unity Catalog requires for managed tables.
/// The catalog does not check them again unless the protocol changes.
const UNITY_FIXED_PROPERTIES: [&str; 5] = [
    "delta.enableInCommitTimestamps",
    "delta.enableDeletionVectors",
    "delta.checkpointPolicy",
    "delta.checkpoint.writeStatsAsStruct",
    "delta.checkpoint.writeStatsAsJson",
];

/// The configuration that a REPLACE gives an existing table, from the configuration of
/// the statement.
fn delta_replace_configuration(
    snapshot: &crate::table::DeltaSnapshot,
    catalog_managed: bool,
    mut configuration: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    if catalog_managed {
        ensure_unity_fixed_properties_kept(
            snapshot.metadata().configuration(),
            configuration
                .iter()
                .map(|(key, value)| (key.as_str(), Some(value.as_str()))),
        )?;
        // A catalog-managed table keeps its identity in the catalog, the catalog
        // coordination, and the in-commit timestamps that the coordination requires,
        // as Delta Spark retains them on REPLACE. It also keeps the properties that
        // Unity Catalog requires for managed tables, which the catalog does not check
        // again unless the protocol changes. All the other properties are replaced by
        // the ones of the statement.
        for (key, value) in snapshot.metadata().configuration() {
            if is_delta_replace_retained_property(key) && !configuration.contains_key(key) {
                configuration.insert(key.clone(), value.clone());
            }
        }
    }
    crate::transaction::row_tracking::retain_for_replace(
        snapshot.protocol(),
        snapshot.metadata().configuration(),
        &mut configuration,
    );
    Ok(configuration)
}

/// Whether REPLACE keeps a property of a catalog-managed table that the statement
/// does not set.
fn is_delta_replace_retained_property(key: &str) -> bool {
    [
        DELTA_UNITY_TABLE_ID_KEY,
        DELTA_UNITY_TABLE_ID_LEGACY_KEY,
        "delta.feature.catalogManaged",
        "delta.inCommitTimestampEnablementVersion",
        "delta.inCommitTimestampEnablementTimestamp",
    ]
    .iter()
    .chain(UNITY_FIXED_PROPERTIES.iter())
    .any(|retained| key.eq_ignore_ascii_case(retained))
}

/// Fails if the changes to the properties of a catalog-managed table set a property
/// with a fixed value that the table has to another value, or remove it.
/// A removal is a change with no value.
fn ensure_unity_fixed_properties_kept<'a>(
    existing: &HashMap<String, String>,
    changes: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<()> {
    for (key, value) in changes {
        if !UNITY_FIXED_PROPERTIES
            .iter()
            .any(|fixed| key.eq_ignore_ascii_case(fixed))
        {
            continue;
        }
        let Some((key, required)) = existing.iter().find(|(k, _)| k.eq_ignore_ascii_case(key))
        else {
            continue;
        };
        match value {
            Some(value) if value == required => {}
            Some(value) => {
                return plan_err!(
                    "Unity Catalog requires the table property {key}={required} \
                     for managed Delta tables, but it is set to {value}"
                );
            }
            None => {
                return plan_err!(
                    "Unity Catalog requires the table property {key}={required} \
                     for managed Delta tables"
                );
            }
        }
    }
    Ok(())
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

/// The table definition that a write gives an existing table that it replaces, from the
/// properties of the statement, with the property rules of REPLACE.
fn delta_write_replacement(
    ctx: &dyn Session,
    snapshot: &crate::table::DeltaSnapshot,
    table_properties: &HashMap<String, String>,
) -> Result<DeltaTableReplacement> {
    let mut configuration = canonicalize_and_validate_table_properties(table_properties)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    validate_delta_retention_properties(ctx.config(), &configuration)?;
    let description = configuration.remove("comment");
    let configuration =
        delta_catalog_managed_create_table_properties(configuration.into_iter().collect())
            .into_iter()
            .collect();
    let mut configuration = delta_replace_configuration(
        snapshot,
        snapshot.protocol().is_catalog_managed(),
        configuration,
    )?;
    crate::transaction::row_tracking::add_materialized_column_names(&mut configuration);
    Ok(DeltaTableReplacement {
        configuration,
        description,
    })
}

fn resolve_delta_metadata_configuration(
    table_properties: &HashMap<String, String>,
) -> crate::spec::DeltaResult<HashMap<String, String>> {
    // Explicit option aliases are canonicalized by the splitter; metadata's custom keys are literal.
    let is_delta_property = |key: &str| key.to_ascii_lowercase().starts_with("delta.");
    let mut configuration = canonicalize_and_validate_table_properties(
        table_properties
            .iter()
            .filter(|(key, _)| is_delta_property(key))
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )?;
    configuration.extend(
        table_properties
            .iter()
            .filter(|(key, _)| !is_delta_property(key))
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    Ok(configuration)
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
                    } else if key.to_ascii_lowercase().starts_with("delta.") {
                        let property_key =
                            crate::spec::properties::canonicalize_alter_table_property_key(&key);
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
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit};

    use super::*;

    #[test]
    fn write_replacement_follows_replace_property_rules() -> Result<()> {
        let schema = StructType::try_new(vec![StructField::new(
            "id",
            crate::spec::DataType::LONG,
            true,
        )])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let metadata = metadata_for_create_with_struct_type(
            schema,
            vec![],
            0,
            HashMap::from([
                (DELTA_UNITY_TABLE_ID_KEY.to_string(), "table-id".to_string()),
                (
                    "delta.enableInCommitTimestamps".to_string(),
                    "true".to_string(),
                ),
                ("delta.checkpointPolicy".to_string(), "v2".to_string()),
                ("team".to_string(), "a".to_string()),
            ]),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let snapshot = crate::physical_plan::metadata_only_snapshot(
            protocol_with_catalog_managed(&crate::spec::Protocol::new(
                3,
                7,
                Some(vec![]),
                Some(vec![]),
            )),
            metadata,
        )?;
        let state = datafusion::prelude::SessionContext::new().state();
        let properties = |items: &[(&str, &str)]| {
            items
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<HashMap<_, _>>()
        };

        let replacement = delta_write_replacement(
            &state,
            &snapshot,
            &properties(&[("comment", "replaced"), ("owner-team", "b")]),
        )?;
        assert_eq!(replacement.description.as_deref(), Some("replaced"));
        assert_eq!(
            replacement.configuration,
            properties(&[
                (DELTA_UNITY_TABLE_ID_KEY, "table-id"),
                ("delta.enableInCommitTimestamps", "true"),
                ("delta.checkpointPolicy", "v2"),
                ("owner-team", "b"),
            ])
        );

        let error = delta_write_replacement(
            &state,
            &snapshot,
            &properties(&[("delta.checkpointPolicy", "classic")]),
        )
        .map(|_| ())
        .map_err(|e| e.to_string());
        assert!(
            error.as_ref().is_err_and(|e| e
                .contains("requires the table property delta.checkpointPolicy=v2")),
            "{error:?}"
        );
        Ok(())
    }

    #[test]
    fn unity_fixed_properties_are_kept() {
        let existing = HashMap::from([
            ("delta.checkpointPolicy".to_string(), "v2".to_string()),
            (
                "delta.enableDeletionVectors".to_string(),
                "true".to_string(),
            ),
        ]);
        let check = |changes: &[(&str, Option<&str>)]| {
            ensure_unity_fixed_properties_kept(&existing, changes.iter().copied())
                .map_err(|e| e.to_string())
        };
        assert!(check(&[]).is_ok());
        assert!(check(&[("delta.checkpointPolicy", Some("v2"))]).is_ok());
        assert!(check(&[("team", Some("a")), ("team", None)]).is_ok());
        // A fixed property that the table does not have can be set.
        assert!(check(&[("delta.checkpoint.writeStatsAsJson", Some("false"))]).is_ok());
        let error = check(&[("DELTA.CHECKPOINTPOLICY", Some("classic"))]);
        assert!(
            error.as_ref().is_err_and(|e| e.contains(
                "requires the table property delta.checkpointPolicy=v2 \
                 for managed Delta tables, but it is set to classic"
            )),
            "{error:?}"
        );
        let error = check(&[("delta.enableDeletionVectors", None)]);
        assert!(
            error.as_ref().is_err_and(
                |e| e.contains("requires the table property delta.enableDeletionVectors=true")
            ),
            "{error:?}"
        );
    }

    #[test]
    fn replace_where_selects_only_column_bearing_conjuncts() {
        let column = col("p").eq(lit(1));
        let null = lit(ScalarValue::Boolean(None));
        for (expr, expected) in [
            (lit(true), vec![]),
            (lit(false), vec![]),
            (null.clone(), vec![]),
            (column.clone().and(lit(false)), vec![column.clone()]),
            (lit(false).and(column.clone()), vec![column.clone()]),
            (column.clone().and(null.clone()), vec![column.clone()]),
            (
                column.clone().or(null.clone()),
                vec![column.clone().or(null)],
            ),
            (
                column.clone().and(col("v").gt(lit(0))),
                vec![column, col("v").gt(lit(0))],
            ),
        ] {
            let condition = ExprWithSource::new(expr, Some("original predicate".to_string()));
            let constraints = replace_where_constraints(&condition);
            assert_eq!(
                constraints
                    .iter()
                    .map(|c| c.expr.clone())
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(
                constraints
                    .iter()
                    .all(|c| c.expression == "original predicate"
                        && c.violation == DeltaConstraintViolation::ReplaceWhere)
            );
        }
    }

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
    fn append_only_protocol_upgrade_respects_legacy_and_table_feature_versions()
    -> crate::spec::DeltaResult<()> {
        let metadata = crate::schema::metadata_for_create_with_struct_type(
            StructType::try_new([StructField::nullable("id", DeltaDataType::INTEGER)])?,
            vec![],
            0,
            HashMap::from([("delta.appendOnly".to_string(), "true".to_string())]),
        )?;
        let desired = Protocol::new(1, 2, None, None);

        let legacy_v2 = append_only_protocol_for_existing_table(
            &Protocol::new(1, 2, None, None),
            &desired,
            &metadata,
        );
        assert_eq!(legacy_v2, desired);

        let upgraded_v1 = append_only_protocol_for_existing_table(
            &Protocol::new(1, 1, None, None),
            &desired,
            &metadata,
        );
        assert_eq!(upgraded_v1.min_writer_version(), 7);
        assert!(upgraded_v1.has_writer_feature(&TableFeature::AppendOnly));

        let existing_v7 = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::V2Checkpoint]),
            Some(vec![TableFeature::V2Checkpoint]),
        );
        let explicit_v7 =
            append_only_protocol_for_existing_table(&existing_v7, &desired, &metadata);
        let (merged, upgraded) = merge_protocol_for_upgrade(&existing_v7, &explicit_v7);
        assert!(upgraded);
        assert!(merged.has_writer_feature(&TableFeature::V2Checkpoint));
        assert!(merged.has_writer_feature(&TableFeature::AppendOnly));
        Ok(())
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

    /// Creates the metadata of a Unity Catalog managed table in memory and returns the result
    /// with the actions of version 0.
    async fn create_unity_managed_table_metadata(
        properties: Vec<(&str, &str)>,
        required_protocol: Option<serde_json::Value>,
    ) -> Result<(LakeSourceCreateTableResult, Vec<serde_json::Value>)> {
        let columns = vec![LakeSourceCreateTableColumn {
            metadata: Default::default(),
            name: "id".to_string(),
            data_type: ArrowDataType::Int64,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            identity: None,
        }];
        create_unity_managed_table_metadata_with_columns(columns, properties, required_protocol)
            .await
    }

    async fn create_unity_managed_table_metadata_with_columns(
        columns: Vec<LakeSourceCreateTableColumn>,
        properties: Vec<(&str, &str)>,
        required_protocol: Option<serde_json::Value>,
    ) -> Result<(LakeSourceCreateTableResult, Vec<serde_json::Value>)> {
        use object_store::ObjectStoreExt;
        use sail_common_datafusion::catalog::{
            CatalogProviderId, CatalogTableIdentity, LakehouseAuthority, LakehouseFormat,
            MetadataPointerAuthority, ScanAuthority, TableLifecycle,
        };

        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime_env = Arc::new(RuntimeEnv::default());
        runtime_env.register_object_store(
            &parse_location_to_url("memory:///staging/t")?,
            store.clone(),
        );
        let context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".to_string()),
            vec!["main".to_string(), "default".to_string(), "t".to_string()],
            CatalogTableIdentity {
                table_id: None,
                table_uri: Some("memory:///staging/t".to_string()),
            },
            LakehouseOperation::Create,
            LakehouseFormat::Delta,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::Managed,
                pointer: MetadataPointerAuthority::DeltaRatifiedCommits,
                commit: CommitAuthority::DeltaRatifiedCommit,
            },
            ScanAuthority::ClientLakeSource,
        );
        let result = DeltaLakeSource
            .create_table_metadata(
                runtime_env,
                &SessionConfig::new(),
                LakeSourceCreateTableInfo {
                    path: "memory:///staging/t".to_string(),
                    columns,
                    comment: None,
                    partition_by: vec![],
                    properties: properties
                        .into_iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                    replace: false,
                    lakehouse_table: Some(context),
                    required_protocol: required_protocol
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                },
            )
            .await?;
        let commit =
            object_store::path::Path::from("staging/t/_delta_log/00000000000000000000.json");
        let actions = match store.get(&commit).await {
            Ok(commit) => {
                let commit = commit.bytes().await?;
                serde_json::Deserializer::from_slice(&commit)
                    .into_iter::<serde_json::Value>()
                    .collect::<serde_json::Result<Vec<_>>>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            }
            Err(object_store::Error::NotFound { .. }) => vec![],
            Err(e) => return Err(e.into()),
        };
        Ok((result, actions))
    }

    fn find_action<'a>(
        actions: &'a [serde_json::Value],
        name: &str,
    ) -> Result<&'a serde_json::Value> {
        actions
            .iter()
            .find_map(|action| action.get(name))
            .ok_or_else(|| DataFusionError::Internal(format!("missing {name} action")))
    }

    fn sorted_strings(value: &serde_json::Value) -> Vec<&str> {
        let mut values = value
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>();
        values.sort();
        values
    }

    #[tokio::test]
    async fn unity_staged_table_version_0_has_required_protocol_and_properties() -> Result<()> {
        let required_protocol = serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged", "deletionVectors", "v2Checkpoint"],
            "writerFeatures": [
                "catalogManaged", "deletionVectors", "inCommitTimestamp", "v2Checkpoint"
            ]
        });
        let (result, actions) = create_unity_managed_table_metadata(
            vec![
                ("owner", "sail"),
                ("io.unitycatalog.tableId", "table-id"),
                ("delta.checkpointPolicy", "v2"),
                ("delta.enableDeletionVectors", "true"),
                ("delta.checkpoint.writeStatsAsStruct", "true"),
                ("delta.checkpoint.writeStatsAsJson", "true"),
                ("delta.feature.appendOnly", "supported"),
            ],
            Some(required_protocol),
        )
        .await?;

        let protocol = find_action(&actions, "protocol")?;
        assert_eq!(protocol["minReaderVersion"], 3);
        assert_eq!(protocol["minWriterVersion"], 7);
        assert_eq!(
            sorted_strings(&protocol["readerFeatures"]),
            vec![
                "catalogManaged",
                "deletionVectors",
                "v2Checkpoint",
                "vacuumProtocolCheck"
            ]
        );
        assert_eq!(
            sorted_strings(&protocol["writerFeatures"]),
            vec![
                "appendOnly",
                "catalogManaged",
                "deletionVectors",
                "inCommitTimestamp",
                "invariants",
                "v2Checkpoint",
                "vacuumProtocolCheck"
            ]
        );
        let metadata = find_action(&actions, "metaData")?;
        assert_eq!(
            metadata["configuration"],
            serde_json::json!({
                "delta.checkpoint.writeStatsAsJson": "true",
                "delta.checkpoint.writeStatsAsStruct": "true",
                "delta.checkpointPolicy": "v2",
                "delta.enableDeletionVectors": "true",
                "delta.enableInCommitTimestamps": "true",
                "io.unitycatalog.tableId": "table-id"
            })
        );
        let in_commit_timestamp = &find_action(&actions, "commitInfo")?["inCommitTimestamp"];
        assert!(in_commit_timestamp.is_i64(), "{in_commit_timestamp}");

        // The result reports version 0 as it is written for registering the table.
        assert!(result.properties.is_empty());
        let Some(create_commit) = result.created_metadata else {
            return plan_err!("missing created metadata");
        };
        fn to_json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value> {
            serde_json::to_value(value).map_err(|e| DataFusionError::External(Box::new(e)))
        }
        assert_eq!(&to_json(&create_commit.protocol)?, protocol);
        assert_eq!(
            Some(create_commit.metadata.schema_string.as_str()),
            metadata["schemaString"].as_str()
        );
        assert_eq!(
            to_json(&create_commit.metadata.partition_columns)?,
            metadata["partitionColumns"]
        );
        assert_eq!(
            to_json(&create_commit.metadata.configuration)?,
            metadata["configuration"]
        );
        assert_eq!(
            create_commit.metadata.description.as_deref(),
            metadata["description"].as_str()
        );
        assert_eq!(
            Some(create_commit.in_commit_timestamp),
            in_commit_timestamp.as_i64()
        );
        Ok(())
    }

    fn staged_table_info(
        properties: Vec<(&str, &str)>,
        required_protocol: serde_json::Value,
    ) -> Result<LakeSourceCreateTableInfo> {
        Ok(LakeSourceCreateTableInfo {
            path: "memory:///staging/t".to_string(),
            columns: vec![LakeSourceCreateTableColumn {
                metadata: Default::default(),
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: true,
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
            }],
            comment: None,
            partition_by: vec![],
            properties: properties
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            replace: false,
            lakehouse_table: None,
            required_protocol: Some(
                serde_json::from_value(required_protocol)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ),
        })
    }

    /// The first commit of a write to a staged table creates the table with the protocol
    /// and the metadata that CREATE TABLE creates it with.
    #[tokio::test]
    async fn unity_staged_table_write_has_the_protocol_and_metadata_of_create_table() -> Result<()>
    {
        let required_protocol = serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged", "deletionVectors", "v2Checkpoint"],
            "writerFeatures": [
                "catalogManaged", "deletionVectors", "inCommitTimestamp", "v2Checkpoint"
            ]
        });
        let properties = vec![
            ("owner", "sail"),
            ("io.unitycatalog.tableId", "table-id"),
            ("delta.checkpointPolicy", "v2"),
            ("delta.enableDeletionVectors", "true"),
            ("delta.feature.appendOnly", "supported"),
        ];
        let (_, actions) = create_unity_managed_table_metadata(
            properties.clone(),
            Some(required_protocol.clone()),
        )
        .await?;
        let (protocol, metadata) = staged_table_protocol_and_metadata(
            &SessionConfig::new(),
            &staged_table_info(properties, required_protocol)?,
        )?;

        let protocol =
            serde_json::to_value(&protocol).map_err(|e| DataFusionError::External(Box::new(e)))?;
        assert_eq!(&protocol, find_action(&actions, "protocol")?);
        let created = find_action(&actions, "metaData")?;
        let metadata =
            serde_json::to_value(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;
        for key in [
            "schemaString",
            "partitionColumns",
            "configuration",
            "description",
        ] {
            assert_eq!(metadata.get(key), created.get(key), "{key}");
        }
        Ok(())
    }

    /// Version 0 of a staged table that is written with the data of the write reports the
    /// row ID high-water mark of the data, which the catalog registers the table with.
    #[tokio::test]
    async fn unity_staged_table_version_0_with_data_reports_the_row_id_high_water_mark()
    -> Result<()> {
        let required_protocol = serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged"],
            "writerFeatures": ["catalogManaged", "domainMetadata", "inCommitTimestamp", "rowTracking"]
        });
        let (protocol, metadata) = staged_table_protocol_and_metadata(
            &SessionConfig::new(),
            &staged_table_info(
                vec![
                    ("io.unitycatalog.tableId", "table-id"),
                    ("delta.enableRowTracking", "true"),
                ],
                required_protocol,
            )?,
        )?;
        let table_url = parse_location_to_url("memory:///staging/t")?;
        let store = Arc::new(object_store::memory::InMemory::new());
        let log_store =
            create_delta_table_with_object_store(table_url.clone(), store, Default::default())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .log_store();
        let add = |path: &str, rows: i64| {
            CommitAction::Add(crate::spec::Add {
                path: path.to_string(),
                size: 1,
                data_change: true,
                stats: Some(format!(r#"{{"numRecords":{rows}}}"#)),
                ..Default::default()
            })
        };
        let operation = DeltaOperation::Create {
            mode: SaveMode::ErrorIfExists,
            location: table_url.to_string(),
            protocol: Box::new(protocol.clone()),
            metadata: Box::new(metadata.clone()),
        };
        CommitBuilder::from(CommitProperties::default())
            .with_actions(vec![
                CommitAction::Protocol(protocol),
                CommitAction::Metadata(metadata),
                add("a.parquet", 2),
                add("b.parquet", 3),
            ])
            .build(None, log_store.clone(), operation)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let created = unity_create_commit(log_store.as_ref()).await?;
        assert_eq!(created.row_id_high_water_mark, Some(4));
        assert!(
            created
                .protocol
                .writer_features
                .is_some_and(|features| features.iter().any(|feature| feature == "rowTracking"))
        );
        assert_eq!(
            created
                .metadata
                .configuration
                .get("io.unitycatalog.tableId"),
            Some(&"table-id".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn unity_staged_table_with_required_row_tracking() -> Result<()> {
        let required_protocol = serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged"],
            "writerFeatures": ["catalogManaged", "domainMetadata", "inCommitTimestamp", "rowTracking"]
        });
        let (_, actions) = create_unity_managed_table_metadata(
            vec![
                ("io.unitycatalog.tableId", "table-id"),
                ("delta.enableRowTracking", "true"),
            ],
            Some(required_protocol),
        )
        .await?;
        let writer_features = &find_action(&actions, "protocol")?["writerFeatures"];
        let writer_features = sorted_strings(writer_features);
        assert!(
            writer_features.contains(&"rowTracking"),
            "{writer_features:?}"
        );
        assert!(
            writer_features.contains(&"domainMetadata"),
            "{writer_features:?}"
        );
        let configuration = &find_action(&actions, "metaData")?["configuration"];
        assert_eq!(configuration["delta.enableRowTracking"], "true");
        for (key, prefix) in [
            (
                "delta.rowTracking.materializedRowIdColumnName",
                "_row-id-col-",
            ),
            (
                "delta.rowTracking.materializedRowCommitVersionColumnName",
                "_row-commit-version-col-",
            ),
        ] {
            assert!(
                configuration[key]
                    .as_str()
                    .is_some_and(|name| name.starts_with(prefix)),
                "{configuration}"
            );
        }
        Ok(())
    }

    /// A staged table is created with the column mapping of the schema that the data of
    /// the write is written with.
    #[tokio::test]
    async fn unity_staged_table_has_the_column_mapping_of_the_write() -> Result<()> {
        let properties = vec![
            ("io.unitycatalog.tableId", "table-id"),
            ("delta.columnMapping.mode", "name"),
        ];
        let point = ArrowDataType::Struct(
            vec![
                Field::new("x", ArrowDataType::Int64, true),
                Field::new("y", ArrowDataType::Utf8, true),
            ]
            .into(),
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, true),
            Field::new("point", point, true),
        ]));
        let Some(write_schema) = DeltaLakeSource.create_table_schema_for_write(
            &schema,
            &properties
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<Vec<_>>(),
        )?
        else {
            return plan_err!("expected a schema with column mapping");
        };
        let columns = write_schema
            .fields()
            .iter()
            .map(|field| LakeSourceCreateTableColumn {
                metadata: field
                    .metadata()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
            })
            .collect();
        let required_protocol = serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged"],
            "writerFeatures": ["catalogManaged", "inCommitTimestamp"]
        });
        let (_, actions) = create_unity_managed_table_metadata_with_columns(
            columns,
            properties,
            Some(required_protocol),
        )
        .await?;
        let metadata = find_action(&actions, "metaData")?;
        assert_eq!(
            metadata["configuration"]["delta.columnMapping.mode"],
            "name"
        );
        assert_eq!(
            metadata["configuration"]["delta.columnMapping.maxColumnId"],
            "4"
        );
        let table = serde_json::from_str::<StructType>(
            metadata["schemaString"].as_str().unwrap_or_default(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let written = StructType::try_from(write_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        assert_eq!(table, written);
        Ok(())
    }

    /// A required protocol whose table features do not match its versions is rejected,
    /// rather than merged without the table features.
    #[tokio::test]
    async fn unity_staged_table_rejects_invalid_required_protocol() -> Result<()> {
        for required_protocol in [
            serde_json::json!({
                "minReaderVersion": 2,
                "minWriterVersion": 5,
                "readerFeatures": ["futureFeature"]
            }),
            serde_json::json!({
                "minReaderVersion": 3,
                "minWriterVersion": 5,
                "readerFeatures": ["futureFeature"]
            }),
            serde_json::json!({
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "readerFeatures": ["futureFeature"],
                "writerFeatures": ["futureFeature"]
            }),
            serde_json::json!({
                "minReaderVersion": 1,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": []
            }),
            serde_json::json!({
                "minReaderVersion": 1,
                "minWriterVersion": 2,
                "readerFeatures": [],
                "writerFeatures": []
            }),
        ] {
            let result = create_unity_managed_table_metadata(
                vec![("io.unitycatalog.tableId", "table-id")],
                Some(required_protocol.clone()),
            )
            .await;
            let Err(error) = result else {
                return plan_err!("expected an error for {required_protocol}");
            };
            assert!(matches!(error, DataFusionError::Plan(_)), "{error:?}");
            assert!(
                error
                    .to_string()
                    .contains("invalid Delta protocol required by Unity Catalog"),
                "{error}"
            );
        }

        // A legacy protocol without table features is valid.
        let (result, actions) = create_unity_managed_table_metadata(
            vec![("io.unitycatalog.tableId", "table-id")],
            Some(serde_json::json!({"minReaderVersion": 1, "minWriterVersion": 2})),
        )
        .await?;
        assert!(result.created_metadata.is_some());
        let protocol = find_action(&actions, "protocol")?;
        assert_eq!(protocol["minReaderVersion"], 3);
        assert_eq!(protocol["minWriterVersion"], 7);
        Ok(())
    }

    #[tokio::test]
    async fn unity_staged_table_rejects_unsupported_required_features() -> Result<()> {
        for (features, unsupported) in [
            (vec!["catalogManaged", "clustering"], "clustering"),
            (vec!["catalogManaged", "futureFeature"], "futureFeature"),
        ] {
            let required_protocol = serde_json::json!({
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["catalogManaged"],
                "writerFeatures": features
            });
            let result = create_unity_managed_table_metadata(
                vec![("io.unitycatalog.tableId", "table-id")],
                Some(required_protocol),
            )
            .await;
            let Err(error) = result else {
                return plan_err!("expected an error for {unsupported}");
            };
            assert!(
                matches!(error, DataFusionError::NotImplemented(_)),
                "{error:?}"
            );
            assert!(
                error.to_string().contains(&format!(
                    "Delta table features required by Unity Catalog: {unsupported}"
                )),
                "{error}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn unity_managed_table_version_0_without_required_protocol() -> Result<()> {
        let (result, actions) = create_unity_managed_table_metadata(
            vec![("io.unitycatalog.tableId", "table-id")],
            None,
        )
        .await?;
        assert!(result.properties.is_empty());
        assert!(result.created_metadata.is_none());
        let protocol = find_action(&actions, "protocol")?;
        assert_eq!(
            sorted_strings(&protocol["readerFeatures"]),
            vec!["catalogManaged", "vacuumProtocolCheck"]
        );
        assert_eq!(
            find_action(&actions, "metaData")?["configuration"],
            serde_json::json!({
                "delta.enableInCommitTimestamps": "true",
                "delta.feature.catalogManaged": "supported",
                "io.unitycatalog.tableId": "table-id"
            })
        );
        Ok(())
    }

    /// REPLACE can enable row tracking on a table without it, and a table with row tracking
    /// keeps the feature and the names of its materialized columns, as in Delta Spark.
    #[tokio::test]
    async fn replace_enables_and_retains_row_tracking() -> Result<()> {
        use object_store::ObjectStoreExt;

        let location = "memory:///path/t";
        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime_env = Arc::new(RuntimeEnv::default());
        runtime_env.register_object_store(&parse_location_to_url(location)?, store.clone());
        let info = |properties: &[(&str, &str)], replace| LakeSourceCreateTableInfo {
            path: location.to_string(),
            columns: vec![LakeSourceCreateTableColumn {
                metadata: Default::default(),
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: true,
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
            }],
            comment: None,
            partition_by: vec![],
            properties: properties
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            replace,
            lakehouse_table: None,
            required_protocol: None,
        };
        let commit = |version: i64| {
            let store = store.clone();
            async move {
                let commit = store
                    .get(&object_store::path::Path::from(format!(
                        "path/t/_delta_log/{version:020}.json"
                    )))
                    .await?
                    .bytes()
                    .await?;
                serde_json::Deserializer::from_slice(&commit)
                    .into_iter::<serde_json::Value>()
                    .collect::<serde_json::Result<Vec<_>>>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            }
        };
        let replace = |properties: &'static [(&'static str, &'static str)]| {
            let runtime_env = runtime_env.clone();
            async move {
                DeltaLakeSource
                    .create_table_metadata(
                        runtime_env,
                        &SessionConfig::new(),
                        info(properties, true),
                    )
                    .await
            }
        };
        let row_id_column = "delta.rowTracking.materializedRowIdColumnName";
        let row_commit_version_column = "delta.rowTracking.materializedRowCommitVersionColumnName";

        DeltaLakeSource
            .create_table_metadata(runtime_env.clone(), &SessionConfig::new(), info(&[], false))
            .await?;
        replace(&[("delta.enableRowTracking", "true")]).await?;
        let actions = commit(1).await?;
        let protocol = find_action(&actions, "protocol")?;
        assert_eq!(protocol["minWriterVersion"], 7);
        let writer_features = sorted_strings(&protocol["writerFeatures"]);
        assert!(
            writer_features.contains(&"rowTracking") && writer_features.contains(&"domainMetadata"),
            "{writer_features:?}"
        );
        let configuration = find_action(&actions, "metaData")?["configuration"].clone();
        assert_eq!(configuration["delta.enableRowTracking"], "true");
        assert!(
            configuration[row_id_column]
                .as_str()
                .is_some_and(|name| name.starts_with("_row-id-col-"))
        );
        assert!(
            configuration[row_commit_version_column]
                .as_str()
                .is_some_and(|name| name.starts_with("_row-commit-version-col-"))
        );
        // A REPLACE without data has no row IDs to record.
        assert!(
            actions
                .iter()
                .all(|action| action.get("domainMetadata").is_none())
        );

        for (version, properties, enabled) in [
            (2, &[][..], "true"),
            (3, &[("delta.enableRowTracking", "false")][..], "false"),
        ] {
            replace(properties).await?;
            let actions = commit(version).await?;
            // The protocol keeps row tracking, so the commit has no protocol action.
            assert!(
                actions
                    .iter()
                    .all(|action| action.get("protocol").is_none())
            );
            let replaced = &find_action(&actions, "metaData")?["configuration"];
            assert_eq!(replaced["delta.enableRowTracking"], enabled);
            assert_eq!(replaced[row_id_column], configuration[row_id_column]);
            assert_eq!(
                replaced[row_commit_version_column],
                configuration[row_commit_version_column]
            );
        }
        Ok(())
    }

    /// REPLACE of a table with column mapping gives the columns new IDs after the IDs
    /// that the table has used, and new physical names, as in Delta Spark.
    #[tokio::test]
    async fn replace_continues_the_column_ids_of_the_table() -> Result<()> {
        use object_store::ObjectStoreExt;

        let location = "memory:///path/t";
        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime_env = Arc::new(RuntimeEnv::default());
        runtime_env.register_object_store(&parse_location_to_url(location)?, store.clone());
        let column = |name: &str| LakeSourceCreateTableColumn {
            metadata: Default::default(),
            name: name.to_string(),
            data_type: ArrowDataType::Int64,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            identity: None,
        };
        let info = |columns, replace| LakeSourceCreateTableInfo {
            path: location.to_string(),
            columns,
            comment: None,
            partition_by: vec![],
            properties: vec![("delta.columnMapping.mode".to_string(), "name".to_string())],
            replace,
            lakehouse_table: None,
            required_protocol: None,
        };
        let schema = |version: i64| {
            let store = store.clone();
            async move {
                let commit = store
                    .get(&object_store::path::Path::from(format!(
                        "path/t/_delta_log/{version:020}.json"
                    )))
                    .await?
                    .bytes()
                    .await?;
                let actions = serde_json::Deserializer::from_slice(&commit)
                    .into_iter::<serde_json::Value>()
                    .collect::<serde_json::Result<Vec<_>>>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let metadata = find_action(&actions, "metaData")?.clone();
                let schema = serde_json::from_str::<StructType>(
                    metadata["schemaString"].as_str().unwrap_or_default(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let columns = schema
                    .fields()
                    .map(|field| {
                        let id = match field.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
                            Some(MetadataValue::Number(id)) => *id,
                            _ => -1,
                        };
                        let physical_name =
                            field.physical_name(ColumnMappingMode::Name).to_string();
                        (field.name().clone(), id, physical_name)
                    })
                    .collect::<Vec<_>>();
                Ok::<_, DataFusionError>((
                    columns,
                    metadata["configuration"]["delta.columnMapping.maxColumnId"].clone(),
                ))
            }
        };

        DeltaLakeSource
            .create_table_metadata(
                runtime_env.clone(),
                &SessionConfig::new(),
                info(vec![column("a"), column("b")], false),
            )
            .await?;
        let (created, max_column_id) = schema(0).await?;
        assert_eq!(max_column_id, "2");
        DeltaLakeSource
            .create_table_metadata(
                runtime_env,
                &SessionConfig::new(),
                info(vec![column("b"), column("a")], true),
            )
            .await?;
        let (replaced, max_column_id) = schema(1).await?;
        assert_eq!(max_column_id, "4");
        assert_eq!(
            replaced
                .iter()
                .map(|(name, id, _)| (name.as_str(), *id))
                .collect::<Vec<_>>(),
            vec![("b", 3), ("a", 4)]
        );
        for (_, _, physical_name) in &replaced {
            assert!(physical_name.starts_with("col-"), "{physical_name}");
            assert!(
                created
                    .iter()
                    .all(|(_, _, created)| created != physical_name),
                "{physical_name}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn replace_without_catalog_is_refused_for_catalog_managed_table() -> Result<()> {
        use object_store::ObjectStore;
        use sail_common_datafusion::catalog::{
            CatalogProviderId, CatalogTableIdentity, LakehouseAuthority, LakehouseFormat,
            MetadataPointerAuthority, ScanAuthority, TableLifecycle,
        };

        let location = "memory:///managed/t";
        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime_env = Arc::new(RuntimeEnv::default());
        runtime_env.register_object_store(&parse_location_to_url(location)?, store.clone());
        let context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".to_string()),
            vec!["main".to_string(), "default".to_string(), "t".to_string()],
            CatalogTableIdentity {
                table_id: None,
                table_uri: Some(location.to_string()),
            },
            LakehouseOperation::Create,
            LakehouseFormat::Delta,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::Managed,
                pointer: MetadataPointerAuthority::DeltaRatifiedCommits,
                commit: CommitAuthority::DeltaRatifiedCommit,
            },
            ScanAuthority::ClientLakeSource,
        );
        let info = |replace, lakehouse_table| LakeSourceCreateTableInfo {
            path: location.to_string(),
            columns: vec![LakeSourceCreateTableColumn {
                metadata: Default::default(),
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: true,
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
            }],
            comment: None,
            partition_by: vec![],
            properties: vec![(
                "io.unitycatalog.tableId".to_string(),
                "table-id".to_string(),
            )],
            replace,
            lakehouse_table,
            required_protocol: None,
        };
        DeltaLakeSource
            .create_table_metadata(
                runtime_env.clone(),
                &SessionConfig::new(),
                info(false, Some(context)),
            )
            .await?;

        // A replacement that does not go through the catalog must not commit to the log.
        let result = DeltaLakeSource
            .create_table_metadata(runtime_env, &SessionConfig::new(), info(true, None))
            .await;
        let error = match result {
            Ok(_) => return plan_err!("the replacement is expected to fail"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("catalog-managed Delta tables"), "{error}");
        let commits = store
            .list_with_delimiter(Some(&object_store::path::Path::from(
                "managed/t/_delta_log",
            )))
            .await?
            .objects
            .into_iter()
            .map(|object| object.location.to_string())
            .filter(|location| location.ends_with(".json"))
            .collect::<Vec<_>>();
        assert_eq!(
            commits,
            vec!["managed/t/_delta_log/00000000000000000000.json"]
        );
        Ok(())
    }
}
