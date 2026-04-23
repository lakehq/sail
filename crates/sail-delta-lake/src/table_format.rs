use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, plan_err, DFSchema, DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::column_features::ColumnFeatures;
use sail_common_datafusion::datasource::{
    MergeStrategy, OptionLayer, PhysicalSinkMode, RowLevelCommand, RowLevelWriteInfo, SinkInfo,
    SourceInfo, TableFormat, TableFormatRegistry,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;
use sail_data_source::error::DataSourceResult;
use sail_data_source::options::gen::{
    DeltaReadOptions, DeltaReadPartialOptions, DeltaWriteOptions, DeltaWritePartialOptions,
};
use sail_data_source::options::{BuildPartialOptions, PartialOptions};
use sail_data_source::resolve_listing_urls;
use url::Url;

use crate::kernel::DeltaSnapshotConfig;
use crate::physical_plan::planner::{
    plan_delete, plan_delete_mor, plan_merge, DeltaPhysicalPlanner, DeltaPlannerConfig,
    PlannerContext,
};
use crate::spec::{
    canonicalize_and_validate_table_properties, route_table_property_key, CommitAction,
    DeltaOperation,
};
use crate::table::{open_table_with_object_store, open_table_with_object_store_and_table_config};
use crate::{create_delta_provider, create_delta_source, DeltaTableError};

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
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = resolve_delta_read_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        create_delta_source(ctx, table_url, schema, options).await
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        let options = resolve_delta_read_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        create_delta_provider(ctx, table_url, schema, options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let path = info.path();
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            table_properties,
            options,
            logical_schema,
        } = info;

        if is_flow_event_schema(&input.schema()) {
            return not_impl_err!("writing streaming data to Delta table");
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for Delta format");
        }
        let partition_by = partition_by
            .into_iter()
            .map(|field| field.column)
            .collect::<Vec<_>>();

        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        let (options, routed_table_properties) =
            split_delta_write_options_and_table_properties(options);
        let delta_options = resolve_delta_write_options(options)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = match open_table_with_object_store_and_table_config(
            table_url.clone(),
            object_store,
            Default::default(),
            // Only partition columns and table existence are needed at planning time;
            // skip replaying Add/Remove file actions which are not used here.
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
        let table_exists = table.is_some();
        let mut metadata_configuration = resolve_delta_metadata_configuration(&table_properties)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if table_exists {
            if !routed_table_properties.is_empty() {
                let mut keys: Vec<_> = routed_table_properties.keys().cloned().collect();
                keys.sort();
                log::warn!(
                    "ignoring write-time Delta table properties for existing table at {table_url}: {}",
                    keys.join(", ")
                );
            }
        } else {
            let routed_metadata_configuration =
                resolve_delta_metadata_configuration(&routed_table_properties)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            metadata_configuration.extend(routed_metadata_configuration);
        }

        match mode {
            PhysicalSinkMode::ErrorIfExists if table_exists => {
                return plan_err!("Delta table already exists at path: {table_url}");
            }
            PhysicalSinkMode::IgnoreIfExists if table_exists => {
                return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    input.schema(),
                )));
            }
            PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("unsupported sink mode for Delta: {mode:?}")
            }
            _ => {}
        }

        let unified_mode = mode;
        let table_schema_for_cond = None;

        // Get existing partition columns from table metadata if available
        let existing_partition_columns = if let Some(table) = &table {
            Some(
                table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .metadata()
                    .partition_columns()
                    .clone(),
            )
        } else {
            None
        };

        // Validate partition column mismatch for append/overwrite operations
        if let Some(existing_partitions) = &existing_partition_columns {
            if !partition_by.is_empty() && partition_by != *existing_partitions {
                // Allow partition column changes only when overwriting with schema changes
                // For append mode, this is always an error
                match unified_mode {
                    PhysicalSinkMode::Append => {
                        return plan_err!(
                            "Partition column mismatch. Table is partitioned by {:?}, but write specified {:?}. \
                            Cannot change partitioning on append.",
                            existing_partitions,
                            partition_by
                        );
                    }
                    PhysicalSinkMode::Overwrite | PhysicalSinkMode::OverwriteIf { .. }
                        // For overwrite mode, check if schema overwrite is allowed
                        if !delta_options.overwrite_schema => {
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

        let partition_columns = if !partition_by.is_empty() {
            partition_by
        } else {
            existing_partition_columns.unwrap_or_default()
        };

        let table_config = DeltaPlannerConfig::new(
            table_url,
            delta_options,
            metadata_configuration,
            partition_columns,
            table_schema_for_cond,
            table_exists,
        )
        .with_generation_expressions(extract_generation_expressions(logical_schema.as_deref()));
        let planner_ctx = PlannerContext::new(ctx, table_config);
        let planner = DeltaPhysicalPlanner::new(planner_ctx);
        let sink_exec = planner.create_plan(input, unified_mode, sort_order).await?;

        Ok(sink_exec)
    }

    async fn create_row_level_writer(
        &self,
        ctx: &dyn Session,
        info: RowLevelWriteInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Determine the actual strategy: if the table has deletion vectors enabled,
        // override to MergeOnRead for DELETE operations. The trait-level merge_strategy()
        // only provides a default hint; here we inspect the actual table properties.
        let effective_strategy = if info.command == RowLevelCommand::Delete {
            detect_merge_strategy(ctx, &info)
                .await
                .unwrap_or(info.merge_strategy)
        } else {
            info.merge_strategy
        };

        match (effective_strategy, info.command) {
            // ── Merge-on-Read DELETE ──────────────────────────────────────────
            (MergeStrategy::MergeOnRead, RowLevelCommand::Delete) => {
                let table_url = Self::parse_table_url(ctx, vec![info.target.path]).await?;
                let condition = info.condition.ok_or_else(|| {
                    DataFusionError::Plan("DELETE operation requires a WHERE condition".to_string())
                })?;
                let delta_options = resolve_delta_write_options(info.target.options)?;
                let delete_config = DeltaPlannerConfig::new(
                    table_url,
                    delta_options,
                    HashMap::new(),
                    Vec::new(),
                    None,
                    true,
                );
                let delete_ctx = PlannerContext::new(ctx, delete_config);
                plan_delete_mor(&delete_ctx, condition).await
            }
            // ── MoR for MERGE/UPDATE: not yet implemented ────────────────────
            (MergeStrategy::MergeOnRead, RowLevelCommand::Merge) => {
                not_impl_err!(
                    "Merge-on-Read strategy for MERGE is not yet implemented for Delta Lake"
                )
            }
            (MergeStrategy::MergeOnRead, RowLevelCommand::Update) => {
                not_impl_err!(
                    "Merge-on-Read strategy for UPDATE is not yet implemented for Delta Lake"
                )
            }
            // ── Copy-on-Write DELETE ─────────────────────────────────────────
            (MergeStrategy::Eager, RowLevelCommand::Delete) => {
                let table_url = Self::parse_table_url(ctx, vec![info.target.path]).await?;
                let condition = info.condition.ok_or_else(|| {
                    DataFusionError::Plan("DELETE operation requires a WHERE condition".to_string())
                })?;
                let delta_options = resolve_delta_write_options(info.target.options)?;
                let delete_config = DeltaPlannerConfig::new(
                    table_url,
                    delta_options,
                    HashMap::new(),
                    Vec::new(),
                    None,
                    true,
                );
                let delete_ctx = PlannerContext::new(ctx, delete_config);
                plan_delete(&delete_ctx, condition).await
            }
            // ── Copy-on-Write MERGE ──────────────────────────────────────────
            (MergeStrategy::Eager, RowLevelCommand::Merge) => {
                let table_url = Self::parse_table_url(ctx, vec![info.target.path.clone()]).await?;
                let delta_options = resolve_delta_write_options(info.target.options.clone())?;
                let merge_config = DeltaPlannerConfig::new(
                    table_url,
                    delta_options,
                    HashMap::new(),
                    info.target.partition_by.clone(),
                    None,
                    true,
                );
                let merge_ctx = PlannerContext::new(ctx, merge_config);
                plan_merge(&merge_ctx, info).await
            }
            (_, RowLevelCommand::Update) => {
                not_impl_err!("UPDATE is not yet implemented for Delta Lake")
            }
        }
    }

    async fn alter_table_properties(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<()> {
        use crate::kernel::transaction::CommitBuilder;
        use crate::schema::manager::protocol_for_create;
        use crate::spec::TableProperties;

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
        let new_config = new_metadata.configuration().clone();
        let desired_protocol = protocol_for_create(
            false,
            false,
            TableProperties::from(new_config.iter()).enable_in_commit_timestamps(),
            false,
            &new_config,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

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

    async fn create_table(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        info: sail_common_datafusion::datasource::CreateTableInfo,
    ) -> Result<()> {
        use chrono::Utc;

        use crate::kernel::transaction::CommitBuilder;
        use crate::kernel::SaveMode;
        use crate::physical_plan::inject_generation_expressions;
        use crate::schema::{
            annotate_for_column_mapping, compute_max_column_id,
            metadata_for_create_with_struct_type, normalize_delta_schema, protocol_for_create,
            schema_has_generated_columns,
        };
        use crate::spec::{
            contains_timestampntz_arrow, Action, ColumnMappingMode, StructType, TableProperties,
        };
        use crate::table::create_delta_table_with_object_store;

        let sail_common_datafusion::datasource::CreateTableInfo {
            path,
            schema,
            partition_by,
            properties,
            if_not_exists,
            replace,
            generated_columns,
        } = info;

        let url = parse_location_to_url(&path)?;
        let object_store = runtime_env
            .object_store_registry
            .get_store(&url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Detect whether a Delta table already exists at this location.
        // When REPLACE is requested, load the full snapshot so we can tombstone
        // the existing file actions.
        let existing_snapshot_config = DeltaSnapshotConfig {
            require_files: replace,
            ..Default::default()
        };
        let existing_table = match open_table_with_object_store_and_table_config(
            url.clone(),
            object_store.clone(),
            Default::default(),
            existing_snapshot_config,
        )
        .await
        {
            Ok(table) => Some(table),
            Err(DeltaTableError::InvalidTableLocation(_))
            | Err(DeltaTableError::FileNotFound(_)) => None,
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };

        // Canonicalize properties before building Metadata/Protocol.
        let configuration = canonicalize_and_validate_table_properties(
            properties.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Fast-path: adopt an existing table when the caller only asked us to
        // ensure it exists (IF NOT EXISTS), or did not declare any schema
        // (external table with no column list). If a schema IS declared,
        // validate that it matches the on-disk schema.
        if let Some(table) = &existing_table {
            if !replace {
                if !if_not_exists && !schema.fields().is_empty() {
                    let existing_schema = table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .arrow_schema()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    if !delta_schemas_compatible(&existing_schema, &schema) {
                        return plan_err!(
                            "Delta table already exists at {path} with a different schema"
                        );
                    }
                }
                // Adopt the existing table.
                return Ok(());
            }
        }

        // Build Protocol + Metadata for the new (or replaced) table.
        let normalized = normalize_delta_schema(&schema);
        let has_timestamp_ntz = contains_timestampntz_arrow(normalized.as_ref());
        let mut kernel_schema = StructType::try_from(normalized.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if !generated_columns.is_empty() {
            kernel_schema = inject_generation_expressions(kernel_schema, &generated_columns);
        }

        let effective_mode = configuration
            .get("delta.columnMapping.mode")
            .and_then(|v| ColumnMappingMode::try_from(v.as_str()).ok())
            .unwrap_or_default();

        let mut configuration = configuration;
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

        let protocol = protocol_for_create(
            !matches!(effective_mode, ColumnMappingMode::None),
            has_timestamp_ntz,
            TableProperties::from(configuration.iter()).enable_in_commit_timestamps(),
            schema_has_generated_columns(&metadata_schema),
            &configuration,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let metadata = metadata_for_create_with_struct_type(
            metadata_schema,
            partition_by.clone(),
            Utc::now().timestamp_millis(),
            configuration,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut actions: Vec<CommitAction> = vec![
            CommitAction::Protocol(protocol.clone()),
            CommitAction::Metadata(metadata.clone()),
        ];

        // REPLACE: tombstone all existing active files before writing the new metadata.
        let (save_mode, reference, log_store) = match existing_table {
            Some(table) if replace => {
                let snapshot = table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .clone();
                let deletion_timestamp = Utc::now().timestamp_millis();
                for add in snapshot.adds() {
                    actions.push(CommitAction::Remove(
                        add.clone().into_remove(deletion_timestamp),
                    ));
                }
                (SaveMode::Overwrite, Some(snapshot), table.log_store())
            }
            Some(_) => {
                // This path should not be reachable given the earlier fast-path,
                // but being defensive for `replace=false` + existing: adopt.
                return Ok(());
            }
            None => {
                let fresh = create_delta_table_with_object_store(
                    url.clone(),
                    object_store,
                    Default::default(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                (SaveMode::ErrorIfExists, None, fresh.log_store())
            }
        };

        let operation = DeltaOperation::Create {
            mode: save_mode,
            location: url.to_string(),
            protocol: Box::new(protocol),
            metadata: Box::new(metadata),
        };
        // Silence unused-import warnings when Action is only referenced transitively.
        let _ = std::marker::PhantomData::<Action>;

        CommitBuilder::default()
            .with_actions(actions)
            .build(reference, log_store, operation)
            .await
            .map(|_| ())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }
}

/// Returns true if the declared `new` Arrow schema is compatible with the
/// `existing` Delta table schema. Two schemas are considered compatible when
/// they have the same top-level columns with matching names (case-sensitive) and
/// matching data types. Nullability of declared fields may be looser than the
/// on-disk fields (Spark allows declaring a NOT NULL column as nullable and
/// vice versa at `CREATE TABLE` time). Nested struct fields and partitioning
/// checks are deferred to future iterations.
fn delta_schemas_compatible(
    existing: &datafusion::arrow::datatypes::Schema,
    declared: &datafusion::arrow::datatypes::Schema,
) -> bool {
    if existing.fields().len() != declared.fields().len() {
        return false;
    }
    for (a, b) in existing.fields().iter().zip(declared.fields().iter()) {
        if a.name() != b.name() {
            return false;
        }
        if a.data_type() != b.data_type() {
            return false;
        }
    }
    true
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
fn parse_location_to_url(path: &str) -> Result<Url> {
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
    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        let mut urls = resolve_listing_urls(ctx, paths.clone()).await?;
        match (urls.pop(), urls.is_empty()) {
            (Some(path), true) => Ok(<ListingTableUrl as AsRef<Url>>::as_ref(&path).clone()),
            _ => plan_err!("expected a single path for Delta table sink: {paths:?}"),
        }
    }
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

/// Detect the merge strategy for a Delta table by inspecting its snapshot properties.
///
/// Returns `MergeOnRead` if the table has deletion vectors enabled in both protocol features
/// and table properties. Otherwise returns `Eager` (Copy-on-Write).
async fn detect_merge_strategy(
    ctx: &dyn Session,
    info: &RowLevelWriteInfo,
) -> Result<MergeStrategy> {
    let mut urls = resolve_listing_urls(ctx, vec![info.target.path.clone()]).await?;
    let table_url = match (urls.pop(), urls.is_empty()) {
        (Some(path), true) => <ListingTableUrl as AsRef<Url>>::as_ref(&path).clone(),
        _ => return Ok(MergeStrategy::Eager),
    };
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(&table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    match open_table_with_object_store(table_url, object_store, Default::default()).await {
        Ok(table) => {
            let snapshot = table
                .snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if snapshot.verify_deletion_vectors().is_ok() {
                Ok(MergeStrategy::MergeOnRead)
            } else {
                Ok(MergeStrategy::Eager)
            }
        }
        Err(_) => Ok(MergeStrategy::Eager),
    }
}

pub fn resolve_delta_read_options(options: Vec<OptionLayer>) -> DataSourceResult<DeltaReadOptions> {
    let mut partial = DeltaReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

pub fn resolve_delta_write_options(
    options: Vec<OptionLayer>,
) -> DataSourceResult<DeltaWriteOptions> {
    let mut partial = DeltaWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
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

fn split_delta_write_options_and_table_properties(
    options: Vec<OptionLayer>,
) -> (Vec<OptionLayer>, HashMap<String, String>) {
    let mut table_properties = HashMap::new();
    let clean_options = options
        .into_iter()
        .map(|layer| match layer {
            OptionLayer::OptionList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else {
                        clean_items.push((key, value));
                    }
                }
                OptionLayer::OptionList { items: clean_items }
            }
            OptionLayer::TablePropertyList { items } => {
                let mut clean_items = Vec::with_capacity(items.len());
                for (key, value) in items {
                    if let Some(property_key) = route_table_property_key(&key) {
                        table_properties.insert(property_key, value);
                    } else {
                        clean_items.push((key, value));
                    }
                }
                OptionLayer::TablePropertyList { items: clean_items }
            }
            other => other,
        })
        .collect();
    (clean_options, table_properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_delta_write_options_and_table_properties() {
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
            split_delta_write_options_and_table_properties(options);

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
    }
}
