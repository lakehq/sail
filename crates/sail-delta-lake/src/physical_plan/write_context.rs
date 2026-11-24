// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::column_features::SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY;
use sail_common_datafusion::datasource::{
    MERGE_SOURCE_METRIC_COLUMN, OPERATION_COLUMN, PhysicalSinkMode,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::conversion::DeltaTypeConverter;
use crate::delta_log::LogStore;
use crate::physical_plan::writer_options::DeltaWriterExecOptions;
use crate::schema::{
    add_type_widening_metadata, annotate_for_column_mapping,
    annotate_new_fields_for_column_mapping, collect_type_changes, column_mapping_metadata_only,
    compute_max_column_id, evolve_schema, format_type_change_path, get_physical_schema,
    inject_default_expressions, inject_generation_expressions, inject_identity_columns,
    is_supported_type_change_for_schema_evolution, metadata_for_create_with_struct_type,
    normalize_delta_schema, protocol_can_write_type_widening, protocol_for_metadata,
    schema_contains_type_widening_metadata, strip_column_mapping_metadata,
};
use crate::snapshot::DeltaSnapshotConfig;
use crate::spec::{
    Action, ColumnMappingMode, DeltaOperation, DomainMetadata, Metadata, Protocol, SaveMode,
    StructType, Transaction,
};
use crate::table::DeltaSnapshot;

/// Metadata-only table state pinned during coordinator-side write planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSnapshotContext {
    pub version: i64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub txns: HashMap<String, Transaction>,
    pub domain_metadata: HashMap<String, DomainMetadata>,
    pub commit_timestamps: BTreeMap<i64, i64>,
}

impl DeltaSnapshotContext {
    pub fn from_snapshot(snapshot: &DeltaSnapshot) -> Self {
        Self {
            version: snapshot.version(),
            protocol: snapshot.protocol().clone(),
            metadata: snapshot.metadata().clone(),
            txns: snapshot.app_txns().clone(),
            domain_metadata: snapshot.domain_metadata().clone(),
            commit_timestamps: snapshot.commit_timestamps().clone(),
        }
    }

    pub fn to_snapshot(
        &self,
        log_store: &dyn LogStore,
        config: DeltaSnapshotConfig,
    ) -> crate::spec::DeltaResult<DeltaSnapshot> {
        DeltaSnapshot::from_metadata_only_parts(
            log_store,
            config,
            self.version,
            self.protocol.clone(),
            self.metadata.clone(),
            self.txns.clone(),
            self.domain_metadata.clone(),
            self.commit_timestamps.clone(),
        )
    }
}

/// Commit-time state shared by Delta write producers and the final Delta commit node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaCommitContext {
    pub base_snapshot: Option<DeltaSnapshotContext>,
    #[serde(default)]
    pub transaction: Option<Transaction>,
    #[serde(default)]
    pub identity_columns_to_track: Vec<String>,
    #[serde(default)]
    pub identity_writer_partitions: Option<usize>,
}

impl DeltaCommitContext {
    pub fn from_snapshot(snapshot: &DeltaSnapshot) -> Self {
        Self {
            base_snapshot: Some(DeltaSnapshotContext::from_snapshot(snapshot)),
            identity_columns_to_track: Vec::new(),
            identity_writer_partitions: None,
            transaction: None,
        }
    }

    pub fn base_version(&self) -> Option<i64> {
        self.base_snapshot.as_ref().map(|snapshot| snapshot.version)
    }
}

/// Coordinator-prepared file-write context for Delta data-file producers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaWriteContext {
    pub commit_context: DeltaCommitContext,
    pub final_schema: StructType,
    pub effective_column_mapping_mode: ColumnMappingMode,
    pub initial_actions: Vec<Action>,
    pub schema_actions: Vec<Action>,
    pub operation: Option<DeltaOperation>,
    pub logical_kernel_for_mapping: Option<StructType>,
    pub physical_partition_columns: Vec<String>,
}

impl DeltaWriteContext {
    pub fn final_schema_ref(&self) -> Result<SchemaRef> {
        Ok(Arc::new(
            Schema::try_from(&self.final_schema)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ))
    }

    pub fn writer_schema(&self) -> Result<SchemaRef> {
        if matches!(self.effective_column_mapping_mode, ColumnMappingMode::None) {
            self.final_schema_ref()
        } else {
            let logical_kernel = self.logical_kernel_for_mapping.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "Delta write context missing logical schema for column mapping".to_string(),
                )
            })?;
            Ok(Arc::new(get_physical_schema(
                logical_kernel,
                self.effective_column_mapping_mode,
            )?))
        }
    }
}

/// The table definition that an overwrite gives an existing table when it replaces the
/// table, in the commit that overwrites the data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaTableReplacement {
    pub configuration: HashMap<String, String>,
    pub description: Option<String>,
}

impl DeltaTableReplacement {
    /// Replaces the configuration and the description of the metadata that the write
    /// commits, and upgrades the protocol for the new configuration. The schema and the
    /// partitioning stay the ones of the write.
    ///
    /// The column mapping mode in the configuration is applied to the data that the write
    /// produces, as Delta Spark allows any change of the mode on REPLACE. As in Delta
    /// Spark, the columns get new physical names and IDs even if the mode stays the same.
    pub fn apply(&self, context: &mut DeltaWriteContext, snapshot: &DeltaSnapshot) -> Result<()> {
        let mut metadata = context
            .schema_actions
            .iter()
            .rev()
            .find_map(|action| match action {
                Action::Metadata(metadata) => Some(metadata.clone()),
                _ => None,
            })
            .unwrap_or_else(|| snapshot.metadata().clone());
        let mut configuration = self.configuration.clone();
        let mode = configuration
            .get("delta.columnMapping.mode")
            .and_then(|mode| ColumnMappingMode::try_from(mode.as_str()).ok())
            .unwrap_or_default();
        if mode == ColumnMappingMode::None
            && context.effective_column_mapping_mode == ColumnMappingMode::None
        {
            // The data is written without column mapping, as the table has none.
            for key in [
                "delta.columnMapping.mode",
                "delta.columnMapping.maxColumnId",
            ] {
                match metadata.configuration().get(key) {
                    Some(value) => configuration.insert(key.to_string(), value.clone()),
                    None => configuration.remove(key),
                };
            }
        } else {
            metadata = self.apply_column_mapping_mode(
                mode,
                metadata,
                &mut configuration,
                context,
                snapshot,
            )?;
        }
        let metadata = metadata
            .with_configuration(configuration)
            .with_description(self.description.clone());
        let desired =
            protocol_for_metadata(&metadata).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let desired =
            crate::lake_source::avoid_stable_type_widening_auto_upgrade_for_preview_tables(
                snapshot.protocol(),
                &desired,
                metadata.configuration(),
            );
        let protocol = snapshot.protocol().merge_upgrade_requirements(&desired);
        context.schema_actions = (protocol != *snapshot.protocol())
            .then_some(Action::Protocol(protocol))
            .into_iter()
            .chain([Action::Metadata(metadata)])
            .collect();
        Ok(())
    }
}

impl DeltaTableReplacement {
    /// Gives the replaced table the column mapping mode of the statement, and writes the
    /// data with it. As in REPLACE without a query, the columns get new physical names, and
    /// column IDs after the IDs that the table has used.
    fn apply_column_mapping_mode(
        &self,
        mode: ColumnMappingMode,
        metadata: Metadata,
        configuration: &mut HashMap<String, String>,
        context: &mut DeltaWriteContext,
        snapshot: &DeltaSnapshot,
    ) -> Result<Metadata> {
        let schema = crate::schema::without_column_mapping_metadata(
            &metadata
                .parse_schema()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let schema = if matches!(mode, ColumnMappingMode::None) {
            configuration.remove("delta.columnMapping.maxColumnId");
            schema
        } else {
            let used_column_id =
                if snapshot.effective_column_mapping_mode() != ColumnMappingMode::None {
                    let current = StructType::try_from(snapshot.schema())
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    snapshot
                        .metadata()
                        .configuration()
                        .get("delta.columnMapping.maxColumnId")
                        .and_then(|id| id.parse::<i64>().ok())
                        .unwrap_or_default()
                        .max(compute_max_column_id(&current))
                } else {
                    0
                };
            let (annotated, max_column_id) = annotate_new_fields_for_column_mapping(
                &StructType::try_new(Vec::<crate::spec::StructField>::new())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
                &schema,
                used_column_id + 1,
            );
            configuration.insert(
                "delta.columnMapping.mode".to_string(),
                mode.as_ref().to_string(),
            );
            configuration.insert(
                "delta.columnMapping.maxColumnId".to_string(),
                max_column_id.to_string(),
            );
            annotated
        };
        context.physical_partition_columns = metadata
            .partition_columns()
            .iter()
            .map(|name| {
                schema
                    .field(name)
                    .map(|field| field.physical_name(mode).to_string())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Partition column '{name}' not found in logical schema"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        context.final_schema =
            crate::schema::without_column_mapping_metadata(&context.final_schema);
        context.effective_column_mapping_mode = mode;
        context.logical_kernel_for_mapping =
            (!matches!(mode, ColumnMappingMode::None)).then(|| schema.clone());
        metadata
            .with_schema(&schema)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

/// A snapshot of a table at version 1 with the protocol and the metadata, for tests.
#[cfg(test)]
pub(crate) fn metadata_only_snapshot(
    protocol: Protocol,
    metadata: Metadata,
) -> Result<DeltaSnapshot> {
    let log_store = crate::table::create_logstore_with_object_store(
        Arc::new(object_store::memory::InMemory::new()),
        Url::parse("memory:///t/").map_err(|e| DataFusionError::External(Box::new(e)))?,
        crate::delta_log::StorageConfig,
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    DeltaSnapshotContext {
        version: 1,
        protocol,
        metadata,
        txns: HashMap::new(),
        domain_metadata: HashMap::new(),
        commit_timestamps: BTreeMap::new(),
    }
    .to_snapshot(log_store.as_ref(), DeltaSnapshotConfig::default())
    .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Schema handling mode for Delta Lake writes.
#[derive(Debug, Clone, Copy, PartialEq)]
enum SchemaMode {
    Merge,
    Overwrite,
}

#[expect(clippy::too_many_arguments)]
pub fn prepare_delta_write_context(
    table_url: &Url,
    table_snapshot: Option<&DeltaSnapshot>,
    options: &DeltaWriterExecOptions,
    metadata_configuration: &HashMap<String, String>,
    partition_columns: &[String],
    sink_mode: &PhysicalSinkMode,
    table_exists: bool,
    input_schema: &SchemaRef,
    create_table_schema: Option<&SchemaRef>,
    create_actions: Option<&(Protocol, Metadata)>,
    operation_override: Option<DeltaOperation>,
) -> Result<DeltaWriteContext> {
    let input_schema =
        strip_column_mapping_metadata(&normalize_delta_schema(&apply_target_nullability(
            &schema_without_writer_metric_columns(
                input_schema,
                options.merge_change_data_preimage.as_deref(),
            ),
            &options.target_nullability,
        )));
    let mut initial_actions: Vec<Action> = Vec::new();
    let planned_operation = operation_for_sink_mode(table_url, partition_columns, sink_mode);

    let (final_schema, schema_actions) = if table_exists {
        let snapshot = table_snapshot.ok_or_else(|| {
            DataFusionError::Internal(
                "Delta write planning requires a pinned table snapshot for existing tables"
                    .to_string(),
            )
        })?;
        let save_mode = match &planned_operation {
            Some(DeltaOperation::Write { mode, .. }) => *mode,
            Some(DeltaOperation::Create { mode, .. }) => *mode,
            _ => SaveMode::Append,
        };
        let schema_mode = get_schema_mode(options, save_mode, sink_mode)?;
        let (schema, mut metadata) =
            handle_schema_evolution(snapshot, &input_schema, schema_mode, partition_columns)?;
        if matches!(sink_mode, PhysicalSinkMode::Overwrite)
            && let Some(comment) = metadata_configuration.get("comment")
            && snapshot.metadata().description() != Some(comment.as_str())
        {
            metadata = Some(
                metadata
                    .unwrap_or_else(|| snapshot.metadata().clone())
                    .with_description(Some(comment.clone())),
            );
        }
        (schema, metadata.into_iter().map(Action::Metadata).collect())
    } else {
        (input_schema.clone(), Vec::new())
    };
    let final_schema = normalize_delta_schema(&final_schema);

    let effective_mode = if let Some(snapshot) = table_snapshot {
        snapshot.effective_column_mapping_mode()
    } else {
        metadata_configuration
            .get("delta.columnMapping.mode")
            .and_then(|v| ColumnMappingMode::try_from(v.as_str()).ok())
            .unwrap_or_default()
    };

    let mut operation = planned_operation;
    let mut annotated_schema_opt: Option<StructType> = None;
    if !table_exists {
        let (protocol, metadata) = if let Some((protocol, metadata)) = create_actions {
            // The data is written with the column mapping of the table that is created.
            if !matches!(effective_mode, ColumnMappingMode::None) {
                annotated_schema_opt = Some(
                    metadata
                        .parse_schema()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                );
            }
            (protocol.clone(), metadata.clone())
        } else {
            let mut kernel_schema = StructType::try_from(final_schema.as_ref())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if !options.generation_expressions.is_empty() {
                kernel_schema =
                    inject_generation_expressions(kernel_schema, &options.generation_expressions);
            }
            if !options.default_expressions.is_empty() {
                kernel_schema =
                    inject_default_expressions(kernel_schema, &options.default_expressions);
            }
            if !options.identity_columns.is_empty() {
                kernel_schema = inject_identity_columns(kernel_schema, &options.identity_columns);
            }

            let mut configuration = metadata_configuration.clone();
            let metadata_schema = if !matches!(effective_mode, ColumnMappingMode::None) {
                let annotated_schema = match create_table_schema {
                    // The table is created before the data is written, with the column
                    // mapping of this schema.
                    Some(create_table_schema) => {
                        let assigned = column_mapping_metadata_only(
                            &StructType::try_from(normalize_delta_schema(create_table_schema))
                                .map_err(|e| DataFusionError::External(Box::new(e)))?,
                        );
                        annotate_new_fields_for_column_mapping(
                            &assigned,
                            &kernel_schema,
                            compute_max_column_id(&assigned) + 1,
                        )
                        .0
                    }
                    None => annotate_for_column_mapping(&kernel_schema),
                };
                configuration.insert(
                    "delta.columnMapping.mode".to_string(),
                    effective_mode.as_ref().to_string(),
                );
                configuration.insert(
                    "delta.columnMapping.maxColumnId".to_string(),
                    compute_max_column_id(&annotated_schema).to_string(),
                );
                annotated_schema_opt = Some(annotated_schema.clone());
                annotated_schema
            } else {
                kernel_schema
            };

            let metadata = metadata_for_create_with_struct_type(
                metadata_schema,
                partition_columns.to_vec(),
                chrono::Utc::now().timestamp_millis(),
                configuration,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let protocol = protocol_for_metadata(&metadata)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            (protocol, metadata)
        };

        initial_actions.push(Action::Protocol(protocol.clone()));
        initial_actions.push(Action::Metadata(metadata.clone()));

        operation = Some(DeltaOperation::Create {
            mode: SaveMode::ErrorIfExists,
            location: table_url.to_string(),
            protocol: Box::new(protocol),
            metadata: Box::new(metadata),
        });
    }

    let (physical_partition_columns, logical_kernel_for_mapping) =
        if !matches!(effective_mode, ColumnMappingMode::None) {
            let logical_kernel = if let Some(metadata_schema) = schema_actions
                .iter()
                .find_map(|action| match action {
                    Action::Metadata(metadata) => Some(
                        metadata
                            .parse_schema()
                            .map_err(|e| DataFusionError::External(Box::new(e))),
                    ),
                    _ => None,
                })
                .transpose()?
            {
                metadata_schema
            } else if let Some(snapshot) = table_snapshot {
                StructType::try_from(snapshot.schema())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                annotated_schema_opt.ok_or_else(|| {
                    DataFusionError::Plan(
                        "Annotated schema should be present for new table with column mapping"
                            .to_string(),
                    )
                })?
            };

            let resolved_partitions = partition_columns
                .iter()
                .map(|logical_name| {
                    let field = logical_kernel.field(logical_name).ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Partition column '{}' not found in logical schema",
                            logical_name
                        ))
                    })?;
                    Ok(field.physical_name(effective_mode).to_string())
                })
                .collect::<Result<Vec<_>>>()?;
            (resolved_partitions, Some(logical_kernel))
        } else {
            (partition_columns.to_vec(), None)
        };

    let final_schema = StructType::try_from(final_schema.as_ref())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let commit_context = table_snapshot
        .map(DeltaCommitContext::from_snapshot)
        .unwrap_or_default();

    Ok(DeltaWriteContext {
        commit_context,
        final_schema,
        effective_column_mapping_mode: effective_mode,
        initial_actions,
        schema_actions,
        operation: operation_override.or(operation),
        logical_kernel_for_mapping,
        physical_partition_columns,
    })
}

fn is_writer_metric_column(name: &str) -> bool {
    name == OPERATION_COLUMN || name == MERGE_SOURCE_METRIC_COLUMN
}

fn schema_without_writer_metric_columns(schema: &SchemaRef, preimage: Option<&str>) -> SchemaRef {
    if !schema.fields().iter().any(|field| {
        is_writer_metric_column(field.name()) || preimage == Some(field.name().as_str())
    }) {
        return Arc::clone(schema);
    }

    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|field| {
                !is_writer_metric_column(field.name()) && preimage != Some(field.name().as_str())
            })
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    ))
}

fn apply_target_nullability(
    schema: &SchemaRef,
    target_nullability: &HashMap<String, bool>,
) -> SchemaRef {
    if target_nullability.is_empty()
        && !schema.fields().iter().any(|field| {
            field
                .metadata()
                .contains_key(SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY)
        })
    {
        return Arc::clone(schema);
    }

    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                let nullable = target_nullability
                    .get(field.name())
                    .copied()
                    .unwrap_or_else(|| field.is_nullable());
                let mut metadata = field.metadata().clone();
                metadata.remove(SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY);
                Field::new(field.name().clone(), field.data_type().clone(), nullable)
                    .with_metadata(metadata)
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn operation_for_sink_mode(
    _table_url: &Url,
    partition_columns: &[String],
    sink_mode: &PhysicalSinkMode,
) -> Option<DeltaOperation> {
    let partition_by = (!partition_columns.is_empty()).then(|| partition_columns.to_vec());
    match sink_mode {
        PhysicalSinkMode::Append => Some(DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by,
            predicate: None,
        }),
        PhysicalSinkMode::Overwrite => Some(DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            partition_by,
            predicate: None,
        }),
        PhysicalSinkMode::OverwriteIf { source, .. } => Some(DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            partition_by,
            predicate: source.clone(),
        }),
        PhysicalSinkMode::ErrorIfExists | PhysicalSinkMode::IgnoreIfExists => None,
        PhysicalSinkMode::OverwritePartitions => {
            Some(DeltaOperation::DynamicPartitionOverwrite { partition_by })
        }
    }
}

fn get_schema_mode(
    options: &DeltaWriterExecOptions,
    save_mode: SaveMode,
    sink_mode: &PhysicalSinkMode,
) -> Result<Option<SchemaMode>> {
    // Conditional overwrites retain the table schema unless schema merging is enabled.
    let overwrite_schema =
        options.overwrite_schema && !matches!(sink_mode, PhysicalSinkMode::OverwriteIf { .. });
    match (options.merge_schema, overwrite_schema) {
        (true, true) => Err(DataFusionError::Plan(
            "Cannot specify both mergeSchema and overwriteSchema options".to_string(),
        )),
        (false, true) => {
            if save_mode != SaveMode::Overwrite {
                Err(DataFusionError::Plan(
                    "overwriteSchema option can only be used with overwrite save mode".to_string(),
                ))
            } else {
                Ok(Some(SchemaMode::Overwrite))
            }
        }
        (true, false) => Ok(Some(SchemaMode::Merge)),
        (false, false) => Ok(None),
    }
}

fn handle_schema_evolution(
    snapshot: &DeltaSnapshot,
    input_schema: &SchemaRef,
    schema_mode: Option<SchemaMode>,
    partition_columns: &[String],
) -> Result<(SchemaRef, Option<Metadata>)> {
    let table_arrow_schema = Arc::new(
        snapshot
            .metadata()
            .parse_schema_arrow()
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    match schema_mode {
        Some(SchemaMode::Merge) => {
            let merged_schema = merge_schemas(&table_arrow_schema, input_schema)?;
            if merged_schema.fields() != table_arrow_schema.fields() {
                let mut candidate_kernel = StructType::try_from(merged_schema.as_ref())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let current_metadata = snapshot.metadata();
                let current_kernel = StructType::try_from(snapshot.schema())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let mode = snapshot.effective_column_mapping_mode();
                let type_changes = collect_type_changes(&current_kernel, &candidate_kernel)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                if !type_changes.is_empty() {
                    if !snapshot.table_properties().enable_type_widening() {
                        return Err(DataFusionError::Plan(
                            "Delta type widening schema evolution requires table property \
                             delta.enableTypeWidening=true"
                                .to_string(),
                        ));
                    }
                    if !protocol_can_write_type_widening(snapshot.protocol()) {
                        return Err(DataFusionError::Plan(
                            "Delta type widening schema evolution requires the typeWidening \
                             reader and writer table features"
                                .to_string(),
                        ));
                    }
                    for (field_path, change) in &type_changes {
                        if !is_supported_type_change_for_schema_evolution(
                            snapshot.protocol(),
                            &change.from_type,
                            &change.to_type,
                        ) {
                            return Err(DataFusionError::Plan(format!(
                                "Delta type widening change at {} is not supported for \
                                 schema evolution: {} -> {}",
                                format_type_change_path(field_path, &change.field_path),
                                change.from_type,
                                change.to_type
                            )));
                        }
                    }
                }
                if !type_changes.is_empty()
                    || schema_contains_type_widening_metadata(&current_kernel)
                {
                    candidate_kernel =
                        add_type_widening_metadata(&current_kernel, &candidate_kernel)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                }
                let (_final_kernel, updated_metadata) =
                    evolve_schema(&current_kernel, &candidate_kernel, current_metadata, mode)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok((merged_schema, Some(updated_metadata)))
            } else {
                Ok((table_arrow_schema, None))
            }
        }
        Some(SchemaMode::Overwrite) => {
            let candidate_kernel = StructType::try_from(input_schema.as_ref())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let current_metadata = snapshot.metadata();
            let current_kernel = StructType::try_from(snapshot.schema())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mode = snapshot.effective_column_mapping_mode();
            let (_final_kernel, updated_metadata) =
                evolve_schema(&current_kernel, &candidate_kernel, current_metadata, mode)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let updated_metadata =
                updated_metadata.with_partition_columns(partition_columns.to_vec());
            Ok((input_schema.clone(), Some(updated_metadata)))
        }
        None => {
            validate_schema_compatibility(&table_arrow_schema, input_schema)?;
            Ok((table_arrow_schema, None))
        }
    }
}

fn merge_schemas(table_schema: &Schema, input_schema: &Schema) -> Result<SchemaRef> {
    let mut field_map: HashMap<String, Field> = HashMap::new();
    let mut field_order: Vec<String> = Vec::new();

    for field in table_schema.fields() {
        let field_name = field.name().clone();
        field_map.insert(field_name.clone(), field.as_ref().clone());
        field_order.push(field_name);
    }

    for input_field in input_schema.fields() {
        let field_name = input_field.name().clone();
        if let Some(existing_field) = field_map.get(&field_name) {
            let promoted_field =
                DeltaTypeConverter::promote_field_types(existing_field, input_field)?;
            field_map.insert(field_name, promoted_field);
        } else {
            field_map.insert(field_name.clone(), input_field.as_ref().clone());
            field_order.push(field_name);
        }
    }

    let merged_fields = field_order
        .into_iter()
        .map(|name| {
            field_map.remove(&name).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Field '{name}' missing during schema merge construction",
                ))
            })
        })
        .collect::<Result<Vec<Field>>>()?;

    Ok(Arc::new(Schema::new(merged_fields)))
}

fn validate_schema_compatibility(table_schema: &Schema, input_schema: &Schema) -> Result<()> {
    for input_field in input_schema.fields() {
        match table_schema.field_with_name(input_field.name()) {
            Ok(table_field) => {
                if table_field.data_type() != input_field.data_type()
                    && DeltaTypeConverter::validate_cast_safety(
                        input_field.data_type(),
                        table_field.data_type(),
                        input_field.name(),
                    )
                    .is_err()
                {
                    return Err(DataFusionError::Plan(format!(
                        "Schema mismatch for field '{}': table has type {:?}, input has type {:?}. Use mergeSchema=true to allow schema evolution.",
                        input_field.name(),
                        table_field.data_type(),
                        input_field.data_type()
                    )));
                }
            }
            Err(_) => {
                return Err(DataFusionError::Plan(format!(
                    "Field '{}' not found in table schema. Use mergeSchema=true to allow schema evolution.",
                    input_field.name()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::spec::{DataType, MetadataValue, StructField, TableFeature};

    fn mapped_field(name: &str, physical_name: &str, id: i64, data_type: DataType) -> StructField {
        StructField::new(name, data_type, true).with_metadata([
            ("delta.columnMapping.id", MetadataValue::Number(id)),
            (
                "delta.columnMapping.physicalName",
                MetadataValue::String(physical_name.to_string()),
            ),
        ])
    }

    #[test]
    fn table_replacement_replaces_configuration_and_description() -> Result<()> {
        let schema = StructType::try_new(vec![StructField::new("id", DataType::LONG, true)])
            .expect("schema");
        let metadata = metadata_for_create_with_struct_type(
            schema,
            vec![],
            0,
            HashMap::from([
                ("team".to_string(), "a".to_string()),
                ("comment".to_string(), "old".to_string()),
            ]),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let snapshot = metadata_only_snapshot(
            Protocol::new(3, 7, Some(vec![]), Some(vec![])),
            metadata.clone(),
        )?;
        let mut context = DeltaWriteContext {
            commit_context: DeltaCommitContext::from_snapshot(&snapshot),
            final_schema: StructType::try_from(snapshot.schema()).expect("schema"),
            effective_column_mapping_mode: ColumnMappingMode::None,
            initial_actions: Vec::new(),
            schema_actions: Vec::new(),
            operation: None,
            logical_kernel_for_mapping: None,
            physical_partition_columns: Vec::new(),
        };
        let configuration = HashMap::from([
            ("delta.enableRowTracking".to_string(), "true".to_string()),
            ("owner-team".to_string(), "b".to_string()),
        ]);
        DeltaTableReplacement {
            configuration: configuration.clone(),
            description: None,
        }
        .apply(&mut context, &snapshot)?;

        // The protocol is upgraded for row tracking in the same commit as the metadata.
        let [
            Action::Protocol(new_protocol),
            Action::Metadata(new_metadata),
        ] = context.schema_actions.as_slice()
        else {
            panic!("{:?}", context.schema_actions);
        };
        assert!(new_protocol.has_writer_feature(&TableFeature::RowTracking));
        assert!(new_protocol.has_writer_feature(&TableFeature::DomainMetadata));
        assert_eq!(new_metadata.configuration(), &configuration);
        assert_eq!(new_metadata.description(), None);
        assert_eq!(new_metadata.id(), metadata.id());
        assert_eq!(new_metadata.schema_string(), metadata.schema_string());
        Ok(())
    }

    /// REPLACE with a query gives the table the column mapping mode of the statement, as
    /// Delta Spark does, and the data is written with the new physical names.
    #[test]
    fn table_replacement_applies_the_column_mapping_mode_of_the_statement() -> Result<()> {
        let replace = |schema: StructType,
                       configuration: HashMap<String, String>,
                       mode: Option<&'static str>| {
            let metadata = metadata_for_create_with_struct_type(
                schema.clone(),
                vec!["p".to_string()],
                0,
                configuration,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let snapshot = metadata_only_snapshot(
                protocol_for_metadata(&metadata)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
                metadata,
            )?;
            let mut context = DeltaWriteContext {
                commit_context: DeltaCommitContext::from_snapshot(&snapshot),
                final_schema: schema.clone(),
                effective_column_mapping_mode: snapshot.effective_column_mapping_mode(),
                initial_actions: Vec::new(),
                schema_actions: Vec::new(),
                operation: None,
                logical_kernel_for_mapping: (snapshot.effective_column_mapping_mode()
                    != ColumnMappingMode::None)
                    .then_some(schema),
                physical_partition_columns: vec!["p".to_string()],
            };
            DeltaTableReplacement {
                configuration: HashMap::from_iter(
                    mode.map(|mode| ("delta.columnMapping.mode".to_string(), mode.to_string())),
                ),
                description: None,
            }
            .apply(&mut context, &snapshot)?;
            let metadata = context
                .schema_actions
                .iter()
                .find_map(|action| match action {
                    Action::Metadata(metadata) => Some(metadata.clone()),
                    _ => None,
                })
                .expect("metadata");
            Ok::<_, DataFusionError>((context, metadata))
        };

        let unmapped = StructType::try_new(vec![
            StructField::new("id", DataType::LONG, true),
            StructField::new("p", DataType::STRING, true),
        ])
        .expect("schema");
        let (context, metadata) = replace(unmapped.clone(), HashMap::new(), Some("name"))?;
        let protocol = context
            .schema_actions
            .iter()
            .find_map(|action| match action {
                Action::Protocol(protocol) => Some(protocol),
                _ => None,
            });
        // The protocol is upgraded for column mapping.
        assert!(protocol.is_some_and(|protocol| protocol.min_writer_version() >= 5));
        assert_eq!(
            context.effective_column_mapping_mode,
            ColumnMappingMode::Name
        );
        assert_eq!(
            metadata.configuration().get("delta.columnMapping.mode"),
            Some(&"name".to_string())
        );
        assert_eq!(
            metadata
                .configuration()
                .get("delta.columnMapping.maxColumnId"),
            Some(&"2".to_string())
        );
        let schema = metadata.parse_schema().expect("schema");
        assert_eq!(context.logical_kernel_for_mapping.as_ref(), Some(&schema));
        let physical_names = schema
            .fields()
            .map(|field| field.physical_name(ColumnMappingMode::Name).to_string())
            .collect::<Vec<_>>();
        assert!(
            physical_names.iter().all(|name| name.starts_with("col-")),
            "{physical_names:?}"
        );
        assert_eq!(context.physical_partition_columns, physical_names[1..]);
        assert_eq!(
            context
                .writer_schema()?
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            physical_names
        );

        // A table with column mapping that is replaced without a mode has no column mapping,
        // and a new mode gives the columns new physical names and IDs.
        let mapped = StructType::try_new(vec![
            mapped_field("id", "col-id", 1, DataType::LONG),
            mapped_field("p", "col-p", 5, DataType::STRING),
        ])
        .expect("schema");
        let mapped_configuration = HashMap::from([
            ("delta.columnMapping.mode".to_string(), "name".to_string()),
            (
                "delta.columnMapping.maxColumnId".to_string(),
                "7".to_string(),
            ),
        ]);
        let (context, metadata) = replace(mapped.clone(), mapped_configuration.clone(), None)?;
        assert_eq!(
            context.effective_column_mapping_mode,
            ColumnMappingMode::None
        );
        assert!(
            !metadata
                .configuration()
                .keys()
                .any(|key| key.starts_with("delta.columnMapping.")),
            "{:?}",
            metadata.configuration()
        );
        assert_eq!(metadata.parse_schema().expect("schema"), unmapped);
        assert_eq!(context.logical_kernel_for_mapping, None);
        assert_eq!(context.physical_partition_columns, vec!["p".to_string()]);
        assert_eq!(
            context
                .writer_schema()?
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            vec!["id".to_string(), "p".to_string()]
        );

        // The same mode, as a new mode, gives the columns new physical names and IDs.
        for (mode, expected) in [
            ("id", ColumnMappingMode::Id),
            ("name", ColumnMappingMode::Name),
        ] {
            let (context, metadata) =
                replace(mapped.clone(), mapped_configuration.clone(), Some(mode))?;
            assert_eq!(context.effective_column_mapping_mode, expected);
            assert_eq!(
                metadata
                    .configuration()
                    .get("delta.columnMapping.maxColumnId"),
                Some(&"9".to_string())
            );
            let schema = metadata.parse_schema().expect("schema");
            let physical_names = schema
                .fields()
                .map(|field| field.physical_name(expected).to_string())
                .collect::<Vec<_>>();
            for name in &physical_names {
                assert!(name.starts_with("col-"), "{mode}: {physical_names:?}");
                assert!(
                    !["col-id", "col-p"].contains(&name.as_str()),
                    "{mode}: {physical_names:?}"
                );
            }
            assert_eq!(context.physical_partition_columns, physical_names[1..]);
            assert_eq!(context.logical_kernel_for_mapping.as_ref(), Some(&schema));
        }
        Ok(())
    }

    #[test]
    fn writer_schema_preserves_nested_parquet_field_ids() -> Result<()> {
        let nested = StructType::try_new(vec![mapped_field("rate", "col-rate", 2, DataType::LONG)])
            .expect("nested schema");
        let logical = StructType::try_new(vec![mapped_field(
            "details",
            "col-details",
            1,
            DataType::Struct(Box::new(nested)),
        )])
        .expect("logical schema");
        let context = DeltaWriteContext {
            commit_context: DeltaCommitContext::default(),
            final_schema: logical.clone(),
            effective_column_mapping_mode: ColumnMappingMode::Name,
            initial_actions: Vec::new(),
            schema_actions: Vec::new(),
            operation: None,
            logical_kernel_for_mapping: Some(logical),
            physical_partition_columns: Vec::new(),
        };

        let writer_schema = context.writer_schema()?;
        let details = writer_schema.field_with_name("col-details")?;
        let datafusion::arrow::datatypes::DataType::Struct(children) = details.data_type() else {
            panic!("details must be a struct");
        };
        let rate = children.first().expect("nested rate field");

        assert_eq!(rate.name(), "col-rate");
        assert_eq!(
            rate.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("2")
        );
        Ok(())
    }
}
