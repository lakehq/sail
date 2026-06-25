use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::column_features::SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY;
use sail_common_datafusion::datasource::{
    PhysicalSinkMode, MERGE_SOURCE_METRIC_COLUMN, OPERATION_COLUMN,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::conversion::DeltaTypeConverter;
use crate::delta_log::LogStore;
use crate::physical_plan::writer_options::DeltaWriterExecOptions;
use crate::schema::{
    add_type_widening_metadata, annotate_for_column_mapping, collect_type_changes,
    compute_max_column_id, evolve_schema, format_type_change_path, get_physical_schema,
    inject_default_expressions, inject_generation_expressions, inject_identity_columns,
    is_supported_type_change_for_schema_evolution, metadata_for_create_with_struct_type,
    normalize_delta_schema, protocol_can_write_type_widening, protocol_for_create,
    schema_contains_type_widening_metadata, schema_has_column_defaults,
    schema_has_generated_columns, schema_has_identity_columns,
};
use crate::snapshot::DeltaSnapshotConfig;
use crate::spec::{
    contains_timestampntz_arrow, contains_variant_arrow, Action, ColumnMappingMode, DeltaOperation,
    DomainMetadata, Metadata, Protocol, SaveMode, StructType, TableProperties, Transaction,
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
}

impl DeltaCommitContext {
    pub fn from_snapshot(snapshot: &DeltaSnapshot) -> Self {
        Self {
            base_snapshot: Some(DeltaSnapshotContext::from_snapshot(snapshot)),
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
            )))
        }
    }
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
    operation_override: Option<DeltaOperation>,
) -> Result<DeltaWriteContext> {
    let input_schema = normalize_delta_schema(&apply_target_nullability(
        &schema_without_writer_metric_columns(input_schema),
        &options.target_nullability,
    ));
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
        let schema_mode = get_schema_mode(options, save_mode)?;
        handle_schema_evolution(snapshot, &input_schema, schema_mode, partition_columns)?
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
        let has_timestamp_ntz = contains_timestampntz_arrow(final_schema.as_ref());
        let has_variant = contains_variant_arrow(final_schema.as_ref());
        let mut kernel_schema = StructType::try_from(final_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if !options.generation_expressions.is_empty() {
            kernel_schema =
                inject_generation_expressions(kernel_schema, &options.generation_expressions);
        }
        if !options.default_expressions.is_empty() {
            kernel_schema = inject_default_expressions(kernel_schema, &options.default_expressions);
        }
        if !options.identity_columns.is_empty() {
            kernel_schema = inject_identity_columns(kernel_schema, &options.identity_columns);
        }

        let mut configuration = metadata_configuration.clone();
        let metadata_schema = if !matches!(effective_mode, ColumnMappingMode::None) {
            let annotated_schema = annotate_for_column_mapping(&kernel_schema);
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

        let protocol = protocol_for_create(
            !matches!(effective_mode, ColumnMappingMode::None),
            has_timestamp_ntz,
            TableProperties::from(configuration.iter()).enable_in_commit_timestamps(),
            schema_has_generated_columns(&metadata_schema),
            schema_has_column_defaults(&metadata_schema),
            schema_has_identity_columns(&metadata_schema),
            has_variant,
            &configuration,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let metadata = metadata_for_create_with_struct_type(
            metadata_schema,
            partition_columns.to_vec(),
            chrono::Utc::now().timestamp_millis(),
            configuration,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

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

fn schema_without_writer_metric_columns(schema: &SchemaRef) -> SchemaRef {
    if !schema
        .fields()
        .iter()
        .any(|field| is_writer_metric_column(field.name()))
    {
        return Arc::clone(schema);
    }

    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|field| !is_writer_metric_column(field.name()))
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
    table_url: &Url,
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
        PhysicalSinkMode::OverwritePartitions => Some(DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            partition_by,
            predicate: Some(format!("__unsupported_overwrite_partitions_at={table_url}")),
        }),
    }
}

fn get_schema_mode(
    options: &DeltaWriterExecOptions,
    save_mode: SaveMode,
) -> Result<Option<SchemaMode>> {
    match (options.merge_schema, options.overwrite_schema) {
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
) -> Result<(SchemaRef, Vec<Action>)> {
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
                Ok((merged_schema, vec![Action::Metadata(updated_metadata)]))
            } else {
                Ok((table_arrow_schema, Vec::new()))
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
            Ok((
                input_schema.clone(),
                vec![Action::Metadata(updated_metadata)],
            ))
        }
        None => {
            validate_schema_compatibility(&table_arrow_schema, input_schema)?;
            Ok((table_arrow_schema, Vec::new()))
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
