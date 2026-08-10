use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::variant::VariantShreddingConfig;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::datasource::type_converter::{arrow_schema_to_iceberg, iceberg_schema_to_arrow};
use crate::physical_plan::write_location;
use crate::physical_plan::writer_options::IcebergWriterExecOptions;
use crate::schema_evolution::{SchemaEvolver, SchemaMode};
use crate::spec::partition::{PartitionSpec, UnboundPartitionField, UnboundPartitionSpec};
use crate::spec::{FormatVersion, Schema as IcebergSchema, TableMetadata, TableRequirement};
use crate::utils::partition_transform::{
    catalog_partition_field_from_iceberg, format_partition_expr,
    iceberg_transform_from_partition_field, partition_field_name,
};

/// Metadata subset pinned for an existing Iceberg table write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergBaseWriteContext {
    pub format_version: FormatVersion,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: i32,
    pub properties: HashMap<String, String>,
    pub last_column_id: i32,
    pub current_schema_id: i32,
    pub last_partition_id: i32,
    pub current_snapshot_id: Option<i64>,
}

impl IcebergBaseWriteContext {
    fn from_metadata(metadata: &TableMetadata) -> Self {
        Self {
            format_version: metadata.format_version,
            partition_specs: metadata.partition_specs.clone(),
            default_spec_id: metadata.default_spec_id,
            properties: metadata.properties.clone(),
            last_column_id: metadata.last_column_id,
            current_schema_id: metadata.current_schema_id,
            last_partition_id: metadata.last_partition_id,
            current_snapshot_id: metadata.current_snapshot_id,
        }
    }

    pub fn default_partition_spec(&self) -> Option<&PartitionSpec> {
        self.partition_specs
            .iter()
            .find(|spec| spec.spec_id() == self.default_spec_id)
    }
}

/// Coordinator-prepared state consumed by Iceberg data-file writers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergWriteContext {
    pub base_table: Option<IcebergBaseWriteContext>,
    pub writer_schema: IcebergSchema,
    pub writer_partition_spec: Option<PartitionSpec>,
    pub data_location: String,
    pub commit_writer_schema: bool,
    pub commit_writer_partition_spec: bool,
    pub requirements: Vec<TableRequirement>,
    pub variant_shredding: VariantShreddingConfig,
}

impl IcebergWriteContext {
    pub fn validate_table_state(&self, table_exists: bool) -> Result<()> {
        if table_exists != self.base_table.is_some() {
            return Err(DataFusionError::Plan(format!(
                "Iceberg write context table state mismatch: table_exists={table_exists}, \
                 base_present={}",
                self.base_table.is_some()
            )));
        }
        if self.commit_writer_partition_spec && self.writer_partition_spec.is_none() {
            return Err(DataFusionError::Plan(
                "Iceberg write context cannot commit a missing writer partition spec".to_string(),
            ));
        }
        self.data_location()?;
        Ok(())
    }

    pub fn writer_arrow_schema(&self) -> Result<SchemaRef> {
        Ok(Arc::new(iceberg_schema_to_arrow(&self.writer_schema)?))
    }

    pub fn writer_partition_spec_id(&self) -> i32 {
        self.writer_partition_spec
            .as_ref()
            .map(PartitionSpec::spec_id)
            .unwrap_or(0)
    }

    pub fn unbound_writer_partition_spec(&self) -> UnboundPartitionSpec {
        let fields = self
            .writer_partition_spec
            .as_ref()
            .map(|spec| {
                spec.fields()
                    .iter()
                    .map(|field| UnboundPartitionField {
                        source_id: field.source_id,
                        name: field.name.clone(),
                        transform: field.transform,
                    })
                    .collect()
            })
            .unwrap_or_default();
        UnboundPartitionSpec { fields }
    }

    pub fn data_location(&self) -> Result<Url> {
        Url::parse(&self.data_location).map_err(|error| {
            DataFusionError::Plan(format!(
                "Invalid data location in Iceberg write context: {error}"
            ))
        })
    }
}

pub(crate) fn input_schema_with_logical_metadata(
    physical_schema: SchemaRef,
    logical_schema: Option<&SchemaRef>,
) -> SchemaRef {
    let Some(logical_schema) = logical_schema else {
        return physical_schema;
    };

    let fields = physical_schema
        .fields()
        .iter()
        .map(|physical_field| {
            let Ok(logical_field) = logical_schema.field_with_name(physical_field.name()) else {
                return Arc::clone(physical_field);
            };
            if logical_field.metadata().is_empty() {
                return Arc::clone(physical_field);
            }

            let mut metadata = physical_field.metadata().clone();
            metadata.extend(logical_field.metadata().clone());
            Arc::new(physical_field.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Vec<_>>();

    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        physical_schema.metadata().clone(),
    ))
}

pub fn prepare_iceberg_write_context(
    table_url: &Url,
    base_metadata: Option<&TableMetadata>,
    options: &IcebergWriterExecOptions,
    partition_columns: &[CatalogPartitionField],
    sink_mode: &PhysicalSinkMode,
    input_schema: &ArrowSchema,
) -> Result<IcebergWriteContext> {
    let schema_mode = get_schema_mode(options, sink_mode)?;

    let (
        writer_schema,
        writer_partition_spec,
        data_location,
        commit_writer_schema,
        commit_writer_partition_spec,
        requirements,
        variant_shredding,
    ) = if let Some(table_metadata) = base_metadata {
        let data_location = write_location::resolve_data_location_from_options_and_properties(
            options.write_data_path.as_deref(),
            options.write_folder_storage_path.as_deref(),
            &table_metadata.properties,
            table_url,
        )?;
        let variant_shredding = options.variant_shredding_config(&table_metadata.properties)?;
        let schema_outcome = SchemaEvolver::evolve(table_metadata, input_schema, schema_mode)?;
        let mut partition_spec = table_metadata.default_partition_spec().cloned();
        if matches!(schema_mode, Some(SchemaMode::Overwrite)) {
            if !partition_columns.is_empty() {
                let current_schema = &schema_outcome.iceberg_schema;
                let mut builder = PartitionSpec::builder();
                if let Some(existing) = &partition_spec {
                    builder = builder.with_spec_id(existing.spec_id());
                }
                for field in partition_columns {
                    let field_id =
                        current_schema
                            .field_id_by_name(&field.column)
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "Partition column mismatch: column '{}' not found in schema",
                                    format_partition_expr(field)
                                ))
                            })?;
                    builder = builder.add_field(
                        field_id,
                        partition_field_name(field),
                        iceberg_transform_from_partition_field(field),
                    );
                }
                partition_spec = Some(builder.build());
            }
        } else {
            let current_schema = table_metadata.current_schema().ok_or_else(|| {
                DataFusionError::Plan(
                    "Partition column mismatch: missing current schema".to_string(),
                )
            })?;
            let table_partition_columns =
                extract_partition_columns(&partition_spec, current_schema)?;
            if !partition_columns.is_empty() && partition_columns != table_partition_columns {
                return Err(DataFusionError::Plan(format!(
                    "Partition column mismatch: table uses {:?}, requested {:?}",
                    crate::utils::partition_transform::format_partition_exprs(
                        &table_partition_columns
                    ),
                    crate::utils::partition_transform::format_partition_exprs(partition_columns)
                )));
            }
        }

        let commit_writer_schema = schema_outcome.changed;
        let commit_writer_partition_spec =
            matches!(schema_mode, Some(SchemaMode::Overwrite)) && partition_spec.is_some();
        let requirements = vec![
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: table_metadata.last_column_id,
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: table_metadata.current_schema_id,
            },
        ];
        (
            schema_outcome.iceberg_schema,
            partition_spec,
            data_location,
            commit_writer_schema,
            commit_writer_partition_spec,
            requirements,
            variant_shredding,
        )
    } else {
        let (_, metadata_properties) =
            crate::properties::metadata_properties_from_table_properties(
                &options.table_properties,
            )?;
        let variant_shredding = options.variant_shredding_config(&metadata_properties)?;
        let mut writer_schema = arrow_schema_to_iceberg(input_schema)?;
        writer_schema = SchemaEvolver::assign_schema_field_ids(&writer_schema)?;
        if writer_schema.fields().iter().any(|field| field.id == 0) {
            return Err(DataFusionError::Plan(
                "Invalid Iceberg schema: field id 0 detected after assignment".to_string(),
            ));
        }
        for field in partition_columns {
            if writer_schema.field_id_by_name(&field.column).is_none() {
                return Err(DataFusionError::Plan(format!(
                    "Partition column mismatch: column '{}' not found in schema",
                    format_partition_expr(field)
                )));
            }
        }
        let mut builder = PartitionSpec::builder();
        for field in partition_columns {
            if let Some(field_id) = writer_schema.field_id_by_name(&field.column) {
                builder = builder.add_field(
                    field_id,
                    partition_field_name(field),
                    iceberg_transform_from_partition_field(field),
                );
            }
        }
        let partition_spec = Some(builder.build());
        let data_location = write_location::resolve_data_location_from_options_and_properties(
            options.write_data_path.as_deref(),
            options.write_folder_storage_path.as_deref(),
            &metadata_properties,
            table_url,
        )?;
        (
            writer_schema,
            partition_spec,
            data_location,
            true,
            true,
            Vec::new(),
            variant_shredding,
        )
    };

    Ok(IcebergWriteContext {
        base_table: base_metadata.map(IcebergBaseWriteContext::from_metadata),
        writer_schema,
        writer_partition_spec,
        data_location: data_location.to_string(),
        commit_writer_schema,
        commit_writer_partition_spec,
        requirements,
        variant_shredding,
    })
}

fn extract_partition_columns(
    partition_spec: &Option<PartitionSpec>,
    iceberg_schema: &IcebergSchema,
) -> Result<Vec<CatalogPartitionField>> {
    let Some(partition_spec) = partition_spec else {
        return Ok(Vec::new());
    };

    partition_spec
        .fields()
        .iter()
        .map(|partition_field| {
            let field = iceberg_schema
                .field_by_id(partition_field.source_id)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Partition column mismatch: field id {} missing in schema",
                        partition_field.source_id
                    ))
                })?;
            catalog_partition_field_from_iceberg(field.name.clone(), partition_field.transform)
                .map_err(DataFusionError::Plan)
        })
        .collect()
}

fn get_schema_mode(
    options: &IcebergWriterExecOptions,
    sink_mode: &PhysicalSinkMode,
) -> Result<Option<SchemaMode>> {
    match (options.merge_schema, options.overwrite_schema) {
        (true, true) => Err(DataFusionError::Plan(
            "Cannot set both mergeSchema=true and overwriteSchema=true for Iceberg writes"
                .to_string(),
        )),
        (true, false) => Ok(Some(SchemaMode::Merge)),
        (false, true) => {
            if matches!(sink_mode, PhysicalSinkMode::Overwrite) {
                Ok(Some(SchemaMode::Overwrite))
            } else {
                Err(DataFusionError::Plan(
                    "overwriteSchema option can only be used with overwrite mode for Iceberg"
                        .to_string(),
                ))
            }
        }
        (false, false) => Ok(None),
    }
}
