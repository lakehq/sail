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
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::StreamExt;
use futures::stream::once;
use parquet::file::properties::WriterProperties;
use sail_common_datafusion::catalog::{CatalogPartitionField, LakehouseExecutionContext};
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, PhysicalSinkMode,
};
use url::Url;

use crate::datasource::type_converter::{arrow_schema_to_iceberg, iceberg_schema_to_arrow};
use crate::io::StoreContext;
use crate::operations::write::config::WriterConfig;
use crate::operations::write::table_writer::IcebergTableWriter;
use crate::physical_plan::action_schema::{
    CommitMeta, encode_add_data_files, encode_commit_meta, encode_delete_data_files,
    iceberg_action_schema,
};
use crate::physical_plan::merge_row_projection::IcebergMergeRowProjection;
use crate::physical_plan::position_delete_writer::PositionDeleteAccumulator;
use crate::physical_plan::write_location;
use crate::physical_plan::writer_options::IcebergWriterExecOptions;
use crate::schema_evolution::{SchemaEvolver, SchemaMode};
use crate::spec::partition::{
    PartitionSpec as BoundPartitionSpec, UnboundPartitionField, UnboundPartitionSpec,
};
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::{FormatVersion, TableMetadata, TableRequirement};
use crate::table::metadata_loader::metadata_location_to_object_path_string;
use crate::table_format::{
    catalog_managed_iceberg_from_properties, metadata_location_from_properties,
};
use crate::utils::get_object_store_from_context;
use crate::utils::partition_transform::{
    catalog_partition_field_from_iceberg, format_partition_expr,
    iceberg_transform_from_partition_field, partition_field_name,
};

#[derive(Debug)]
pub struct IcebergWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    partition_columns: Vec<CatalogPartitionField>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    options: IcebergWriterExecOptions,
    logical_input_schema: Option<SchemaRef>,
    merge_row_intents: bool,
    cache: Arc<PlanProperties>,
}

impl IcebergWriterExec {
    fn extract_partition_columns(
        spec: &Option<BoundPartitionSpec>,
        iceberg_schema: &IcebergSchema,
    ) -> Result<Vec<CatalogPartitionField>> {
        if let Some(spec) = spec {
            let mut cols = Vec::with_capacity(spec.fields().len());
            for f in spec.fields() {
                let field = iceberg_schema.field_by_id(f.source_id).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Partition column mismatch: field id {} missing in schema",
                        f.source_id
                    ))
                })?;
                cols.push(
                    catalog_partition_field_from_iceberg(field.name.clone(), f.transform)
                        .map_err(DataFusionError::Plan)?,
                );
            }
            Ok(cols)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<CatalogPartitionField>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        options: IcebergWriterExecOptions,
        logical_input_schema: Option<SchemaRef>,
    ) -> Self {
        let schema = match iceberg_action_schema() {
            Ok(s) => s,
            Err(e) => {
                log::error!("failed to initialize iceberg action schema: {e}");
                Arc::new(datafusion::arrow::datatypes::Schema::empty())
            }
        };
        let cache = Self::compute_properties(schema.clone());
        Self {
            input,
            table_url,
            partition_columns,
            sink_mode,
            table_exists,
            options,
            logical_input_schema,
            merge_row_intents: false,
            cache,
        }
    }

    pub fn new_merge(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<CatalogPartitionField>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        options: IcebergWriterExecOptions,
        logical_input_schema: Option<SchemaRef>,
    ) -> Self {
        let mut writer = Self::new(
            input,
            table_url,
            partition_columns,
            sink_mode,
            table_exists,
            options,
            logical_input_schema,
        );
        writer.merge_row_intents = true;
        writer
    }

    fn compute_properties(schema: datafusion::arrow::datatypes::SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn partition_columns(&self) -> &[CatalogPartitionField] {
        &self.partition_columns
    }

    pub fn sink_mode(&self) -> &PhysicalSinkMode {
        &self.sink_mode
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn options(&self) -> &IcebergWriterExecOptions {
        &self.options
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.options.lakehouse_table.as_ref()
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn logical_input_schema(&self) -> Option<&SchemaRef> {
        self.logical_input_schema.as_ref()
    }

    pub fn reads_merge_row_intents(&self) -> bool {
        self.merge_row_intents
    }

    fn input_schema_with_logical_metadata(&self, physical_schema: SchemaRef) -> SchemaRef {
        let Some(logical_schema) = self.logical_input_schema.as_ref() else {
            return physical_schema;
        };

        let fields = physical_schema
            .fields()
            .iter()
            .map(|physical_field| {
                let Ok(logical_field) = logical_schema.field_with_name(physical_field.name())
                else {
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

        Arc::new(Schema::new_with_metadata(
            fields,
            physical_schema.metadata().clone(),
        ))
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
}

#[async_trait]
impl ExecutionPlan for IcebergWriterExec {
    fn name(&self) -> &'static str {
        "IcebergWriterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // FIXME: Write data in parallel and roll files by target size once writer
        // commit metadata can be merged safely across output partitions.
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergWriterExec requires exactly one child");
        }
        let input = Arc::clone(&children[0]);
        let writer = if self.merge_row_intents {
            Self::new_merge(
                input,
                self.table_url.clone(),
                self.partition_columns.clone(),
                self.sink_mode.clone(),
                self.table_exists,
                self.options.clone(),
                self.logical_input_schema.clone(),
            )
        } else {
            Self::new(
                input,
                self.table_url.clone(),
                self.partition_columns.clone(),
                self.sink_mode.clone(),
                self.table_exists,
                self.options.clone(),
                self.logical_input_schema.clone(),
            )
        };
        Ok(Arc::new(writer))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("IcebergWriterExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "IcebergWriterExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let stream = self.input.execute(0, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let partition_columns = self.partition_columns.clone();
        let sink_mode = self.sink_mode.clone();
        let table_exists = self.table_exists;
        let merge_projection = self
            .merge_row_intents
            .then(|| IcebergMergeRowProjection::try_new(self.input.schema()))
            .transpose()?;
        let writes_position_deletes = merge_projection.is_some()
            && self
                .input
                .schema()
                .field_with_name(MERGE_ROW_INDEX_COLUMN)
                .is_ok();
        let physical_input_schema = merge_projection
            .as_ref()
            .map(IcebergMergeRowProjection::data_schema)
            .unwrap_or_else(|| self.input.schema());
        let input_schema = self.input_schema_with_logical_metadata(physical_input_schema);
        let options = self.options.clone();
        let schema_mode = Self::get_schema_mode(&options, &sink_mode)?;

        let schema = self.schema();
        let future = async move {
            match sink_mode {
                PhysicalSinkMode::ErrorIfExists => {
                    if table_exists {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg table already exists at path: {}",
                            table_url
                        )));
                    }
                }
                PhysicalSinkMode::IgnoreIfExists => {
                    if table_exists {
                        return Ok(RecordBatch::new_empty(schema.clone()));
                    }
                }
                PhysicalSinkMode::Append => {}
                PhysicalSinkMode::Overwrite => {}
                PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
                    return Err(DataFusionError::NotImplemented(
                        "predicate or partition overwrite not implemented for Iceberg".to_string(),
                    ));
                }
            }

            let object_store = get_object_store_from_context(&context, &table_url)?;
            let input_schema = input_schema.clone();

            let (
                iceberg_schema,
                table_schema,
                default_spec,
                data_dir,
                spec_id_val,
                commit_schema,
                commit_requirements,
                variant_shredding,
                table_metadata,
            ) = if table_exists {
                let latest_meta =
                    if catalog_managed_iceberg_from_properties(&options.table_properties) {
                        match metadata_location_from_properties(&options.table_properties) {
                            Some(location) => metadata_location_to_object_path_string(&location)?,
                            None => {
                                crate::table::find_latest_metadata_file(&object_store, &table_url)
                                    .await?
                            }
                        }
                    } else {
                        crate::table::find_latest_metadata_file(&object_store, &table_url).await?
                    };
                let bytes = crate::table::metadata_loader::load_metadata_file_bytes(
                    &object_store,
                    &latest_meta,
                )
                .await?;
                let table_meta = TableMetadata::from_json(&bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let data_dir = write_location::resolve_data_dir_from_options_and_properties(
                    options.write_data_path.as_deref(),
                    options.write_folder_storage_path.as_deref(),
                    &table_meta.properties,
                    &table_url,
                )?;
                let variant_shredding = options.variant_shredding_config(&table_meta.properties)?;
                // FIXME: Concurrency Issue with Schema Evolution.
                // This requires a mechanism to reserve Field IDs or restart the Writer task upon conflict.
                let schema_outcome =
                    SchemaEvolver::evolve(&table_meta, input_schema.as_ref(), schema_mode)?;
                let mut default_spec = table_meta.default_partition_spec().cloned();
                if matches!(schema_mode, Some(SchemaMode::Overwrite)) {
                    if !partition_columns.is_empty() {
                        let current_schema = schema_outcome.iceberg_schema.clone();
                        let mut builder = crate::spec::partition::PartitionSpec::builder();
                        if let Some(existing) = &default_spec {
                            builder = builder.with_spec_id(existing.spec_id());
                        }
                        for field in &partition_columns {
                            let fid = current_schema.field_id_by_name(&field.column).ok_or_else(
                                || {
                                    DataFusionError::Plan(format!(
                                        "Partition column mismatch: column '{}' not found in schema",
                                        format_partition_expr(field)
                                    ))
                                },
                            )?;
                            builder = builder.add_field(
                                fid,
                                partition_field_name(field),
                                iceberg_transform_from_partition_field(field),
                            );
                        }
                        default_spec = Some(builder.build());
                    }
                } else {
                    let table_partition_columns = {
                        let current_schema = table_meta.current_schema().ok_or_else(|| {
                            DataFusionError::Plan(
                                "Partition column mismatch: missing current schema".to_string(),
                            )
                        })?;
                        Self::extract_partition_columns(&default_spec, current_schema)?
                    };
                    if partition_columns.is_empty() {
                        // inherit table partitioning
                    } else if partition_columns != table_partition_columns {
                        return Err(DataFusionError::Plan(format!(
                            "Partition column mismatch: table uses {:?}, requested {:?}",
                            crate::utils::partition_transform::format_partition_exprs(
                                &table_partition_columns
                            ),
                            crate::utils::partition_transform::format_partition_exprs(
                                &partition_columns
                            )
                        )));
                    }
                }
                let spec_id_val = default_spec.as_ref().map(|s| s.spec_id()).unwrap_or(0);
                let commit_schema = schema_outcome
                    .changed
                    .then(|| schema_outcome.iceberg_schema.clone());
                let requirements = vec![
                    TableRequirement::LastAssignedFieldIdMatch {
                        last_assigned_field_id: table_meta.last_column_id,
                    },
                    TableRequirement::CurrentSchemaIdMatch {
                        current_schema_id: table_meta.current_schema_id,
                    },
                ];
                (
                    schema_outcome.iceberg_schema,
                    schema_outcome.arrow_schema,
                    default_spec,
                    data_dir,
                    spec_id_val,
                    commit_schema,
                    requirements,
                    variant_shredding,
                    Some(table_meta),
                )
            } else {
                let (_, metadata_properties) =
                    crate::properties::metadata_properties_from_table_properties(
                        &options.table_properties,
                    )?;
                let variant_shredding = options.variant_shredding_config(&metadata_properties)?;
                let input_arrow_schema = input_schema.as_ref().clone();
                let mut iceberg_schema = arrow_schema_to_iceberg(&input_arrow_schema)?;
                iceberg_schema = SchemaEvolver::assign_schema_field_ids(&iceberg_schema)?;
                if iceberg_schema.fields().iter().any(|f| f.id == 0) {
                    return Err(DataFusionError::Plan(
                        "Invalid Iceberg schema: field id 0 detected after assignment".to_string(),
                    ));
                }
                for field in &partition_columns {
                    if iceberg_schema.field_id_by_name(&field.column).is_none() {
                        return Err(DataFusionError::Plan(format!(
                            "Partition column mismatch: column '{}' not found in schema",
                            format_partition_expr(field)
                        )));
                    }
                }
                let mut builder = crate::spec::partition::PartitionSpec::builder();
                for field in &partition_columns {
                    if let Some(fid) = iceberg_schema.field_id_by_name(&field.column) {
                        builder = builder.add_field(
                            fid,
                            partition_field_name(field),
                            iceberg_transform_from_partition_field(field),
                        );
                    }
                }
                let spec = builder.build();
                let sid = spec.spec_id();
                (
                    iceberg_schema.clone(),
                    Arc::new(iceberg_schema_to_arrow(&iceberg_schema)?),
                    Some(spec),
                    write_location::resolve_data_dir_from_options_and_properties(
                        options.write_data_path.as_deref(),
                        options.write_folder_storage_path.as_deref(),
                        &metadata_properties,
                        &table_url,
                    )?,
                    sid,
                    Some(iceberg_schema),
                    Vec::new(),
                    variant_shredding,
                    None,
                )
            };

            // Build unbound partition spec from bound spec if present
            let unbound_spec = if let Some(spec) = default_spec.as_ref() {
                let fields = spec
                    .fields()
                    .iter()
                    .map(|pf| UnboundPartitionField {
                        source_id: pf.source_id,
                        name: pf.name.clone(),
                        transform: pf.transform,
                    })
                    .collect();
                UnboundPartitionSpec { fields }
            } else {
                UnboundPartitionSpec { fields: vec![] }
            };

            let writer_config = WriterConfig {
                table_schema: table_schema.clone(),
                partition_columns: partition_columns.clone(),
                writer_properties: WriterProperties::default(),
                target_file_size: 134_217_728,
                write_batch_size: 32 * 1024,
                num_indexed_cols: 32,
                stats_columns: None,
                iceberg_schema: Arc::new(iceberg_schema.clone()),
                partition_spec: unbound_spec,
                variant_shredding,
            };

            let writer_root = crate::utils::url_to_object_path(&table_url)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            let mut writer = IcebergTableWriter::new(
                object_store.clone(),
                writer_root,
                writer_config,
                spec_id_val,
                data_dir.clone(),
                table_url.clone(),
            );

            let mut position_deletes = if writes_position_deletes {
                let table_metadata = table_metadata.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "Iceberg MERGE position deletes require existing table metadata"
                            .to_string(),
                    )
                })?;
                match table_metadata.format_version {
                    FormatVersion::V1 => {
                        return Err(DataFusionError::Plan(
                            "Iceberg position delete writes require table format-version 2"
                                .to_string(),
                        ));
                    }
                    FormatVersion::V2 => {}
                    FormatVersion::V3 => {
                        return Err(DataFusionError::NotImplemented(
                            "Iceberg v3 MERGE MOR position delete writes are not supported; v3 requires deletion vectors".to_string(),
                        ));
                    }
                }
                Some(PositionDeleteAccumulator::default())
            } else {
                None
            };

            let mut total_rows = 0u64;
            let mut data = stream;
            while let Some(batch_result) = data.next().await {
                let input_batch = batch_result?;
                let batch = if let Some(merge_projection) = &merge_projection {
                    if let Some(position_deletes) = &mut position_deletes {
                        let table_metadata = table_metadata.as_ref().ok_or_else(|| {
                            DataFusionError::Internal(
                                "Iceberg MERGE position deletes require table metadata".to_string(),
                            )
                        })?;
                        let delete_rows =
                            merge_projection.project_position_delete_rows(&input_batch)?;
                        position_deletes.add_batch(
                            table_metadata,
                            &delete_rows,
                            MERGE_FILE_COLUMN,
                            MERGE_ROW_INDEX_COLUMN,
                        )?;
                    }
                    merge_projection.project_data_rows(&input_batch)?
                } else {
                    input_batch
                };
                let batch_row_count = batch.num_rows();
                if batch_row_count == 0 {
                    continue;
                }
                total_rows += u64::try_from(batch_row_count).map_err(|e| {
                    DataFusionError::Execution(format!("Row count overflow: {}", e))
                })?;
                writer
                    .write(&batch)
                    .await
                    .map_err(DataFusionError::Execution)?;
            }

            let data_files = writer.close().await.map_err(DataFusionError::Execution)?;
            let delete_files = if let Some(position_deletes) = position_deletes {
                let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;
                position_deletes
                    .finish(&store_ctx, &table_url, &data_dir)
                    .await?
            } else {
                Vec::new()
            };

            let commit_meta = CommitMeta {
                table_uri: table_url.to_string(),
                row_count: total_rows,
                requirements: commit_requirements,
                table_properties: options.table_properties,
                lakehouse_table: options.lakehouse_table,
                schema: commit_schema.clone(),
                partition_spec: if !table_exists
                    || matches!(schema_mode, Some(SchemaMode::Overwrite))
                {
                    default_spec.clone()
                } else {
                    None
                },
            };

            let schema = iceberg_action_schema()?;
            let batches = vec![
                encode_add_data_files(data_files)?,
                encode_delete_data_files(delete_files)?,
                encode_commit_meta(commit_meta)?,
            ];
            let batch = concat_batches(&schema, &batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergWriterExec(table_path={}", self.table_url)?;
                if self.merge_row_intents {
                    write!(f, ", merge_row_intents=true")?;
                }
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)?;
                if self.merge_row_intents {
                    write!(f, ", merge_row_intents=true")?;
                }
                Ok(())
            }
        }
    }
}
