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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::stream::once;
use futures::StreamExt;
use parquet::file::properties::WriterProperties;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::datasource::type_converter::{arrow_schema_to_iceberg, iceberg_schema_to_arrow};
use crate::operations::write::config::WriterConfig;
use crate::operations::write::table_writer::IcebergTableWriter;
use crate::options::TableIcebergOptions;
use crate::physical_plan::action_schema::{
    encode_add_data_files, encode_commit_meta, iceberg_action_schema, CommitMeta,
};
use crate::schema_evolution::{SchemaEvolver, SchemaMode};
use crate::spec::partition::{
    PartitionSpec as BoundPartitionSpec, UnboundPartitionField, UnboundPartitionSpec,
};
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::{TableMetadata, TableRequirement};
use crate::utils::get_object_store_from_context;

#[derive(Debug)]
pub struct IcebergWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    options: TableIcebergOptions,
    cache: PlanProperties,
}

impl IcebergWriterExec {
    fn extract_partition_columns(
        spec: &Option<BoundPartitionSpec>,
        iceberg_schema: &IcebergSchema,
    ) -> Result<Vec<String>> {
        if let Some(spec) = spec {
            let mut cols = Vec::with_capacity(spec.fields().len());
            for f in spec.fields() {
                let field = iceberg_schema.field_by_id(f.source_id).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Partition column mismatch: field id {} missing in schema",
                        f.source_id
                    ))
                })?;
                cols.push(field.name.clone());
            }
            Ok(cols)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        options: TableIcebergOptions,
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
            cache,
        }
    }

    fn compute_properties(schema: datafusion::arrow::datatypes::SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn sink_mode(&self) -> &PhysicalSinkMode {
        &self.sink_mode
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn options(&self) -> &TableIcebergOptions {
        &self.options
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    fn get_schema_mode(
        options: &TableIcebergOptions,
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

    fn resolve_data_dir(table_meta: &TableMetadata, table_url: &Url) -> String {
        let data_dir = "data".to_string();
        if let Some(val) = table_meta
            .properties
            .get("write.data.path")
            .or_else(|| table_meta.properties.get("write.folder-storage.path"))
        {
            let raw = val.trim();
            if !raw.is_empty() {
                if let Ok(prop_url) = Url::parse(raw) {
                    if prop_url.scheme() == table_url.scheme()
                        && prop_url.host_str() == table_url.host_str()
                    {
                        let base_path = table_url.path().trim_end_matches('/');
                        let prop_path = prop_url.path().trim_start_matches('/');
                        let base_no_leading = base_path.trim_start_matches('/');
                        if let Some(stripped) = prop_path.strip_prefix(base_no_leading) {
                            let rel = stripped.trim_start_matches('/').trim_matches('/');
                            if !rel.is_empty() {
                                return rel.to_string();
                            }
                        }
                    }
                } else {
                    let prop_path = raw;
                    let base_path = table_url.path();
                    if prop_path.starts_with('/') {
                        if let Some(stripped) = prop_path
                            .strip_prefix(base_path)
                            .or_else(|| prop_path.strip_prefix(base_path.trim_start_matches('/')))
                        {
                            let rel = stripped.trim_start_matches('/').trim_matches('/');
                            if !rel.is_empty() {
                                return rel.to_string();
                            }
                        }
                    } else {
                        let rel = prop_path.trim_matches('/');
                        if !rel.is_empty() {
                            return rel.to_string();
                        }
                    }
                }
            }
        }
        data_dir
    }
}

#[async_trait]
impl ExecutionPlan for IcebergWriterExec {
    fn name(&self) -> &'static str {
        "IcebergWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
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
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_exists,
            self.options.clone(),
        )))
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
        let input_schema = self.input.schema();
        let schema_mode = Self::get_schema_mode(&self.options, &sink_mode)?;

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
            ) = if table_exists {
                let latest_meta =
                    crate::table::find_latest_metadata_file(&object_store, &table_url).await?;
                let meta_path = object_store::path::Path::from(latest_meta.as_str());
                let bytes = object_store
                    .get(&meta_path)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .bytes()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let table_meta = TableMetadata::from_json(&bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let data_dir = Self::resolve_data_dir(&table_meta, &table_url);
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
                        use crate::spec::transform::Transform;
                        for name in &partition_columns {
                            let fid = current_schema.field_id_by_name(name).ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "Partition column mismatch: column '{}' not found in schema",
                                    name
                                ))
                            })?;
                            builder = builder.add_field(fid, name.clone(), Transform::Identity);
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
                            table_partition_columns, partition_columns
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
                )
            } else {
                let input_arrow_schema = input_schema.as_ref().clone();
                let mut iceberg_schema = arrow_schema_to_iceberg(&input_arrow_schema)?;
                iceberg_schema = SchemaEvolver::assign_schema_field_ids(&iceberg_schema)?;
                if iceberg_schema.fields().iter().any(|f| f.id == 0) {
                    return Err(DataFusionError::Plan(
                        "Invalid Iceberg schema: field id 0 detected after assignment".to_string(),
                    ));
                }
                for name in &partition_columns {
                    if iceberg_schema.field_id_by_name(name).is_none() {
                        return Err(DataFusionError::Plan(format!(
                            "Partition column mismatch: column '{}' not found in schema",
                            name
                        )));
                    }
                }
                let mut builder = crate::spec::partition::PartitionSpec::builder();
                use crate::spec::transform::Transform;
                for name in &partition_columns {
                    if let Some(fid) = iceberg_schema.field_id_by_name(name) {
                        builder = builder.add_field(fid, name.clone(), Transform::Identity);
                    }
                }
                let spec = builder.build();
                let sid = spec.spec_id();
                (
                    iceberg_schema.clone(),
                    Arc::new(iceberg_schema_to_arrow(&iceberg_schema)?),
                    Some(spec),
                    "data".to_string(),
                    sid,
                    Some(iceberg_schema),
                    Vec::new(),
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
            };

            let writer_root = object_store::path::Path::from(table_url.path());
            let mut writer = IcebergTableWriter::new(
                object_store.clone(),
                writer_root,
                writer_config,
                spec_id_val,
                data_dir,
                table_url.clone(),
            );

            let mut total_rows = 0u64;
            let mut data = stream;
            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;
                let batch_row_count = batch.num_rows();
                total_rows += u64::try_from(batch_row_count).map_err(|e| {
                    DataFusionError::Execution(format!("Row count overflow: {}", e))
                })?;
                writer
                    .write(&batch)
                    .await
                    .map_err(DataFusionError::Execution)?;
            }

            let data_files = writer.close().await.map_err(DataFusionError::Execution)?;

            let commit_meta = CommitMeta {
                table_uri: table_url.to_string(),
                row_count: total_rows,
                operation: if matches!(sink_mode, PhysicalSinkMode::Overwrite) {
                    crate::spec::Operation::Overwrite
                } else {
                    crate::spec::Operation::Append
                },
                requirements: commit_requirements,
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
                write!(f, "IcebergWriterExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
