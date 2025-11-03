use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
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

use crate::arrow_conversion::iceberg_schema_to_arrow;
use crate::spec::partition::{UnboundPartitionField, UnboundPartitionSpec};
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::NestedField;
use crate::utils::get_object_store_from_context;
use crate::writer::config::WriterConfig;
use crate::writer::table_writer::IcebergTableWriter;

#[derive(Debug)]
pub struct IcebergWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    cache: PlanProperties,
}

impl IcebergWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)]));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            table_url,
            partition_columns,
            sink_mode,
            table_exists,
            cache,
        }
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

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
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
        let _input_schema = self.input.schema();

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
                        let batch = RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(StringArray::from(vec!["{}".to_string()]))],
                        )?;
                        return Ok(batch);
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

            fn assign_top_level_field_ids(schema: &IcebergSchema) -> IcebergSchema {
                let mut new_fields: Vec<std::sync::Arc<NestedField>> = Vec::new();
                for (i, f) in schema.fields().iter().enumerate() {
                    let mut newf = NestedField::new(
                        (i as i32) + 1,
                        f.name.clone(),
                        (*f.field_type).clone(),
                        f.required,
                    );
                    if let Some(doc) = &f.doc {
                        newf = newf.with_doc(doc.clone());
                    }
                    if let Some(init) = &f.initial_default {
                        newf = newf.with_initial_default(init.clone());
                    }
                    if let Some(wd) = &f.write_default {
                        newf = newf.with_write_default(wd.clone());
                    }
                    new_fields.push(std::sync::Arc::new(newf));
                }
                IcebergSchema::builder()
                    .with_fields(new_fields)
                    .build()
                    .unwrap_or_else(|_| schema.clone())
            }

            let (iceberg_schema, default_spec, data_dir, spec_id_val) = if table_exists {
                // Load table metadata directly from object store
                let latest_meta = super::super::table_format::find_latest_metadata_file(
                    &object_store,
                    &table_url,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let meta_path = object_store::path::Path::from(latest_meta.as_str());
                let bytes = object_store
                    .get(&meta_path)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .bytes()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let table_meta = crate::spec::TableMetadata::from_json(&bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let iceberg_schema = table_meta.current_schema().cloned().ok_or_else(|| {
                    DataFusionError::Plan("No current schema in table metadata".to_string())
                })?;
                let default_spec = table_meta.default_partition_spec().cloned();
                // Derive data dir from properties when possible, else default to "data"
                let mut data_dir = "data".to_string();
                if let Some(val) = table_meta
                    .properties
                    .get("write.data.path")
                    .or_else(|| table_meta.properties.get("write.folder-storage.path"))
                {
                    let raw = val.trim();
                    if !raw.is_empty() {
                        // Try parse as URL first
                        if let Ok(prop_url) = url::Url::parse(raw) {
                            if prop_url.scheme() == table_url.scheme()
                                && prop_url.host_str() == table_url.host_str()
                            {
                                let base_path = table_url.path().trim_end_matches('/');
                                let prop_path = prop_url.path().trim_start_matches('/');
                                let base_no_leading = base_path.trim_start_matches('/');
                                if let Some(stripped) = prop_path.strip_prefix(base_no_leading) {
                                    let rel = stripped.trim_start_matches('/').trim_matches('/');
                                    if !rel.is_empty() {
                                        data_dir = rel.to_string();
                                    }
                                }
                            }
                        } else {
                            // If absolute filesystem path or relative
                            let prop_path = raw;
                            let base_path = table_url.path();
                            if prop_path.starts_with('/') {
                                if let Some(stripped) =
                                    prop_path.strip_prefix(base_path).or_else(|| {
                                        prop_path.strip_prefix(base_path.trim_start_matches('/'))
                                    })
                                {
                                    let rel = stripped.trim_start_matches('/').trim_matches('/');
                                    if !rel.is_empty() {
                                        data_dir = rel.to_string();
                                    }
                                }
                            } else {
                                // treat as relative to table root
                                data_dir = prop_path.trim_matches('/').to_string();
                            }
                        }
                    }
                }
                let sid = default_spec.as_ref().map(|s| s.spec_id()).unwrap_or(0);
                (iceberg_schema, default_spec, data_dir, sid)
            } else {
                // derive schema/spec from input for new-table overwrite
                let input_arrow_schema = _input_schema.clone().as_ref().clone();
                let mut iceberg_schema =
                    crate::arrow_conversion::arrow_schema_to_iceberg(&input_arrow_schema)?;
                // Ensure valid, non-zero, unique field ids for top-level fields
                iceberg_schema = assign_top_level_field_ids(&iceberg_schema);
                // Validate that no field ids remain zero after assignment
                // Although this should never happen since ``assign_top_level_field_ids`` assigns IDs to all fields.
                // TODO: Consider removing this check.
                if iceberg_schema.fields().iter().any(|f| f.id == 0) {
                    return Err(DataFusionError::Plan(
                        "Invalid Iceberg schema: field id 0 detected after assignment".to_string(),
                    ));
                }
                // build identity partition spec from partition_columns
                let mut builder = crate::spec::partition::PartitionSpec::builder();
                use crate::spec::transform::Transform;
                for name in &partition_columns {
                    if let Some(fid) = iceberg_schema.field_id_by_name(name) {
                        builder = builder.add_field(fid, name.clone(), Transform::Identity);
                    }
                }
                let spec = builder.build();
                let sid = spec.spec_id();
                (iceberg_schema, Some(spec), "data".to_string(), sid)
            };
            let table_schema = Arc::new(iceberg_schema_to_arrow(&iceberg_schema)?);

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

            let mut info = crate::physical_plan::commit::IcebergCommitInfo {
                table_uri: table_url.to_string(),
                row_count: total_rows,
                data_files,
                manifest_path: String::new(),
                manifest_list_path: String::new(),
                updates: vec![],
                requirements: vec![],
                operation: if matches!(sink_mode, PhysicalSinkMode::Overwrite) {
                    crate::spec::Operation::Overwrite
                } else {
                    crate::spec::Operation::Append
                },
                schema: None,
                partition_spec: None,
            };
            if !table_exists {
                // include schema/spec for bootstrap overwrite
                info.schema = Some(iceberg_schema.clone());
                info.partition_spec = default_spec.clone();
            }
            let json =
                serde_json::to_string(&info).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let array = Arc::new(StringArray::from(vec![json]));
            let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
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
