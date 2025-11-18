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
use std::collections::HashSet;
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

use crate::arrow_conversion::{
    arrow_schema_to_iceberg, arrow_type_to_iceberg, iceberg_schema_to_arrow,
};
use crate::options::TableIcebergOptions;
use crate::spec::partition::{UnboundPartitionField, UnboundPartitionSpec};
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::NestedField;
use crate::spec::TableMetadata;
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
    options: TableIcebergOptions,
    cache: PlanProperties,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaMode {
    Merge,
    Overwrite,
}

struct SchemaEvolutionOutcome {
    iceberg_schema: IcebergSchema,
    arrow_schema: Arc<Schema>,
    changed: bool,
}

const UTC_ALIASES: &[&str] = &["UTC", "+00:00", "Etc/UTC", "Z"];

impl IcebergWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        options: TableIcebergOptions,
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
            options,
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

    fn next_schema_id(table_meta: &TableMetadata) -> i32 {
        table_meta
            .schemas
            .iter()
            .map(|s| s.schema_id())
            .max()
            .unwrap_or(0)
            + 1
    }

    fn handle_schema_evolution(
        table_meta: &TableMetadata,
        input_schema: &Schema,
        mode: Option<SchemaMode>,
    ) -> Result<SchemaEvolutionOutcome> {
        let current_schema = table_meta.current_schema().cloned().ok_or_else(|| {
            DataFusionError::Plan("No current schema in Iceberg table metadata".to_string())
        })?;

        match mode {
            Some(SchemaMode::Merge) => {
                Self::merge_schema(table_meta, &current_schema, input_schema)
            }
            Some(SchemaMode::Overwrite) => {
                Self::overwrite_schema(table_meta, &current_schema, input_schema)
            }
            None => {
                let arrow_schema = Arc::new(iceberg_schema_to_arrow(&current_schema)?);
                Self::validate_exact_schema(arrow_schema.as_ref(), input_schema)?;
                Ok(SchemaEvolutionOutcome {
                    iceberg_schema: current_schema,
                    arrow_schema,
                    changed: false,
                })
            }
        }
    }

    fn validate_exact_schema(table_schema: &Schema, input_schema: &Schema) -> Result<()> {
        if table_schema.fields().len() != input_schema.fields().len() {
            return Err(DataFusionError::Plan(
                "Input data schema does not match Iceberg table schema. Set mergeSchema=true to add columns or overwriteSchema=true to replace the schema."
                    .to_string(),
            ));
        }
        Self::validate_existing_columns(table_schema, input_schema)
    }

    fn validate_existing_columns(table_schema: &Schema, input_schema: &Schema) -> Result<()> {
        for field in table_schema.fields() {
            let candidate = input_schema.field_with_name(field.name()).map_err(|_| {
                DataFusionError::Plan(format!(
                    "Column '{}' missing from input data. Set mergeSchema=true or overwriteSchema=true to evolve the schema.",
                    field.name()
                ))
            })?;
            if !Self::field_types_compatible(field, candidate) {
                return Err(DataFusionError::Plan(format!(
                    "Column '{}' has type {:?} in the table but {:?} in the input data. Use overwriteSchema=true to replace the schema.",
                    field.name(),
                    field.data_type(),
                    candidate.data_type(),
                )));
            }
        }
        Ok(())
    }

    fn field_types_compatible(table_field: &Field, input_field: &Field) -> bool {
        let table_type = table_field.data_type();
        let input_type = input_field.data_type();

        if table_type == input_type {
            return true;
        }

        matches!(
            (table_type, input_type),
            (
                DataType::Timestamp(table_unit, table_tz),
                DataType::Timestamp(input_unit, input_tz)
            ) if table_unit == input_unit
                && Self::timestamp_timezone_compatible(table_tz, input_tz)
        )
    }

    fn timestamp_timezone_compatible(
        table_tz: &Option<std::sync::Arc<str>>,
        input_tz: &Option<std::sync::Arc<str>>,
    ) -> bool {
        match (table_tz.as_deref(), input_tz.as_deref()) {
            (None, None) => true,
            (Some(a), Some(b)) => Self::tz_alias_eq(a, b),
            (None, Some(tz)) | (Some(tz), None) => Self::is_utc_alias(tz),
        }
    }

    fn tz_alias_eq(lhs: &str, rhs: &str) -> bool {
        lhs == rhs || (Self::is_utc_alias(lhs) && Self::is_utc_alias(rhs))
    }

    fn is_utc_alias(tz: &str) -> bool {
        UTC_ALIASES
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(tz.trim()))
    }

    fn merge_schema(
        table_meta: &TableMetadata,
        current_schema: &IcebergSchema,
        input_schema: &Schema,
    ) -> Result<SchemaEvolutionOutcome> {
        let current_arrow = Arc::new(iceberg_schema_to_arrow(current_schema)?);
        Self::validate_existing_columns(current_arrow.as_ref(), input_schema)?;

        let mut next_field_id = table_meta.last_column_id + 1;
        let mut additional_fields = Vec::new();
        for field in input_schema.fields() {
            if current_schema.field_by_name(field.name()).is_some() {
                continue;
            }
            let iceberg_type = arrow_type_to_iceberg(field.data_type()).map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to convert column '{}' to an Iceberg type: {e}",
                    field.name()
                ))
            })?;
            let nested = NestedField::new(
                next_field_id,
                field.name().clone(),
                iceberg_type,
                !field.is_nullable(),
            );
            additional_fields.push(Arc::new(nested));
            next_field_id += 1;
        }

        if additional_fields.is_empty() {
            return Ok(SchemaEvolutionOutcome {
                iceberg_schema: current_schema.clone(),
                arrow_schema: current_arrow,
                changed: false,
            });
        }

        let mut merged_fields: Vec<_> = current_schema.fields().iter().cloned().collect();
        merged_fields.extend(additional_fields.into_iter());
        let mut builder = IcebergSchema::builder()
            .with_schema_id(Self::next_schema_id(table_meta))
            .with_fields(merged_fields);
        let identifiers: Vec<i32> = current_schema.identifier_field_ids().collect();
        if !identifiers.is_empty() {
            builder = builder.with_identifier_field_ids(identifiers);
        }
        let new_schema = builder
            .build()
            .map_err(|e| DataFusionError::Plan(format!("Failed to build merged schema: {e}")))?;
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&new_schema)?);
        Ok(SchemaEvolutionOutcome {
            iceberg_schema: new_schema,
            arrow_schema,
            changed: true,
        })
    }

    fn overwrite_schema(
        table_meta: &TableMetadata,
        current_schema: &IcebergSchema,
        input_schema: &Schema,
    ) -> Result<SchemaEvolutionOutcome> {
        let mut identifier_names = HashSet::new();
        for id in current_schema.identifier_field_ids() {
            if let Some(name) = current_schema.name_by_field_id(id) {
                identifier_names.insert(name.to_string());
            }
        }

        let mut identifier_ids = Vec::new();
        let mut next_field_id = table_meta.last_column_id + 1;
        let mut new_fields = Vec::new();

        for field in input_schema.fields() {
            let iceberg_type = arrow_type_to_iceberg(field.data_type()).map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to convert column '{}' to an Iceberg type: {e}",
                    field.name()
                ))
            })?;
            let reused_id = current_schema
                .field_by_name(field.name())
                .map(|existing| existing.id);
            let field_id = if let Some(id) = reused_id {
                id
            } else {
                let id = next_field_id;
                next_field_id += 1;
                id
            };
            let nested = NestedField::new(
                field_id,
                field.name().clone(),
                iceberg_type,
                !field.is_nullable(),
            );
            if identifier_names.contains(field.name()) {
                identifier_ids.push(field_id);
            }
            new_fields.push(Arc::new(nested));
        }

        let mut builder = IcebergSchema::builder()
            .with_schema_id(Self::next_schema_id(table_meta))
            .with_fields(new_fields);
        if !identifier_ids.is_empty() {
            builder = builder.with_identifier_field_ids(identifier_ids);
        }
        let new_schema = builder.build().map_err(|e| {
            DataFusionError::Plan(format!("Failed to build overwritten schema: {e}"))
        })?;
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&new_schema)?);
        Ok(SchemaEvolutionOutcome {
            iceberg_schema: new_schema,
            arrow_schema,
            changed: true,
        })
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
            let input_schema = input_schema.clone();

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

            let (iceberg_schema, table_schema, default_spec, data_dir, spec_id_val, commit_schema) =
                if table_exists {
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
                    let table_meta = TableMetadata::from_json(&bytes)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let data_dir = Self::resolve_data_dir(&table_meta, &table_url);
                    let schema_outcome = Self::handle_schema_evolution(
                        &table_meta,
                        input_schema.as_ref(),
                        schema_mode,
                    )?;
                    let default_spec = table_meta.default_partition_spec().cloned();
                    let spec_id_val = default_spec.as_ref().map(|s| s.spec_id()).unwrap_or(0);
                    let commit_schema = schema_outcome
                        .changed
                        .then(|| schema_outcome.iceberg_schema.clone());
                    (
                        schema_outcome.iceberg_schema,
                        schema_outcome.arrow_schema,
                        default_spec,
                        data_dir,
                        spec_id_val,
                        commit_schema,
                    )
                } else {
                    // derive schema/spec from input for new-table overwrite
                    let input_arrow_schema = input_schema.as_ref().clone();
                    let mut iceberg_schema = arrow_schema_to_iceberg(&input_arrow_schema)?;
                    iceberg_schema = assign_top_level_field_ids(&iceberg_schema);
                    if iceberg_schema.fields().iter().any(|f| f.id == 0) {
                        return Err(DataFusionError::Plan(
                            "Invalid Iceberg schema: field id 0 detected after assignment"
                                .to_string(),
                        ));
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

            let info = crate::physical_plan::commit::IcebergCommitInfo {
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
                schema: commit_schema.clone(),
                partition_spec: if !table_exists {
                    default_spec.clone()
                } else {
                    None
                },
            };
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
