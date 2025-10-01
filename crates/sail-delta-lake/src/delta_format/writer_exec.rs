use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::kernel::schema::StructType;
#[allow(deprecated)]
use deltalake::kernel::{Action, MetadataExt}; // TODO: Follow upstream for `MetadataExt`.
use deltalake::logstore::StorageConfig;
use deltalake::protocol::{DeltaOperation, SaveMode};
use futures::stream::{once, StreamExt};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::delta_datafusion::delta_to_datafusion_error;
use crate::delta_datafusion::type_converter::DeltaTypeConverter;
use crate::delta_format::CommitInfo;
use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::options::TableDeltaOptions;
use crate::table::open_table_with_object_store;

/// Schema handling mode for Delta Lake writes
#[derive(Debug, Clone, Copy, PartialEq)]
enum SchemaMode {
    /// Merge new schema with existing schema
    Merge,
    /// Overwrite existing schema with new schema
    Overwrite,
}

/// Physical execution node for Delta Lake writing operations
#[derive(Debug)]
pub struct DeltaWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    sink_schema: SchemaRef,
    condition: Option<Arc<dyn PhysicalExpr>>,
    cache: PlanProperties,
}

impl DeltaWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        sink_schema: SchemaRef,
        condition: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            options,
            partition_columns,
            sink_mode,
            table_exists,
            sink_schema,
            condition,
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
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

    pub fn options(&self) -> &TableDeltaOptions {
        &self.options
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sink_schema(&self) -> &SchemaRef {
        &self.sink_schema
    }

    pub fn sink_mode(&self) -> &PhysicalSinkMode {
        &self.sink_mode
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn condition(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.condition
    }
}

#[async_trait]
impl ExecutionPlan for DeltaWriterExec {
    fn name(&self) -> &'static str {
        "DeltaWriterExec"
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
            return internal_err!("DeltaWriterExec requires exactly one child");
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.options.clone(),
            self.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_exists,
            self.sink_schema.clone(),
            self.condition.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaWriterExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "DeltaWriterExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let stream = self.input.execute(0, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let options = self.options.clone();
        let partition_columns = self.partition_columns.clone();
        let sink_mode = self.sink_mode.clone();
        let table_exists = self.table_exists;
        let input_schema = self.input.schema();
        let condition = self.condition.clone();
        // let sink_schema = self.sink_schema.clone();

        let schema = self.schema();
        let future = async move {
            let TableDeltaOptions {
                target_file_size,
                write_batch_size,
                ..
            } = &options;

            let storage_config = StorageConfig::default();
            let object_store = Self::get_object_store(&context, &table_url)?;

            // Calculate initial_actions and operation based on sink_mode
            let initial_actions: Vec<Action> = Vec::new();
            let mut operation: Option<DeltaOperation> = None;

            let table_result = open_table_with_object_store(
                table_url.clone(),
                object_store.clone(),
                storage_config.clone(),
            )
            .await;

            #[allow(clippy::unwrap_used)]
            let table = if table_exists {
                Some(table_result.unwrap())
            } else {
                None
            };

            match &sink_mode {
                PhysicalSinkMode::Append => {
                    operation = Some(DeltaOperation::Write {
                        mode: SaveMode::Append,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.clone())
                        },
                        predicate: None,
                    });
                }
                PhysicalSinkMode::Overwrite => {
                    let predicate_str = condition.map(|c| format!("{}", fmt_sql(c.as_ref())));
                    operation = Some(DeltaOperation::Write {
                        mode: SaveMode::Overwrite,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.clone())
                        },
                        predicate: predicate_str,
                    });
                }
                PhysicalSinkMode::OverwriteIf { .. } => {
                    let predicate_str = condition.map(|c| format!("{}", fmt_sql(c.as_ref())));
                    operation = Some(DeltaOperation::Write {
                        mode: SaveMode::Overwrite,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.clone())
                        },
                        predicate: predicate_str,
                    });
                }
                PhysicalSinkMode::ErrorIfExists => {
                    if table_exists {
                        return Err(DataFusionError::Plan(format!(
                            "Delta table already exists at path: {table_url}"
                        )));
                    }
                }
                PhysicalSinkMode::IgnoreIfExists => {
                    if table_exists {
                        // Table exists, ignore the write operation and return empty commit info
                        let commit_info = CommitInfo {
                            row_count: 0,
                            actions: Vec::new(),
                            initial_actions: Vec::new(),
                            operation: None,
                        };
                        let commit_info_json = serde_json::to_string(&commit_info)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        let data_array = Arc::new(StringArray::from(vec![commit_info_json]));
                        let batch = RecordBatch::try_new(schema, vec![data_array])?;
                        return Ok(batch);
                    }
                }
                PhysicalSinkMode::OverwritePartitions => {
                    return Err(DataFusionError::NotImplemented(
                        "OverwritePartitions mode not implemented".to_string(),
                    ));
                }
            }

            // Handle schema evolution if table exists
            let (final_schema, schema_actions) = if let Some(table) = &table {
                // Determine save mode from operation
                let save_mode = match &operation {
                    Some(DeltaOperation::Write { mode, .. }) => *mode,
                    Some(DeltaOperation::Create { mode, .. }) => *mode,
                    _ => SaveMode::Append,
                };

                // Get schema mode based on options
                let schema_mode = Self::get_schema_mode(&options, save_mode)?;

                Self::handle_schema_evolution(table, &input_schema, schema_mode).await?
            } else {
                (input_schema.clone(), Vec::new())
            };

            let writer_config = WriterConfig::new(
                final_schema.clone(),
                partition_columns.to_vec(),
                None,
                *target_file_size,
                *write_batch_size,
                32,
                None,
            );

            let writer_path = object_store::path::Path::from(table_url.path());
            let mut writer = DeltaWriter::new(object_store.clone(), writer_path, writer_config);

            let mut total_rows = 0u64;
            let mut data = stream;

            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;
                total_rows += batch.num_rows() as u64;

                let validated_batch = Self::validate_and_adapt_batch(batch, &final_schema)?;

                writer
                    .write(&validated_batch)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

            let add_actions = writer
                .close()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Combine add_actions and schema_actions into a single actions vector
            let mut actions: Vec<Action> = schema_actions;
            actions.extend(add_actions.into_iter().map(Action::Add));

            let commit_info = CommitInfo {
                row_count: total_rows,
                actions,
                initial_actions,
                operation,
            };
            let commit_info_json = serde_json::to_string(&commit_info)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let data_array = Arc::new(StringArray::from(vec![commit_info_json]));
            let batch = RecordBatch::try_new(schema, vec![data_array])?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DeltaWriterExec {
    fn get_object_store(
        context: &Arc<TaskContext>,
        table_url: &Url,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        context
            .runtime_env()
            .object_store_registry
            .get_store(table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Determine the schema mode based on options and save mode
    fn get_schema_mode(
        options: &TableDeltaOptions,
        save_mode: SaveMode,
    ) -> Result<Option<SchemaMode>> {
        match (options.merge_schema, options.overwrite_schema) {
            (true, true) => Err(DataFusionError::Plan(
                "Cannot specify both mergeSchema and overwriteSchema options".to_string(),
            )),
            (false, true) => {
                if save_mode != SaveMode::Overwrite {
                    Err(DataFusionError::Plan(
                        "overwriteSchema option can only be used with overwrite save mode"
                            .to_string(),
                    ))
                } else {
                    Ok(Some(SchemaMode::Overwrite))
                }
            }
            (true, false) => Ok(Some(SchemaMode::Merge)),
            (false, false) => Ok(None), // Default, should check
        }
    }

    /// Handle schema evolution based on the schema mode
    async fn handle_schema_evolution(
        table: &crate::table::DeltaTable,
        input_schema: &SchemaRef,
        schema_mode: Option<SchemaMode>,
    ) -> Result<(SchemaRef, Vec<Action>)> {
        let table_metadata = table
            .snapshot()
            .map_err(delta_to_datafusion_error)?
            .metadata();
        let table_schema = table_metadata
            .parse_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table_arrow_schema = std::sync::Arc::new((&table_schema).try_into_arrow()?);

        match schema_mode {
            Some(SchemaMode::Merge) => {
                // Merge schemas
                let merged_schema = Self::merge_schemas(&table_arrow_schema, input_schema)?;
                if merged_schema.fields() != table_arrow_schema.fields() {
                    // Schema has changed, create metadata action
                    let new_delta_schema: StructType = merged_schema
                        .as_ref()
                        .try_into_kernel()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let current_metadata = table
                        .snapshot()
                        .map_err(delta_to_datafusion_error)?
                        .metadata();
                    // TODO: Follow upstream for `with_schema`
                    #[allow(deprecated)]
                    let new_metadata = current_metadata
                        .clone()
                        .with_schema(&new_delta_schema)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    Ok((merged_schema, vec![Action::Metadata(new_metadata)]))
                } else {
                    Ok((table_arrow_schema, Vec::new()))
                }
            }
            Some(SchemaMode::Overwrite) => {
                // Use input schema as-is
                let new_delta_schema: StructType = input_schema
                    .as_ref()
                    .try_into_kernel()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let current_metadata = table
                    .snapshot()
                    .map_err(delta_to_datafusion_error)?
                    .metadata();
                // TODO: Follow upstream for `with_schema`
                #[allow(deprecated)]
                let new_metadata = current_metadata
                    .clone()
                    .with_schema(&new_delta_schema)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                Ok((input_schema.clone(), vec![Action::Metadata(new_metadata)]))
            }
            None => {
                // Validate schema compatibility
                Self::validate_schema_compatibility(&table_arrow_schema, input_schema)?;
                Ok((table_arrow_schema, Vec::new()))
            }
        }
    }

    /// Merge two Arrow schemas
    fn merge_schemas(table_schema: &Schema, input_schema: &Schema) -> Result<SchemaRef> {
        let mut field_map: HashMap<String, Field> = HashMap::new();
        let mut field_order: Vec<String> = Vec::new();

        // Add all fields from table schema first (preserve order)
        for field in table_schema.fields() {
            let field_name = field.name().clone();
            field_map.insert(field_name.clone(), field.as_ref().clone());
            field_order.push(field_name);
        }

        // Process fields from input schema
        for input_field in input_schema.fields() {
            let field_name = input_field.name().clone();

            if let Some(existing_field) = field_map.get(&field_name) {
                // Field exists in both schemas - check for type compatibility and promotion
                let promoted_field =
                    DeltaTypeConverter::promote_field_types(existing_field, input_field)?;
                field_map.insert(field_name, promoted_field);
            } else {
                // New field from input schema
                field_map.insert(field_name.clone(), input_field.as_ref().clone());
                field_order.push(field_name);
            }
        }

        // Build merged fields in the correct order
        #[allow(clippy::unwrap_used)]
        let merged_fields: Vec<Field> = field_order
            .into_iter()
            .map(|name| field_map.remove(&name).unwrap())
            .collect();

        Ok(std::sync::Arc::new(Schema::new(merged_fields)))
    }

    /// Validate schema compatibility
    fn validate_schema_compatibility(table_schema: &Schema, input_schema: &Schema) -> Result<()> {
        // Simple validation: check if all input fields exist in table schema with compatible types
        for input_field in input_schema.fields() {
            match table_schema.field_with_name(input_field.name()) {
                Ok(table_field) => {
                    if table_field.data_type() != input_field.data_type() {
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

    /// Validate and adapt a batch to match the final schema
    fn validate_and_adapt_batch(
        batch: RecordBatch,
        final_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let batch_schema = batch.schema();

        // If schemas are identical, no adaptation needed
        if batch_schema.as_ref() == final_schema.as_ref() {
            return Ok(batch);
        }

        // Check if all required fields are present and types are compatible
        let mut adapted_columns = Vec::with_capacity(final_schema.fields().len());

        for final_field in final_schema.fields() {
            match batch_schema.column_with_name(final_field.name()) {
                Some((batch_index, batch_field)) => {
                    let batch_column = batch.column(batch_index);

                    if batch_field.data_type() == final_field.data_type() {
                        adapted_columns.push(batch_column.clone());
                    } else {
                        // Types don't match, validate cast safety and attempt casting
                        DeltaTypeConverter::validate_cast_safety(
                            batch_field.data_type(),
                            final_field.data_type(),
                            final_field.name(),
                        )?;

                        let casted_column =
                            datafusion::arrow::compute::cast(batch_column, final_field.data_type())
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        adapted_columns.push(casted_column);
                    }
                }
                None => {
                    // Column missing from batch
                    if final_field.is_nullable() {
                        let null_array = datafusion::arrow::array::new_null_array(
                            final_field.data_type(),
                            batch.num_rows(),
                        );
                        adapted_columns.push(null_array);
                    } else {
                        return Err(DataFusionError::Plan(format!(
                            "Required non-nullable column '{}' is missing from input data",
                            final_field.name()
                        )));
                    }
                }
            }
        }

        // Create new RecordBatch with adapted columns
        RecordBatch::try_new(final_schema.clone(), adapted_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for DeltaWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaWriterExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
