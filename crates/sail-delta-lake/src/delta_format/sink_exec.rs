use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{EquivalenceProperties, LexRequirement};
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::kernel::schema::StructType;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
#[allow(deprecated)]
use deltalake::kernel::{Action, MetadataExt, Protocol};
use deltalake::logstore::StorageConfig;
use deltalake::protocol::{DeltaOperation, SaveMode};
use futures::stream::once;
use futures::StreamExt;
use sail_common_datafusion::datasource::TableDeltaOptions;
use url::Url;

use crate::delta_datafusion::type_converter::DeltaTypeConverter;
use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::table::{create_delta_table_with_object_store, open_table_with_object_store};

/// Schema handling mode for Delta Lake writes
#[derive(Debug, Clone, Copy, PartialEq)]
enum SchemaMode {
    /// Merge new schema with existing schema
    Merge,
    /// Overwrite existing schema with new schema
    Overwrite,
}

/// Custom physical execution node for Delta Lake sink operations
#[derive(Debug)]
pub struct DeltaSinkExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    initial_actions: Vec<Action>,
    operation: Option<DeltaOperation>,
    table_exists: bool,
    sink_schema: SchemaRef,
    sort_order: Option<LexRequirement>,
    cache: PlanProperties,
}

impl DeltaSinkExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        initial_actions: Vec<Action>,
        operation: Option<DeltaOperation>,
        table_exists: bool,
        sink_schema: SchemaRef,
        sort_order: Option<LexRequirement>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            true,
        )]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            options,
            partition_columns,
            initial_actions,
            operation,
            table_exists,
            sink_schema,
            sort_order,
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

    pub fn initial_actions(&self) -> &[Action] {
        &self.initial_actions
    }

    pub fn operation(&self) -> Option<&DeltaOperation> {
        self.operation.as_ref()
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sink_schema(&self) -> &SchemaRef {
        &self.sink_schema
    }

    pub fn sort_order(&self) -> &Option<LexRequirement> {
        &self.sort_order
    }
}

#[async_trait]
impl ExecutionPlan for DeltaSinkExec {
    fn name(&self) -> &'static str {
        "DeltaSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaSinkExec requires exactly one child");
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.options.clone(),
            self.partition_columns.clone(),
            self.initial_actions.clone(),
            self.operation.clone(),
            self.table_exists,
            self.sink_schema.clone(),
            self.sort_order.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaSinkExec can only be executed in a single partition");
        }

        let input_stream = self.input.execute(0, Arc::clone(&context))?;

        // Clone the necessary data for the async block
        let table_url = self.table_url.clone();
        let options = self.options.clone();
        let input_schema = self.input.schema();
        let partition_columns = self.partition_columns.clone();
        let initial_actions = self.initial_actions.clone();
        let operation = self.operation.clone();
        let table_exists = self.table_exists;

        let schema = self.schema();
        let future = async move {
            // Inline the write_all logic here
            let TableDeltaOptions {
                target_file_size,
                write_batch_size,
                ..
            } = &options;

            let storage_config = StorageConfig::default();
            let object_store = Self::get_object_store(&context, &table_url)?;

            let table = if table_exists {
                open_table_with_object_store(
                    table_url.clone(),
                    object_store.clone(),
                    storage_config.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                create_delta_table_with_object_store(
                    table_url.clone(),
                    object_store.clone(),
                    storage_config.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into()
            };

            // Determine save mode from operation
            let save_mode = match &operation {
                Some(DeltaOperation::Write { mode, .. }) => *mode,
                Some(DeltaOperation::Create { mode, .. }) => *mode,
                _ => SaveMode::Append,
            };

            // Get schema mode based on options
            let schema_mode = Self::get_schema_mode(&options, save_mode)?;

            // Handle schema validation and evolution if table exists
            let (final_schema, schema_actions) = if table_exists {
                Self::handle_schema_evolution(&table, &input_schema, schema_mode).await?
            } else {
                (input_schema.clone(), Vec::new())
            };

            // Create writer with potentially evolved schema
            let writer_config = WriterConfig::new(
                final_schema.clone(),
                partition_columns.to_vec(),
                None, // TODO: Make compression configurable
                *target_file_size,
                *write_batch_size,
                32, // TODO: Default num_indexed_cols for now
                None,
            );

            let writer_path = object_store::path::Path::from(table_url.path());
            let mut writer = DeltaWriter::new(object_store.clone(), writer_path, writer_config);

            let mut total_rows = 0u64;
            let mut has_data = false;
            let mut data = input_stream;

            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;
                total_rows += batch.num_rows() as u64;
                has_data = true;

                let validated_batch = Self::validate_and_adapt_batch(batch, &final_schema)?;

                writer
                    .write(&validated_batch)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

            // Early return if no data was processed
            if !has_data {
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let add_actions = writer
                .close()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut actions: Vec<Action> = initial_actions.to_vec();
            actions.extend(schema_actions);
            actions.extend(add_actions.into_iter().map(Action::Add));

            if actions.is_empty() {
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let operation = if !table_exists {
                let delta_schema: StructType = input_schema
                    .as_ref()
                    .try_into_kernel()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                #[allow(clippy::unwrap_used)]
                let protocol: Protocol = serde_json::from_value(serde_json::json!({
                    "minReaderVersion": 1,
                    "minWriterVersion": 2,
                }))
                .unwrap();

                #[allow(deprecated)]
                let metadata = deltalake::kernel::new_metadata(
                    &delta_schema,
                    partition_columns.to_vec(),
                    HashMap::<String, String>::new(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                actions.insert(0, Action::Protocol(protocol.clone()));
                actions.insert(1, Action::Metadata(metadata.clone()));

                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: table_url.to_string(),
                    protocol,
                    metadata,
                }
            } else {
                operation.clone().unwrap_or(DeltaOperation::Write {
                    mode: save_mode,
                    partition_by: if partition_columns.is_empty() {
                        None
                    } else {
                        Some(partition_columns.to_vec())
                    },
                    predicate: None,
                })
            };

            // Commit the transaction
            let snapshot = if table_exists {
                Some(
                    table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                )
            } else {
                None
            };

            let reference = snapshot.as_ref().map(|s| *s as &dyn TableReference);

            CommitBuilder::from(CommitProperties::default())
                .with_actions(actions)
                .build(reference, table.log_store(), operation)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let array = Arc::new(UInt64Array::from(vec![total_rows]));
            let batch = RecordBatch::try_new(schema, vec![array])?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DeltaSinkExec {
    /// Get object store from TaskContext
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
        table: &deltalake::DeltaTable,
        input_schema: &SchemaRef,
        schema_mode: Option<SchemaMode>,
    ) -> Result<(SchemaRef, Vec<Action>)> {
        let table_metadata = table
            .metadata()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table_schema = table_metadata
            .parse_schema()
            .map_err(|e: delta_kernel::Error| DataFusionError::External(Box::new(e)))?;
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
                        .metadata()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
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
                    .metadata()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
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
    fn merge_schemas(
        table_schema: &datafusion::arrow::datatypes::Schema,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<SchemaRef> {
        use std::collections::HashMap;

        use datafusion::arrow::datatypes::{Field, Schema};

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
    fn validate_schema_compatibility(
        table_schema: &datafusion::arrow::datatypes::Schema,
        input_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<()> {
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

                        let casted_column = cast(batch_column, final_field.data_type())
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

impl DisplayAs for DeltaSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaSinkExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
