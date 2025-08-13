use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result};
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::kernel::schema::StructType;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
// TODO: Follow upstream for MetadataExt usage!!!
#[allow(deprecated)]
use deltalake::kernel::{Action, MetadataExt, Protocol};
use deltalake::logstore::StorageConfig;
use deltalake::protocol::{DeltaOperation, SaveMode};
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

#[derive(Debug)]
pub struct DeltaDataSink {
    table_url: Url,
    options: TableDeltaOptions,
    schema: SchemaRef,
    partition_columns: Vec<String>,
    initial_actions: Vec<Action>,
    operation: Option<DeltaOperation>,
    table_exists: bool,
}

impl DeltaDataSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_url: Url,
        options: TableDeltaOptions,
        schema: SchemaRef,
        partition_columns: Vec<String>,
        initial_actions: Vec<Action>,
        operation: Option<DeltaOperation>,
        table_exists: bool,
    ) -> Self {
        Self {
            table_url,
            options,
            schema,
            partition_columns,
            initial_actions,
            operation,
            table_exists,
        }
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn options(&self) -> &TableDeltaOptions {
        &self.options
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
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

    /// Get object store from TaskContext
    fn get_object_store(
        &self,
        context: &Arc<TaskContext>,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Determine the schema mode based on options and save mode
    fn get_schema_mode(&self, save_mode: SaveMode) -> Result<Option<SchemaMode>> {
        match (self.options.merge_schema, self.options.overwrite_schema) {
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
        &self,
        table: &deltalake::DeltaTable,
        schema_mode: Option<SchemaMode>,
    ) -> Result<(SchemaRef, Vec<Action>)> {
        let table_metadata = table
            .metadata()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table_schema = table_metadata
            .parse_schema()
            .map_err(|e: delta_kernel::Error| DataFusionError::External(Box::new(e)))?;
        let table_arrow_schema = std::sync::Arc::new((&table_schema).try_into_arrow()?);
        let input_schema = self.schema.as_ref();

        match schema_mode {
            Some(SchemaMode::Merge) => {
                // Merge schemas
                let merged_schema = self.merge_schemas(&table_arrow_schema, input_schema)?;
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

                Ok((self.schema.clone(), vec![Action::Metadata(new_metadata)]))
            }
            None => {
                // Validate schema compatibility
                self.validate_schema_compatibility(&table_arrow_schema, input_schema)?;
                Ok((table_arrow_schema, Vec::new()))
            }
        }
    }

    /// Merge two Arrow schemas
    fn merge_schemas(
        &self,
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
        &self,
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
}

impl DisplayAs for DeltaDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaDataSink(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

#[async_trait]
impl DataSink for DeltaDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let TableDeltaOptions {
            target_file_size,
            write_batch_size,
            ..
        } = &self.options;

        let storage_config = StorageConfig::default();
        let object_store = self.get_object_store(context)?;

        let table = if self.table_exists {
            open_table_with_object_store(
                self.table_url.clone(),
                object_store.clone(),
                storage_config.clone(),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            create_delta_table_with_object_store(
                self.table_url.clone(),
                object_store.clone(),
                storage_config.clone(),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .into()
        };

        // Determine save mode from operation
        let save_mode = match &self.operation {
            Some(DeltaOperation::Write { mode, .. }) => *mode,
            Some(DeltaOperation::Create { mode, .. }) => *mode,
            _ => SaveMode::Append,
        };

        // Get schema mode based on options
        let schema_mode = self.get_schema_mode(save_mode)?;

        // Handle schema validation and evolution if table exists
        let (final_schema, schema_actions) = if self.table_exists {
            self.handle_schema_evolution(&table, schema_mode).await?
        } else {
            (self.schema.clone(), Vec::new())
        };

        // Create writer with potentially evolved schema
        let writer_config = WriterConfig::new(
            final_schema.clone(),
            self.partition_columns.clone(),
            None, // TODO: Make compression configurable
            *target_file_size,
            *write_batch_size,
            32, // TODO: Default num_indexed_cols for now
            None,
        );

        let writer_path = object_store::path::Path::from(self.table_url.path());
        let mut writer = DeltaWriter::new(object_store.clone(), writer_path, writer_config);

        let mut total_rows = 0u64;
        let mut has_data = false;

        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows() as u64;
            has_data = true;

            let validated_batch = self.validate_and_adapt_batch(batch, &final_schema)?;

            writer
                .write(&validated_batch)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        // Early return if no data was processed
        if !has_data {
            return Ok(0);
        }

        let add_actions = writer
            .close()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut actions: Vec<Action> = self.initial_actions.clone();
        actions.extend(schema_actions);
        actions.extend(add_actions.into_iter().map(Action::Add));

        if actions.is_empty() {
            return Ok(0);
        }

        let operation = if !self.table_exists {
            let delta_schema: StructType = self
                .schema
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
                self.partition_columns.clone(),
                HashMap::<String, String>::new(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            actions.insert(0, Action::Protocol(protocol.clone()));
            actions.insert(1, Action::Metadata(metadata.clone()));

            DeltaOperation::Create {
                mode: SaveMode::ErrorIfExists,
                location: self.table_url.to_string(),
                protocol,
                metadata,
            }
        } else {
            self.operation.clone().unwrap_or(DeltaOperation::Write {
                mode: save_mode,
                partition_by: if self.partition_columns.is_empty() {
                    None
                } else {
                    Some(self.partition_columns.clone())
                },
                predicate: None,
            })
        };

        // Commit the transaction
        let snapshot = if self.table_exists {
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

        Ok(total_rows)
    }
}

impl DeltaDataSink {
    fn validate_and_adapt_batch(
        &self,
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
