use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::Result;
use futures::TryStreamExt;
use sail_delta_lake::{
    create_delta_table_with_object_store, open_table_with_object_store, SaveMode, StorageConfig,
    WriteBuilder,
};

/// Delta Lake data sink implementation
#[derive(Debug)]
pub struct DeltaDataSink {
    options: HashMap<String, String>,
    table_paths: Vec<datafusion::datasource::listing::ListingTableUrl>,
    schema: SchemaRef,
}

impl DeltaDataSink {
    /// Create a new DeltaDataSink
    pub fn new(
        options: HashMap<String, String>,
        table_paths: Vec<datafusion::datasource::listing::ListingTableUrl>,
    ) -> Self {
        Self {
            options,
            table_paths,
            schema: Arc::new(datafusion::arrow::datatypes::Schema::empty()),
        }
    }

    /// Parse save mode from options
    fn parse_save_mode(&self) -> SaveMode {
        match self.options.get("save_mode").or(self.options.get("mode")) {
            Some(mode) => match mode.to_lowercase().as_str() {
                "append" => SaveMode::Append,
                "overwrite" => SaveMode::Overwrite,
                "errorifexists" | "error" => SaveMode::ErrorIfExists,
                "ignore" => SaveMode::Ignore,
                _ => {
                    dbg!("Unknown save mode '{}', defaulting to Append", mode);
                    SaveMode::Append
                }
            },
            None => SaveMode::Append,
        }
    }

    /// Parse partition columns from options
    fn parse_partition_columns(&self) -> Vec<String> {
        self.options
            .get("partition_columns")
            .or(self.options.get("partitionBy"))
            .map(|cols| {
                cols.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Parse target file size from options
    fn parse_target_file_size(&self) -> Option<usize> {
        self.options
            .get("target_file_size")
            .or(self.options.get("targetFileSize"))
            .and_then(|s| s.parse().ok())
    }

    /// Parse write batch size from options
    fn parse_write_batch_size(&self) -> Option<usize> {
        self.options
            .get("write_batch_size")
            .or(self.options.get("writeBatchSize"))
            .and_then(|s| s.parse().ok())
    }

    /// Create storage config from options
    fn create_storage_config(&self) -> StorageConfig {
        // For now, use default configuration
        // TODO: Parse additional storage options if needed
        StorageConfig::default()
    }
}

impl DisplayAs for DeltaDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaDataSink(table_paths={:?})", self.table_paths)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_paths={:?}", self.table_paths)
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

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        dbg!("Starting Delta Lake write operation");

        // 1. Collect all record batches
        let batches: Vec<_> = data
            .try_collect()
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        if batches.is_empty() {
            dbg!("No data to write, returning 0 rows");
            return Ok(0);
        }

        // 2. Calculate total rows
        let total_rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
        dbg!("Writing {} rows in {} batches", total_rows, batches.len());

        // 3. Get schema and create MemTable
        let schema = batches[0].schema();
        dbg!("Data schema: {:?}", &schema);

        let mem_table = MemTable::try_new(schema.clone(), vec![batches])
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // 4. Create SessionContext and DataFrame from TaskContext
        // Extract runtime_env from TaskContext to access object store registry
        let session_ctx = SessionContext::new();

        // Get the object store using the table path URL
        let table_path = self.table_paths.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Plan("No table path provided".to_string())
        })?;

        let table_uri = table_path.as_str();
        dbg!("Table URI: {}", table_uri);

        // Parse the table URL to get object store
        let table_url = url::Url::parse(table_uri).map_err(|e| {
            datafusion_common::DataFusionError::Plan(format!("Invalid table URI: {}", e))
        })?;

        // Get ObjectStore from TaskContext's runtime environment
        let object_store = context
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| {
                dbg!("Failed to get object store from registry: {}", &e);
                datafusion_common::DataFusionError::External(Box::new(e))
            })?;

        dbg!("Successfully obtained ObjectStore instance");

        // 5. Create DataFrame from MemTable
        let dataframe = session_ctx
            .read_table(Arc::new(mem_table))
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // 6. Parse configuration options
        let save_mode = self.parse_save_mode();
        let partition_columns = self.parse_partition_columns();
        let storage_config = self.create_storage_config();

        dbg!("Save mode: {:?}", &save_mode);
        dbg!("Partition columns: {:?}", &partition_columns);

        // 7. Try to open existing table or prepare for new table creation
        let table_result =
            open_table_with_object_store(table_uri, object_store.clone(), storage_config.clone())
                .await;

        let write_builder = match table_result {
            Ok(existing_table) => {
                dbg!("Found existing Delta table, preparing write operation");
                let snapshot = existing_table.snapshot().map_err(|e| {
                    dbg!("Failed to get table snapshot: {}", &e);
                    datafusion_common::DataFusionError::External(Box::new(e))
                })?;

                WriteBuilder::new(existing_table.log_store(), Some(snapshot.clone()))
            }
            Err(e) => {
                match save_mode {
                    SaveMode::ErrorIfExists => {
                        dbg!("Table does not exist and save mode is ErrorIfExists");
                        return Err(datafusion_common::DataFusionError::Plan(format!(
                            "Table does not exist at '{}' and save mode is ErrorIfExists",
                            table_uri
                        )));
                    }
                    _ => {
                        dbg!("Table does not exist, will create new table. Error: {}", &e);
                        // Create new table
                        let delta_ops = create_delta_table_with_object_store(
                            table_uri,
                            object_store,
                            storage_config,
                        )
                        .await
                        .map_err(|e| {
                            dbg!("Failed to create new Delta table: {}", &e);
                            datafusion_common::DataFusionError::External(Box::new(e))
                        })?;

                        WriteBuilder::new(delta_ops.0.log_store(), None)
                    }
                }
            }
        };

        // 8. Configure WriteBuilder
        let mut builder = write_builder
            .with_input_dataframe(dataframe)
            .with_session_state(session_ctx.state())
            .with_save_mode(save_mode);

        // Apply partition columns if specified
        if !partition_columns.is_empty() {
            builder = builder.with_partition_columns(partition_columns);
        }

        // Apply optional configurations
        if let Some(target_file_size) = self.parse_target_file_size() {
            dbg!("Setting target file size: {}", target_file_size);
            builder = builder.with_target_file_size(target_file_size);
        }

        if let Some(write_batch_size) = self.parse_write_batch_size() {
            dbg!("Setting write batch size: {}", write_batch_size);
            builder = builder.with_write_batch_size(write_batch_size);
        }

        // Add table name and description for new tables
        if let Some(table_name) = self.options.get("table_name").or(self.options.get("name")) {
            builder = builder.with_table_name(table_name.clone());
        }

        if let Some(description) = self.options.get("description") {
            builder = builder.with_description(description.clone());
        }

        // 9. Execute the write operation
        dbg!("Executing Delta Lake write operation");
        let _result_table = builder.await.map_err(|e| {
            dbg!("Delta write operation failed: {}", &e);
            datafusion_common::DataFusionError::External(Box::new(e))
        })?;

        dbg!(
            "Successfully wrote {} rows to Delta table at '{}'",
            total_rows,
            table_uri
        );
        Ok(total_rows)
    }
}
