use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::MemTable;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result};
use futures::TryStreamExt;
use sail_delta_lake::{
    create_delta_table_with_object_store, open_table_with_object_store, DeltaTable, SaveMode,
    StorageConfig, WriteBuilder,
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
        schema: SchemaRef,
    ) -> Self {
        Self {
            options,
            table_paths,
            schema,
        }
    }

    /// Get the table path as a string
    fn table_path(&self) -> Result<String> {
        Ok(self.table_paths[0].as_str().to_string())
    }

    /// Extract storage configuration from options
    fn extract_storage_config(&self) -> Result<StorageConfig> {
        let mut storage_options = HashMap::new();

        // Extract AWS S3 configuration
        if let Some(access_key_id) = self.options.get("aws.access_key_id") {
            storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key_id.clone());
        }
        if let Some(secret_access_key) = self.options.get("aws.secret_access_key") {
            storage_options.insert(
                "AWS_SECRET_ACCESS_KEY".to_string(),
                secret_access_key.clone(),
            );
        }
        if let Some(region) = self.options.get("aws.region") {
            storage_options.insert("AWS_REGION".to_string(), region.clone());
        }
        if let Some(endpoint) = self.options.get("aws.endpoint") {
            storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint.clone());
        }

        // Extract Azure configuration
        if let Some(account_name) = self.options.get("azure.account_name") {
            storage_options.insert(
                "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
                account_name.clone(),
            );
        }
        if let Some(account_key) = self.options.get("azure.account_key") {
            storage_options.insert("AZURE_STORAGE_ACCOUNT_KEY".to_string(), account_key.clone());
        }

        // Extract GCS configuration
        if let Some(service_account_path) = self.options.get("gcs.service_account_path") {
            storage_options.insert(
                "GOOGLE_SERVICE_ACCOUNT".to_string(),
                service_account_path.clone(),
            );
        }

        StorageConfig::parse_options(storage_options)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Get object store from TaskContext
    fn get_object_store(
        &self,
        context: &Arc<TaskContext>,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let table_path = self.table_path()?;
        let table_url = url::Url::parse(&table_path)
            .map_err(|e| DataFusionError::Plan(format!("Invalid table URI: {}", e)))?;

        context
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
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
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaDataSink(table_path={:?})", self.table_path())
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={:?}", self.table_path())
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
        // Collect all RecordBatch data
        let batches = data.try_collect::<Vec<_>>().await?;

        if batches.is_empty() {
            return Ok(0);
        }

        // Calculate total rows
        let total_rows = batches.iter().map(|batch| batch.num_rows() as u64).sum();

        // Use TaskContext's configuration to create a new SessionContext, not the default configuration
        let session_config = context.session_config().clone();
        let runtime_env = context.runtime_env();
        let session_context = datafusion::execution::context::SessionContext::new_with_config_rt(
            session_config,
            runtime_env,
        );

        // Create MemTable from RecordBatch
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;

        // Register MemTable to SessionContext
        session_context.register_table("temp_table", Arc::new(mem_table))?;

        // Create DataFrame
        let df = session_context.table("temp_table").await?;

        // Get table path, storage config and object store
        let table_path = self.table_path()?;
        let storage_config = self.extract_storage_config()?;
        let object_store = self.get_object_store(context)?;

        // Check if table exists
        let table_exists =
            open_table_with_object_store(&table_path, object_store.clone(), storage_config.clone())
                .await
                .is_ok();

        if table_exists {
            // Table exists, execute append operation
            let table = open_table_with_object_store(&table_path, object_store, storage_config)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut write_builder = WriteBuilder::new(
                table.log_store().clone(),
                Some(
                    table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .clone(),
                ),
            );

            // Set save mode to append
            write_builder = write_builder.with_save_mode(SaveMode::Append);

            // Set input data and session state
            write_builder = write_builder.with_input_dataframe(df);
            write_builder = write_builder.with_session_state(session_context.state());

            // Execute write
            let _result = write_builder
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        } else {
            // Table does not exist, create new table
            let delta_ops =
                create_delta_table_with_object_store(&table_path, object_store, storage_config)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let table: DeltaTable = delta_ops.into();
            let mut write_builder = WriteBuilder::new(table.log_store().clone(), None);

            // Set save mode to overwrite (create new table)
            write_builder = write_builder.with_save_mode(SaveMode::Overwrite);

            // Set input data and session state
            write_builder = write_builder.with_input_dataframe(df);
            write_builder = write_builder.with_session_state(session_context.state());

            // Execute
            let _result = write_builder
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        Ok(total_rows)
    }
}
