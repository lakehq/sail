use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use sail_delta_lake::operations::write::writer::{DeltaWriter, WriterConfig};
use sail_delta_lake::{
    create_delta_table_with_object_store, open_table_with_object_store, Action, CommitBuilder,
    CommitProperties, DeltaOperation, DeltaTable, Remove, SaveMode, StorageConfig,
    WriterProperties,
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
        let table_path = self.table_path()?;
        let storage_config = self.extract_storage_config()?;
        let object_store = self.get_object_store(context)?;
        let save_mode = self.parse_save_mode();

        dbg!("Starting write_all with save_mode: {:?}", &save_mode);

        // 1. Get or create DeltaTable instance
        let table = match open_table_with_object_store(
            &table_path,
            object_store.clone(),
            storage_config.clone(),
        )
        .await
        {
            Ok(table) => {
                dbg!("Table exists, using existing table");
                table
            }
            Err(_) if save_mode == SaveMode::Overwrite => {
                dbg!("Table does not exist, creating new table with SaveMode::Overwrite");
                // If in overwrite mode and table does not exist, create a new table
                create_delta_table_with_object_store(
                    &table_path,
                    object_store.clone(),
                    storage_config.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into()
            }
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };

        // 2. Configure and create DeltaWriter
        let partition_columns = self.parse_partition_columns();
        let writer_properties = WriterProperties::builder().build();

        let writer_config = WriterConfig::new(
            self.schema.clone(),
            partition_columns.clone(),
            Some(writer_properties),
            self.parse_target_file_size(),
            self.parse_write_batch_size(),
            32, // Default num_indexed_cols for now
            None,
        );

        let mut writer = DeltaWriter::new(object_store.clone(), writer_config);
        let mut total_rows = 0;

        // 3. Consume input stream and write data
        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows() as u64;
            dbg!("Writing batch with {} rows", batch.num_rows());
            writer
                .write(&batch)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        // 4. Close writer and get Add actions
        let add_actions = writer
            .close()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        dbg!("Writer closed, got {} add actions", add_actions.len());

        if add_actions.is_empty() && save_mode != SaveMode::Overwrite {
            // If no data is written and not in overwrite mode, return
            return Ok(0);
        }

        // 5. Execute transaction commit
        let mut actions: Vec<Action> = add_actions.into_iter().map(Action::Add).collect();

        if save_mode == SaveMode::Overwrite {
            // In overwrite mode, delete existing files
            if let Ok(snapshot) = table.snapshot() {
                let existing_files = snapshot
                    .file_actions()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let current_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let remove_actions: Vec<Action> = existing_files
                    .into_iter()
                    .map(|add| {
                        Action::Remove(Remove {
                            path: add.path.clone(),
                            deletion_timestamp: Some(current_timestamp),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(add.partition_values.clone()),
                            size: Some(add.size),
                            deletion_vector: add.deletion_vector.clone(),
                            tags: None,
                            base_row_id: add.base_row_id,
                            default_row_commit_version: add.default_row_commit_version,
                        })
                    })
                    .collect();
                actions.extend(remove_actions);
            }
        }

        let operation = DeltaOperation::Write {
            mode: save_mode,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns)
            },
            predicate: self.options.get("replaceWhere").cloned(),
        };

        dbg!("Committing transaction with {} actions", actions.len());

        // Use CommitBuilder to handle transaction
        let commit_properties = CommitProperties::default();
        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .build(Some(snapshot), table.log_store(), operation)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        dbg!("Transaction committed successfully");
        Ok(total_rows)
    }
}
