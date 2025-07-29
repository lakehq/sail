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
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::schema::StructType;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::kernel::{Action, Protocol, Remove};
use deltalake::logstore::StorageConfig;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::{DeltaOperation, SaveMode};
use futures::StreamExt;
use url::Url;

use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::table::{create_delta_table_with_object_store, open_table_with_object_store};

#[derive(Debug)]
pub struct DeltaDataSink {
    mode: SaveMode,
    table_url: Url,
    // TODO: maybe here we should accept parsed options?
    //   For example, `ParquetSink` accepts `TableParquetOptions`.
    options: HashMap<String, String>,
    schema: SchemaRef,
}

impl DeltaDataSink {
    pub fn new(
        mode: SaveMode,
        table_url: Url,
        options: HashMap<String, String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            mode,
            table_url,
            options,
            schema,
        }
    }

    fn extract_storage_config(&self) -> Result<StorageConfig> {
        let mut storage_options = HashMap::new();
        for (key, value) in &self.options {
            if key.starts_with("storage.") {
                storage_options.insert(key.clone(), value.clone());
            }
        }

        StorageConfig::parse_options(storage_options)
            .map_err(|e| DataFusionError::External(Box::new(e)))
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

    // TODO: The following parsing methods does not make sense, we should find a better way to handle spec::Write.
    // Maybe datafusion has handled it already.
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
    #[allow(dead_code)]
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
        let storage_config = self.extract_storage_config()?;
        let object_store = self.get_object_store(context)?;

        let (table, table_exists) = match open_table_with_object_store(
            &self.table_url,
            object_store.clone(),
            storage_config.clone(),
        )
        .await
        {
            Ok(table) => (table, true),
            Err(e) => {
                if self.mode != SaveMode::Overwrite && self.mode != SaveMode::Append {
                    return Err(DataFusionError::External(Box::new(e)));
                }
                let delta_ops = create_delta_table_with_object_store(
                    &self.table_url,
                    object_store.clone(),
                    storage_config.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                (delta_ops.0, false)
            }
        };

        let partition_columns = self.parse_partition_columns();
        let writer_properties = WriterProperties::builder().build();

        let writer_config = WriterConfig::new(
            self.schema.clone(),
            partition_columns.clone(),
            Some(writer_properties),
            self.parse_target_file_size(),
            self.parse_write_batch_size(),
            32, // TODO: Default num_indexed_cols for now
            None,
        );

        let writer_path = if self.table_url.scheme() == "file" {
            let filesystem_path = self.table_url.path();
            object_store::path::Path::from(filesystem_path)
        } else {
            // For other schemes (s3://, etc.), use the full URL as-is
            object_store::path::Path::from(self.table_url.as_str())
        };

        let mut writer = DeltaWriter::new(object_store.clone(), writer_path, writer_config);
        let mut total_rows = 0;

        // Consume input stream and write data
        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows() as u64;
            writer
                .write(&batch)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        // Close writer and get Add actions
        let add_actions = writer
            .close()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if add_actions.is_empty() && table_exists && self.mode != SaveMode::Overwrite {
            return Ok(0);
        }

        // Prepare actions for commit
        let mut actions: Vec<Action> = add_actions.into_iter().map(Action::Add).collect();

        let operation = if table_exists {
            if self.mode == SaveMode::Overwrite {
                // In overwrite mode, delete existing files
                if let Ok(snapshot) = table.snapshot() {
                    let existing_files = snapshot
                        .file_actions()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let current_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("System time before Unix epoch")
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
            DeltaOperation::Write {
                mode: self.mode,
                partition_by: if partition_columns.is_empty() {
                    None
                } else {
                    Some(partition_columns)
                },
                predicate: self.options.get("replaceWhere").cloned(),
            }
        } else {
            // For new tables, we need to create Protocol and Metadata actions
            // Convert Arrow schema to Delta kernel schema
            let delta_schema: StructType = self
                .schema
                .as_ref()
                .try_into_kernel()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let protocol = Protocol::default();
            let metadata = crate::kernel::models::actions::new_metadata(
                &delta_schema,
                partition_columns.clone(),
                HashMap::<String, String>::new(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Insert Protocol and Metadata actions at the beginning
            actions.insert(0, Action::Protocol(protocol.clone()));
            actions.insert(1, Action::Metadata(metadata.clone()));

            DeltaOperation::Create {
                mode: SaveMode::ErrorIfExists, // Required for Create operation
                location: self.table_url.to_string(),
                protocol,
                metadata,
            }
        };

        if actions.is_empty() {
            return Ok(total_rows);
        }

        // Commit transaction
        let snapshot = if table_exists {
            Some(
                table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )
        } else {
            None
        };

        CommitBuilder::from(CommitProperties::default())
            .with_actions(actions)
            .build(
                snapshot.as_ref().map(|s| *s as &dyn TableReference),
                table.log_store(),
                operation,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(total_rows)
    }
}
