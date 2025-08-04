use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result, ToDFSchema};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::schema::StructType;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::kernel::{Action, Protocol, Remove};
use deltalake::logstore::StorageConfig;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::{DeltaOperation, SaveMode};
use futures::StreamExt;
use sail_common_datafusion::datasource::TableDeltaOptions;
use url::Url;
use uuid::Uuid;

use crate::delta_datafusion::{parse_predicate_expression, DataFusionMixins};
use crate::operations::write::execution::{prepare_predicate_actions, WriterStatsConfig};
use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::table::{create_delta_table_with_object_store, open_table_with_object_store};

#[derive(Debug)]
pub struct DeltaDataSink {
    mode: SaveMode,
    table_url: Url,
    options: TableDeltaOptions,
    schema: SchemaRef,
    partition_columns: Vec<String>,
}

impl DeltaDataSink {
    pub fn new(
        mode: SaveMode,
        table_url: Url,
        options: TableDeltaOptions,
        schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> Self {
        Self {
            mode,
            table_url,
            options,
            schema,
            partition_columns,
        }
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
            replace_where,
            merge_schema: _,
            overwrite_schema: _,
            target_file_size,
            write_batch_size,
            ..
        } = &self.options;

        let storage_config = StorageConfig::default();
        let object_store = self.get_object_store(context)?;

        use crate::delta_datafusion::create_object_store_url;
        let object_store_url = create_object_store_url(&self.table_url);
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), object_store.clone());

        let (table, table_exists) = match open_table_with_object_store(
            self.table_url.clone(),
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
                    self.table_url.clone(),
                    object_store.clone(),
                    storage_config.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                (delta_ops.0, false)
            }
        };

        let partition_columns = self.partition_columns.clone();
        let writer_properties = WriterProperties::builder().build();

        let writer_config = WriterConfig::new(
            self.schema.clone(),
            partition_columns.clone(),
            Some(writer_properties),
            *target_file_size,
            *write_batch_size,
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
        let mut predicate_str: Option<String> = None;

        let operation = if table_exists {
            if self.mode == SaveMode::Overwrite {
                if let Some(predicate) = replace_where {
                    // This is the replace_where logic
                    let snapshot = table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let df_schema = snapshot
                        .arrow_schema()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .to_dfschema()?;

                    let session_state = SessionStateBuilder::new()
                        .with_runtime_env(context.runtime_env())
                        .build();

                    let predicate_expr =
                        parse_predicate_expression(&df_schema, predicate, &session_state)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    predicate_str = Some(predicate.clone());

                    #[allow(clippy::expect_used)]
                    let current_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("System time before Unix epoch")
                        .as_millis() as i64;

                    let (remove_actions, _) = prepare_predicate_actions(
                        predicate_expr,
                        table.log_store(),
                        snapshot,
                        session_state.clone(),
                        partition_columns.clone(),
                        None,
                        current_timestamp,
                        WriterStatsConfig::new(32, None),
                        Uuid::new_v4(),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    actions.extend(remove_actions);
                } else {
                    // This is a full overwrite
                    let snapshot = table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let existing_files = snapshot
                        .file_actions()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    #[allow(clippy::expect_used)]
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
                predicate: predicate_str,
            }
        } else {
            // For new tables, we need to create Protocol and Metadata actions
            // Convert Arrow schema to Delta kernel schema
            let delta_schema: StructType = self
                .schema
                .as_ref()
                .try_into_kernel()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            // TODO: Flexible protocol
            let protocol: Protocol = serde_json::from_value(serde_json::json!({
                "minReaderVersion": 1,
                "minWriterVersion": 2,
            }))
            .unwrap();
            // FIXME: Follow upstream changes.
            #[allow(deprecated)]
            let metadata = deltalake::kernel::new_metadata(
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
