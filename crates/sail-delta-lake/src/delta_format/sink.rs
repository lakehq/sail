use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::schema::StructType;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use deltalake::kernel::{Action, Protocol};
use deltalake::logstore::StorageConfig;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::{DeltaOperation, SaveMode};
use futures::StreamExt;
use sail_common_datafusion::datasource::TableDeltaOptions;
use url::Url;

use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::table::{create_delta_table_with_object_store, open_table_with_object_store};

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

        let writer_properties = WriterProperties::builder().build();
        let writer_config = WriterConfig::new(
            self.schema.clone(),
            self.partition_columns.clone(),
            Some(writer_properties),
            *target_file_size,
            *write_batch_size,
            32, // TODO: Default num_indexed_cols for now
            None,
        );

        let writer_path = object_store::path::Path::from(self.table_url.path());
        let mut writer = DeltaWriter::new(object_store.clone(), writer_path, writer_config);
        let mut total_rows = 0;

        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows() as u64;
            writer
                .write(&batch)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        let add_actions = writer
            .close()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut actions: Vec<Action> = self.initial_actions.clone();
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
                mode: SaveMode::Append,
                partition_by: if self.partition_columns.is_empty() {
                    None
                } else {
                    Some(self.partition_columns.clone())
                },
                predicate: None,
            })
        };

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
