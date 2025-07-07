use std::collections::HashMap;
use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{SessionState, TaskContext};
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{Action, Add};
use deltalake::logstore::LogStoreRef;
use deltalake::protocol::SaveMode;
use deltalake::table::state::DeltaTableState;
use deltalake::DeltaTable;
use futures::future::BoxFuture;
use futures::StreamExt;
use parquet::file::properties::WriterProperties;
use tokio::task::JoinHandle;

use super::WriteError;

pub struct DeltaWriter {
    // TODO: would handle parquet writing
    _placeholder: (),
}

impl DeltaWriter {
    pub fn new() -> Self {
        Self { _placeholder: () }
    }

    pub async fn write(
        &mut self,
        _batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<(), DeltaTableError> {
        // TODO: Implement parquet writing logic
        // This would involve:
        // 1. Converting RecordBatch to parquet format
        // 2. Writing to object store
        // 3. Tracking file metadata
        Ok(())
    }

    pub async fn close(self) -> Result<Vec<Add>, DeltaTableError> {
        // TODO: Implement finalization logic
        // This would return Add actions for the files written
        Ok(vec![])
    }
}

/// Write data into a Delta table using DataFusion DataFrame
pub struct WriteBuilder {
    /// Delta log store for handling metadata and transaction log
    log_store: LogStoreRef,
    /// A snapshot of the table's state (None for new tables)
    snapshot: Option<DeltaTableState>,
    /// Input DataFrame to write
    input_dataframe: Option<DataFrame>,
    /// DataFusion session state for plan execution
    session_state: Option<SessionState>,
    /// Save mode defines how to treat existing data
    mode: SaveMode,
    /// Column names for table partitioning
    partition_columns: Option<Vec<String>>,
    /// Size above which we will write a buffered parquet file to disk
    target_file_size: Option<usize>,
    /// Number of records to be written in single batch to underlying writer
    write_batch_size: Option<usize>,
    /// Parquet writer properties
    writer_properties: Option<WriterProperties>,
    /// Whether to use safe casting (return NULL on cast failure vs error)
    safe_cast: bool,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// Table name (used when creating new tables)
    name: Option<String>,
    /// Table description (used when creating new tables)
    description: Option<String>,
    /// Table configuration (used when creating new tables)
    configuration: HashMap<String, Option<String>>,
}

impl WriteBuilder {
    /// Create a new WriteBuilder
    pub fn new(log_store: LogStoreRef, snapshot: Option<DeltaTableState>) -> Self {
        Self {
            log_store,
            snapshot,
            input_dataframe: None,
            session_state: None,
            mode: SaveMode::Append,
            partition_columns: None,
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            safe_cast: true,
            commit_properties: CommitProperties::default(),
            name: None,
            description: None,
            configuration: HashMap::new(),
        }
    }

    /// Set the input DataFrame to write
    pub fn with_input_dataframe(mut self, dataframe: DataFrame) -> Self {
        self.input_dataframe = Some(dataframe);
        self
    }

    /// Set the session state for plan execution
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.session_state = Some(state);
        self
    }

    /// Set the save mode
    pub fn with_save_mode(mut self, mode: SaveMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set partition columns
    pub fn with_partition_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_columns = Some(columns.into_iter().map(|c| c.into()).collect());
        self
    }

    /// Set target file size for parquet files
    pub fn with_target_file_size(mut self, size: usize) -> Self {
        self.target_file_size = Some(size);
        self
    }

    /// Set write batch size
    pub fn with_write_batch_size(mut self, size: usize) -> Self {
        self.write_batch_size = Some(size);
        self
    }

    /// Set parquet writer properties
    pub fn with_writer_properties(mut self, props: WriterProperties) -> Self {
        self.writer_properties = Some(props);
        self
    }

    /// Set whether to use safe casting
    pub fn with_safe_cast(mut self, safe: bool) -> Self {
        self.safe_cast = safe;
        self
    }

    /// Set commit properties
    pub fn with_commit_properties(mut self, props: CommitProperties) -> Self {
        self.commit_properties = props;
        self
    }

    /// Set table name (for new table creation)
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set table description (for new table creation)
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set table configuration (for new table creation)
    pub fn with_configuration(
        mut self,
        configuration: impl IntoIterator<Item = (impl Into<String>, Option<impl Into<String>>)>,
    ) -> Self {
        self.configuration = configuration
            .into_iter()
            .map(|(k, v)| (k.into(), v.map(|s| s.into())))
            .collect();
        self
    }

    /// Validate preconditions before execution
    fn validate(&self) -> Result<(), WriteError> {
        if self.input_dataframe.is_none() {
            return Err(WriteError::MissingDataFrame);
        }
        if self.session_state.is_none() {
            return Err(WriteError::MissingSessionState);
        }
        Ok(())
    }

    /// Get partition columns, validating against table schema if available
    fn get_partition_columns(&self) -> Result<Vec<String>, WriteError> {
        match (&self.partition_columns, &self.snapshot) {
            (Some(columns), Some(snapshot)) => {
                // Validate partition columns against existing table
                let table_partition_cols = snapshot.metadata().partition_columns.clone();
                if table_partition_cols != *columns {
                    return Err(WriteError::PartitionColumnMismatch {
                        expected: table_partition_cols,
                        got: columns.clone(),
                    });
                }
                Ok(columns.clone())
            }
            (Some(columns), None) => Ok(columns.clone()),
            (None, Some(snapshot)) => Ok(snapshot.metadata().partition_columns.clone()),
            (None, None) => Ok(vec![]),
        }
    }

    /// Execute the write operation
    async fn execute_write(self) -> Result<DeltaTable, WriteError> {
        // Validate preconditions
        self.validate()?;

        // Get partition columns first, before moving values out of self
        let partition_columns = self.get_partition_columns()?;

        // Extract needed values after getting partition columns
        let dataframe = self.input_dataframe.unwrap();
        let session_state = self.session_state.unwrap();
        let log_store = self.log_store.clone();
        let snapshot = self.snapshot.clone();

        // Convert DataFrame to ExecutionPlan
        let logical_plan = dataframe.logical_plan().clone();
        let execution_plan = session_state
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| WriteError::PhysicalPlan { source: e })?;

        // Log partition columns for debugging
        if !partition_columns.is_empty() {
            // TODO: Use partition_columns to properly partition the data during writing
            eprintln!(
                "DEBUG: Writing with partition columns: {:?}",
                partition_columns
            );
        }

        // Execute the plan and write data
        let mut tasks = Vec::new();
        let partition_count = execution_plan
            .properties()
            .output_partitioning()
            .partition_count();

        for partition_id in 0..partition_count {
            let plan = execution_plan.clone();
            let task_ctx = Arc::new(TaskContext::from(&session_state));

            let task: JoinHandle<Result<Vec<Add>, WriteError>> = tokio::spawn(async move {
                let mut writer = DeltaWriter::new();
                let mut stream = plan
                    .execute(partition_id, task_ctx)
                    .map_err(|e| WriteError::PhysicalPlan { source: e })?;

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.map_err(|e| WriteError::PhysicalPlan { source: e })?;
                    writer
                        .write(&batch)
                        .await
                        .map_err(|e| WriteError::DeltaWriter { source: e })?;
                }

                let add_actions = writer
                    .close()
                    .await
                    .map_err(|e| WriteError::DeltaWriter { source: e })?;
                Ok(add_actions)
            });

            tasks.push(task);
        }

        // Wait for all tasks to complete
        let mut all_actions = Vec::new();
        for task in tasks {
            let add_actions = task
                .await
                .map_err(|e| WriteError::WriteTask { source: e })?
                .map_err(|e| e)?;

            for add_action in add_actions {
                all_actions.push(Action::Add(add_action));
            }
        }

        // Create or update the table
        let table = if let Some(_snapshot) = snapshot {
            // Use the public API to create a table from existing state
            let table = DeltaTable::new(log_store, Default::default());
            // TODO: Properly handle table state restoration
            table
        } else {
            // TODO: Handle table creation properly
            return Err(WriteError::CommitFailed {
                message: "Table creation not yet implemented".to_string(),
            });
        };

        // Commit the transaction
        // TODO: Use CommitBuilder and handle different SaveModes properly
        if !all_actions.is_empty() {
            // TODO: Needs more implementation
            return Err(WriteError::CommitFailed {
                message: "Transaction commit logic needs full implementation".to_string(),
            });
        }

        Ok(table)
    }
}

impl std::future::IntoFuture for WriteBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.execute_write().await.map_err(|e| e.into()) })
    }
}
