//! Load data from a Delta Table
//!
//! Since we've disabled the datafusion feature in delta-rs, we need to implement
//! the load operation using sail's datafusion version.

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::transaction::PROTOCOL;
use deltalake::logstore::LogStoreRef;
use deltalake::table::state::DeltaTableState;
use deltalake::{DeltaTable, DeltaTableConfig};
use futures::future::BoxFuture;

use crate::delta_datafusion::{DataFusionMixins, DeltaScanConfig, DeltaTableProvider};

/// Builder for loading data from a Delta table
#[derive(Debug, Clone)]
pub struct LoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// A sub-selection of columns to be loaded
    columns: Option<Vec<String>>,
    /// Configuration for the scan operation
    scan_config: Option<DeltaScanConfig>,
}

impl LoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            columns: None,
            scan_config: None,
        }
    }

    /// Specify column selection to load
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Specify scan configuration
    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self.scan_config = Some(config);
        self
    }
}

impl std::future::IntoFuture for LoadBuilder {
    type Output = DeltaResult<(DeltaTable, SendableRecordBatchStream)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            // Check protocol compatibility - access the snapshot field through the public API
            PROTOCOL.can_read_from(this.snapshot.snapshot())?;

            // Ensure the table is initialized with files
            if !this.snapshot.load_config().require_files {
                return Err(DeltaTableError::NotInitializedWithFiles("reading".into()));
            }

            // Create a new DeltaTable using the public API
            // We need to create a proper DeltaTableConfig
            let config = DeltaTableConfig::default();
            let mut table = DeltaTable::new(this.log_store.clone(), config);
            table.state = Some(this.snapshot.clone());

            // Get the arrow schema for column projection
            let schema = this.snapshot.arrow_schema()?;

            // Convert column names to projection indices
            let projection = this
                .columns
                .map(|cols| {
                    cols.iter()
                        .map(|col| {
                            schema.column_with_name(col).map(|(idx, _)| idx).ok_or(
                                DeltaTableError::SchemaMismatch {
                                    msg: format!("Column '{col}' does not exist in table schema."),
                                },
                            )
                        })
                        .collect::<Result<_, _>>()
                })
                .transpose()?;

            // Create scan configuration
            let scan_config = this.scan_config.unwrap_or(DeltaScanConfig {
                file_column_name: None,
                wrap_partition_values: false,
                enable_parquet_pushdown: true,
                schema: None,
            });

            // Create DeltaTableProvider for scanning
            let table_provider =
                DeltaTableProvider::try_new(this.snapshot, this.log_store, scan_config)?;

            // Create session context for scanning
            let ctx = SessionContext::new();

            // Perform the scan
            let scan_plan = table_provider
                .scan(&ctx.state(), projection.as_ref(), &[], None)
                .await
                .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

            // Coalesce partitions for better performance
            let plan = CoalescePartitionsExec::new(scan_plan);

            // Execute the plan
            let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
            let stream = plan
                .execute(0, task_ctx)
                .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

            Ok((table, stream))
        })
    }
}
