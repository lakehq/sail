use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::Statistics;
use datafusion_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown, TableType};
// Re-export for convenience
pub use deltalake;
use deltalake::kernel::Add;
use deltalake::logstore::LogStoreRef;
use deltalake::table::state::DeltaTableState;
use serde::{Deserialize, Serialize};

use crate::error::DeltaResult;

mod cdf;
pub mod expr;
pub mod logical;
pub mod physical;
pub mod planner;
mod schema_adapter;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additional metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
    /// Wrap partition values in a dictionary encoding
    pub wrap_partition_values: bool,
    /// Allow pushdown of the scan filter
    pub enable_parquet_pushdown: bool,
    /// Schema to read as
    pub schema: Option<SchemaRef>,
}

#[derive(Debug, Clone)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Whether to wrap partition values in a dictionary encoding to potentially save space
    _wrap_partition_values: Option<bool>,
    /// Whether to push down filter in end result or just prune the files
    _enable_parquet_pushdown: bool,
    /// Schema to scan table with
    _schema: Option<SchemaRef>,
}

/// A wrapper for parquet scans
#[derive(Debug, Clone)]
pub struct SailDeltaScan {
    /// The parquet scan to wrap
    pub parquet_scan: Arc<dyn ExecutionPlan>,
    /// The schema of the table to be used when evaluating expressions
    pub logical_schema: Arc<datafusion::arrow::datatypes::Schema>,
    /// Column that contains an index that maps to the original metadata Add
    pub config: DeltaScanConfig,
    /// Metrics for scan reported via DataFusion
    metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
}

impl DisplayAs for SailDeltaScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SailDeltaScan")
    }
}

#[async_trait]
impl ExecutionPlan for SailDeltaScan {
    fn name(&self) -> &str {
        "SailDeltaScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parquet_scan.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.parquet_scan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parquet_scan]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<datafusion::execution::SendableRecordBatchStream> {
        self.parquet_scan.execute(partition, context)
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        // This is deprecated, but let's keep it for now and delegate to the new method
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        self.parquet_scan.partition_statistics(partition)
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

pub(crate) struct SailDeltaScanBuilder<'a> {
    _snapshot: &'a DeltaTableState,
    _log_store: LogStoreRef,
    _filter: Option<Expr>,
    _session_state: &'a SessionState,
    _projection: Option<&'a Vec<usize>>,
    _limit: Option<usize>,
    _config: Option<DeltaScanConfig>,
}

impl<'a> SailDeltaScanBuilder<'a> {
    pub fn new(
        snapshot: &'a DeltaTableState,
        log_store: LogStoreRef,
        session_state: &'a SessionState,
    ) -> Self {
        Self {
            _snapshot: snapshot,
            _log_store: log_store,
            _filter: None,
            _session_state: session_state,
            _projection: None,
            _limit: None,
            _config: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self._filter = filter;
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self._projection = projection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self._limit = limit;
        self
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self._config = Some(config);
        self
    }

    pub async fn build(self) -> DeltaResult<SailDeltaScan> {
        let config = match self._config {
            Some(config) => config,
            None => unimplemented!(), // Need to implement DeltaScanConfigBuilder::build
        };

        let schema = match config.schema.clone() {
            Some(value) => value,
            None => unimplemented!(), // Need to get schema from snapshot
        };

        // TODO: Get logical schema
        let logical_schema = schema.clone();

        // TODO: Perform Pruning of files to scan
        let (_files, _files_scanned, _files_pruned) = {
            let all_files = self._snapshot.file_actions()?;
            let num_files = all_files.len();
            (all_files, num_files, 0)
        };

        // TODO: Create FileScanConfig
        // TODO: Create DataSourceExec (ParquetScan)
        let parquet_scan = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));

        Ok(SailDeltaScan {
            parquet_scan,
            logical_schema,
            config,
            metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
        })
    }
}

/// A Delta table provider that enables additional metadata columns to be included during the scan
#[derive(Debug, Clone)]
pub struct DeltaTableProvider {
    _snapshot: Arc<DeltaTableState>,
    _log_store: LogStoreRef,
    _config: DeltaScanConfig,
    _schema: Arc<datafusion::arrow::datatypes::Schema>,
    _files: Option<Vec<Add>>,
}

impl DeltaTableProvider {
    /// Build a DeltaTableProvider
    pub fn try_new(
        _snapshot: Arc<DeltaTableState>,
        _log_store: LogStoreRef,
        _config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        unimplemented!()
    }

    /// Define which files to consider while building a scan, for advanced usecases
    pub fn with_files(self, _files: Vec<Add>) -> DeltaTableProvider {
        unimplemented!()
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let filter_expr = datafusion::logical_expr::utils::conjunction(filters.iter().cloned());

        let state = session
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Session state not available".to_string(),
                )
            })?;

        let scan = SailDeltaScanBuilder::new(&self._snapshot, self._log_store.clone(), state)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .with_scan_config(self._config.clone())
            .build()
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        unimplemented!()
    }

    fn statistics(&self) -> Option<Statistics> {
        unimplemented!()
    }
}

/// Factory for creating Delta Lake table providers
#[derive(Debug)]
pub struct DeltaTableFactory;

impl DeltaTableFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DeltaTableFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _ctx: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        unimplemented!()
    }
}
