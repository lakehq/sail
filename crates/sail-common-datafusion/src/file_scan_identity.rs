use std::sync::Arc;

use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use object_store::ObjectStore;

/// Evidence supplied by Sail's Parquet planner that these opaque factories
/// implement ordinary cached reads and Sail's default schema evolution. The
/// optimizer verifies their identity after pushdown instead of assuming that
/// arbitrary user-provided factories have equivalent behavior.
#[derive(Debug)]
pub struct ParquetScanIdentity {
    pub reader_factory: Arc<dyn ParquetFileReaderFactory>,
    pub adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    pub store: Arc<dyn ObjectStore>,
}
