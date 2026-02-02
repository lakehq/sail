use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;

use super::exec::PythonDataSourceExec;
use super::executor::PythonExecutor;
use super::filter::{expr_to_filter, exprs_to_python_filters};

/// TableProvider for Python-defined DataSources.
///
/// This allows Python DataSources to be registered in DataFusion's catalog
/// and used in SQL queries.
///
/// # Architecture
///
/// Uses the `PythonExecutor` trait abstraction to enable:
/// - Phase 1 (MVP): `InProcessExecutor` for direct PyO3 calls
/// - Phase 3: `RemoteExecutor` for subprocess isolation
#[derive(Debug)]
pub struct PythonTableProvider {
    /// Executor for Python operations (in-process or remote)
    executor: Arc<dyn PythonExecutor>,
    /// Pickled Python DataSource instance
    command: Vec<u8>,
    /// Cached schema
    schema: SchemaRef,
}

impl PythonTableProvider {
    /// Create a new PythonTableProvider.
    ///
    /// # Arguments
    /// * `executor` - The executor for Python operations
    /// * `command` - Pickled Python DataSource instance
    /// * `schema` - The schema of the data
    pub fn new(executor: Arc<dyn PythonExecutor>, command: Vec<u8>, schema: SchemaRef) -> Self {
        Self {
            executor,
            command,
            schema,
        }
    }

    /// Get the pickled command bytes.
    pub fn command(&self) -> &[u8] {
        &self.command
    }
}

#[async_trait]
impl TableProvider for PythonTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Scan the Python DataSource.
    ///
    /// This creates a physical execution plan for reading from the Python DataSource.
    ///
    /// # Filter Pushdown
    ///
    /// Filters that can be converted to Python filter objects are pushed down
    /// to the Python `DataSourceReader.pushFilters()` method. Filters returned
    /// by that method (rejected) will be applied post-read by DataFusion.
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Convert filters to Python format
        let (pushed_filters, _unpushed_exprs) = exprs_to_python_filters(filters);

        // Get partitions from Python via executor, passing filters to push
        // This ensures pushFilters() and partitions() are called on the same reader instance
        let partitions = self
            .executor
            .get_partitions(&self.command, &self.schema, pushed_filters)
            .await?;

        // Create execution plan (executor is created lazily in execute())
        let exec = PythonDataSourceExec::new(self.command.clone(), self.schema.clone(), partitions);
        let exec = Arc::new(exec) as Arc<dyn ExecutionPlan>;

        // Apply projection if present
        let exec = if let Some(projection) = projection {
            let exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> = projection
                .iter()
                .map(|&i| {
                    let field = self.schema.field(i);
                    (
                        Arc::new(datafusion::physical_expr::expressions::Column::new(
                            field.name(),
                            i,
                        ))
                            as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
                        field.name().clone(),
                    )
                })
                .collect();
            Arc::new(datafusion::physical_plan::projection::ProjectionExec::try_new(exprs, exec)?)
        } else {
            exec
        };

        Ok(exec)
    }

    /// Determine which filters can be pushed down to Python.
    ///
    /// Filters that can be converted to Python filter classes (EqualTo, GreaterThan, etc.)
    /// are marked as `Inexact` (meaning Python will apply them, but DataFusion may also
    /// apply them to ensure correctness). Filters that cannot be converted are `Unsupported`.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|expr| {
                if expr_to_filter(expr).is_some() {
                    // Filter can be pushed - use Inexact since Python may not
                    // guarantee exact results (DataFusion will post-filter if needed)
                    Ok(TableProviderFilterPushDown::Inexact)
                } else {
                    // Cannot convert to Python filter
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect()
    }
}
