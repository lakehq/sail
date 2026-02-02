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
    /// # Arguments
    /// * `state` - Session state
    /// * `projection` - Optional column projection (applied via ProjectionExec wrapper)
    /// * `filters` - Filter expressions (Phase 2: pushdown to Python)
    /// * `limit` - Optional limit on number of rows (Phase 2: pushdown to Python)
    ///
    /// # Projection Handling
    ///
    /// Currently, projection is applied post-read via `ProjectionExec`. This means
    /// Python reads all columns and DataFusion filters them. Phase 2 will implement
    /// true projection pushdown by passing projected schema to `reader(schema)`,
    /// which requires Python DataSources to respect the schema parameter.
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // For MVP: we don't push down filters to Python yet
        // This will be enhanced in future PRs
        let (_pushed_filters, _unpushed_filters) = self.classify_filters(filters);

        // Get partitions from Python via executor
        let partitions = self
            .executor
            .get_partitions(&self.command, &self.schema)
            .await?;

        // Create execution plan with executor reference
        let exec = PythonDataSourceExec::new(
            self.executor.clone(),
            self.command.clone(),
            self.schema.clone(),
            partitions,
        );
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
    /// For MVP, no filters are pushed down yet. This will be enhanced in future PRs.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // For MVP: indicate that filters are not pushed down
        // They will be applied by DataFusion after reading
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

impl PythonTableProvider {
    /// Classify filters into those that can be pushed down vs those that cannot.
    ///
    /// For MVP, no filters are pushed down. Future PRs will implement filter pushdown.
    fn classify_filters(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        let pushed = vec![];
        let unpushed = filters.to_vec();
        (pushed, unpushed)
    }
}
