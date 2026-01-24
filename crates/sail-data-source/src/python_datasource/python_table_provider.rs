use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;

/// TableProvider implementation for Python DataSources.
///
/// This integrates Python DataSources with DataFusion's catalog and execution system.
use super::exec::PythonDataSourceExec;
use super::python_datasource::PythonDataSource;

/// TableProvider for Python-defined DataSources.
///
/// This allows Python DataSources to be registered in DataFusion's catalog
/// and used in SQL queries.
#[derive(Debug)]
pub struct PythonTableProvider {
    /// The Python DataSource
    datasource: Arc<PythonDataSource>,
    /// Cached schema
    schema: SchemaRef,
}

impl PythonTableProvider {
    /// Create a new PythonTableProvider.
    ///
    /// # Arguments
    /// * `datasource` - The Python DataSource
    /// * `schema` - The schema of the data
    pub fn new(datasource: Arc<PythonDataSource>, schema: SchemaRef) -> Self {
        Self { datasource, schema }
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
    /// * `projection` - Optional column projection (TODO: apply in future PR)
    /// * `filters` - Filter expressions (TODO: pushdown in future PR)
    /// * `limit` - Optional limit on number of rows (TODO: pushdown in future PR)
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

        // Get partitions from Python
        // Note: get_partitions needs schema to create the reader
        let partitions = self.datasource.get_partitions(&self.schema)?;

        // Create execution plan from Python datasource (always yields full schema)
        let exec = PythonDataSourceExec::new(
            self.datasource.command().to_vec(),
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
