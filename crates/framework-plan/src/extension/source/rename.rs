use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{Constraints, Result, Statistics};
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use framework_common::utils::{rename_physical_plan, rename_schema};

#[derive(Clone)]
pub(crate) struct RenameTableProvider {
    inner: Arc<dyn TableProvider>,
    names: Vec<String>,
    schema: SchemaRef,
}

impl Debug for RenameTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenameTableProvider")
            .field("names", &self.names)
            .finish()
    }
}

impl RenameTableProvider {
    pub fn try_new(inner: Arc<dyn TableProvider>, names: Vec<String>) -> Result<Self> {
        let schema = rename_schema(&inner.schema(), &names)?;
        Ok(Self {
            inner,
            names,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for RenameTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        // FIXME: rewrite column reference
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // FIXME: rewrite filter column reference
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        if let Some(projection) = projection {
            let names = projection
                .iter()
                .map(|i| self.names[*i].clone())
                .collect::<Vec<_>>();
            rename_physical_plan(plan, &names)
        } else {
            rename_physical_plan(plan, &self.names)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // FIXME: rewrite filter column reference
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // FIXME: "scan" and "insert" names should be different
        let input = rename_physical_plan(input, &self.names)?;
        self.inner.insert_into(state, input, overwrite).await
    }
}
