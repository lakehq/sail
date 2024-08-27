use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
};

pub struct TemporaryTableProvider {
    plan: Arc<LogicalPlan>,
}

impl TemporaryTableProvider {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

#[async_trait]
impl TableProvider for TemporaryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema: Schema = self.plan.schema().as_ref().into();
        Arc::new(schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        Some(&self.plan)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut expr = LogicalPlanBuilder::from(self.plan.as_ref().clone());
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(filter) = filter {
            expr = expr.filter(filter)?
        }
        if let Some(p) = projection {
            expr = expr.select(p.iter().copied())?
        }
        if let Some(l) = limit {
            expr = expr.limit(0, Some(l))?
        }
        let plan = expr.build()?;
        state.create_physical_plan(&plan).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}
