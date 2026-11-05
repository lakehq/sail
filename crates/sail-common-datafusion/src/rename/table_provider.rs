use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{Constraints, Result, Statistics, internal_err};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};

use crate::rename::expression::expression_before_rename;
use crate::rename::physical_plan::rename_projected_physical_plan;
use crate::rename::schema::rename_schema;

#[derive(Clone)]
pub struct RenameTableProvider {
    inner: Arc<dyn TableProvider>,
    /// The list of new column names for the table.
    names: Vec<String>,
    /// The schema for the renamed table.
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

    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[async_trait]
impl TableProvider for RenameTableProvider {
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
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        // The column default is only required for DataFusion SQL planning,
        // which is not used by us. So we do not need to implement this method.
        None
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner_schema = self.inner.schema();
        let filters = filters
            .iter()
            .map(|e| expression_before_rename(e, &self.names, &inner_schema, true))
            .collect::<Result<Vec<_>>>()?;
        let plan = self.inner.scan(state, projection, &filters, limit).await?;
        rename_projected_physical_plan(plan, &self.names, projection)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let inner_schema = self.inner.schema();
        let filters = filters
            .iter()
            .map(|e| expression_before_rename(e, &self.names, &inner_schema, true))
            .collect::<Result<Vec<_>>>()?;
        let filters = filters.iter().collect::<Vec<_>>();
        self.inner.supports_filters_pushdown(&filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("the renamed table is not supposed to be used for DML")
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::projection::ProjectionExec;

    use super::*;

    #[tokio::test]
    async fn scan_preserves_projection_order_and_values() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![Some(3), None])),
            ],
        )?;
        let inner = Arc::new(MemTable::try_new(schema, vec![vec![batch.clone()]])?);
        let provider = RenameTableProvider::try_new(
            inner.clone(),
            vec!["renamed_a".to_string(), "renamed_b".to_string()],
        )?;
        let context = SessionContext::new();
        let state = context.state();
        for projection in [None, Some(&[][..]), Some(&[1, 0][..])] {
            let plan = provider.scan(&state, projection, &[], None).await?;
            assert!(plan.is::<ProjectionExec>());
            let expected_schema = match projection {
                Some(indices) => Arc::new(provider.schema().project(indices)?),
                None => provider.schema(),
            };
            assert_eq!(plan.schema(), expected_schema);
            let reference = inner.scan(&state, projection, &[], None).await?;
            let reference = collect(reference, context.task_ctx()).await?;
            let result = collect(plan, context.task_ctx()).await?;
            assert_eq!(result.len(), 1);
            assert_eq!(reference.len(), 1);
            assert_eq!(result[0].num_rows(), batch.num_rows());
            assert_eq!(result[0].columns(), reference[0].columns());
        }
        Ok(())
    }
}
