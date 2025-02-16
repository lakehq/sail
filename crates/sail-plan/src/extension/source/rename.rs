use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{plan_datafusion_err, Column, Constraints, Result, Statistics};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use sail_common_datafusion::utils::{rename_physical_plan, rename_schema};

#[derive(Clone)]
pub(crate) struct RenameTableProvider {
    inner: Arc<dyn TableProvider>,
    /// A map from the new name to the old name.
    names: HashMap<String, String>,
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
        let names = names
            .iter()
            .zip(inner.schema().fields.iter())
            .map(|(n, f)| (n.clone(), f.name().clone()))
            .collect::<HashMap<_, _>>();
        Ok(Self {
            inner,
            names,
            schema,
        })
    }

    fn to_inner_expr(&self, expr: &Expr) -> Result<Expr> {
        let rewrite = |e: Expr| -> Result<Transformed<Expr>> {
            if let Expr::Column(Column {
                name,
                relation,
                spans,
            }) = e
            {
                let name = self
                    .names
                    .get(&name)
                    .ok_or_else(|| plan_datafusion_err!("column {name} not found"))?
                    .clone();
                Ok(Transformed::yes(Expr::Column(Column {
                    name,
                    relation,
                    spans,
                })))
            } else {
                Ok(Transformed::no(e))
            }
        };
        expr.clone().transform(rewrite).data()
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
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        None
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.names
            .get(column)
            .and_then(|column| self.inner.get_column_default(column))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filters = filters
            .iter()
            .map(|f| self.to_inner_expr(f))
            .collect::<Result<Vec<_>>>()?;
        let names = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let plan = self.inner.scan(state, projection, &filters, limit).await?;
        if let Some(projection) = projection {
            let names = projection
                .iter()
                .map(|i| names[*i].clone())
                .collect::<Vec<_>>();
            rename_physical_plan(plan, &names)
        } else {
            rename_physical_plan(plan, &names)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let filters = filters
            .iter()
            .map(|f| self.to_inner_expr(f))
            .collect::<Result<Vec<_>>>()?;
        let filters = filters.iter().collect::<Vec<_>>();
        self.inner.supports_filters_pushdown(&filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let names = self
            .inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let input = rename_physical_plan(input, &names)?;
        self.inner.insert_into(state, input, insert_op).await
    }
}
