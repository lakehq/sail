use crate::catalog::CatalogContext;
use crate::SqlEngine;
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{DdlStatement, DropView, LogicalPlan};

impl<S: SqlEngine> CatalogContext<'_, S> {
    pub(crate) async fn drop_view(&self, view: TableReference, if_exists: bool) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::DropView(DropView {
            name: view,
            if_exists,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }
}
