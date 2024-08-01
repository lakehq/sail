use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{CreateView, DdlStatement, DropView, LogicalPlan};

use crate::catalog::CatalogManager;

impl<'a> CatalogManager<'a> {
    pub(crate) async fn drop_view(&self, view: TableReference, if_exists: bool) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::DropView(DropView {
            name: view,
            if_exists,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }

    pub(crate) async fn create_view(
        &self,
        input: Arc<LogicalPlan>,
        view: TableReference,
        is_global: bool,
        replace: bool,
        definition: Option<String>,
    ) -> Result<()> {
        // TODO: Support global views
        let _ = is_global;
        let ddl = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: view,
            input,
            or_replace: replace,
            definition,
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }
}
