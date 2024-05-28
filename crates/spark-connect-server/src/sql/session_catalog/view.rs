use crate::sql::session_catalog::SessionCatalogContext;
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{DdlStatement, DropView, LogicalPlan};

impl SessionCatalogContext<'_> {
    pub(crate) async fn drop_temporary_view(&self, view_name: &str) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::DropView(DropView {
            name: TableReference::Bare {
                table: view_name.to_string().into(),
            },
            if_exists: false,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }
}
