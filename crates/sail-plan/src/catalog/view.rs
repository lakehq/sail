use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{CreateView, DdlStatement, DropView, LogicalPlan};

use crate::catalog::CatalogManager;
use crate::temp_view::manage_temporary_views;

impl CatalogManager<'_> {
    pub(crate) async fn drop_temporary_view(
        &self,
        view_name: &str,
        is_global: bool,
        if_exists: bool,
    ) -> Result<()> {
        manage_temporary_views(self.ctx, is_global, |views| {
            views.remove_view(view_name, if_exists)?;
            Ok(())
        })
    }

    pub(crate) async fn drop_view(&self, view: TableReference, if_exists: bool) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::DropView(DropView {
            name: view,
            if_exists,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }

    pub(crate) async fn create_temporary_view(
        &self,
        input: Arc<LogicalPlan>,
        view_name: &str,
        is_global: bool,
        replace: bool,
    ) -> Result<()> {
        manage_temporary_views(self.ctx, is_global, |views| {
            views.add_view(view_name.to_string(), input, replace)?;
            Ok(())
        })
    }

    pub(crate) async fn create_view(
        &self,
        input: Arc<LogicalPlan>,
        view: TableReference,
        replace: bool,
        definition: Option<String>,
        temporary: bool,
    ) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: view,
            input,
            or_replace: replace,
            definition,
            temporary,
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }
}
