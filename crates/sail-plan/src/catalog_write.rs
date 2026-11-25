use std::sync::Arc;

use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, DFSchemaRef, Result, plan_err};
use educe::Educe;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CreateTableOptions;
use sail_common_datafusion::datasource::{DataSourceRegistry, OptionLayer, SinkInfo, SinkMode};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::catalog::CatalogCommandNode;

/// A catalog-owned create must provide the location and access before writer planning.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct CatalogCreateWriteNode {
    table: Vec<String>,
    options: CreateTableOptions,
    format: String,
    sink: SinkInfo,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl CatalogCreateWriteNode {
    pub(crate) fn try_new(
        preconditions: Vec<Arc<LogicalPlan>>,
        format: String,
        sink: SinkInfo,
    ) -> Result<Self> {
        let precondition = preconditions.one()?;
        let LogicalPlan::Extension(extension) = precondition.as_ref() else {
            return plan_err!("Catalog create-and-write requires a create command");
        };
        let Some(command) = extension.node.as_any().downcast_ref::<CatalogCommandNode>() else {
            return plan_err!("Catalog create-and-write requires a create command");
        };
        let CatalogCommand::CreateTable { table, options } = command.command() else {
            return plan_err!("Catalog create-and-write requires a create command");
        };
        Ok(Self {
            table: table.clone(),
            options: options.clone(),
            format,
            sink,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    pub(crate) async fn prepare(
        &self,
        ctx: &SessionContext,
    ) -> crate::error::PlanResult<LogicalPlan> {
        let source = ctx
            .extension::<DataSourceRegistry>()?
            .get_data_source(&self.format)?;
        source
            .create_writer(&ctx.state(), self.sink.clone())
            .await?;
        let resolved = ctx
            .extension::<CatalogManager>()?
            .create_table_for_write(&self.table, self.options.clone())
            .await?;
        let location = resolved.status.location.ok_or_else(|| {
            datafusion_common::plan_datafusion_err!("Catalog create returned no storage location")
        })?;
        let mut sink = self.sink.clone();
        sink.mode = SinkMode::Append;
        sink.options.push(OptionLayer::TablePropertyList {
            items: resolved.status.display_properties,
        });
        sink.options.push(OptionLayer::OptionList {
            items: vec![("path".to_string(), location)],
        });
        sink.lakehouse_table = Some(resolved.execution);
        Ok(source.create_writer(&ctx.state(), sink).await?)
    }
}

impl UserDefinedLogicalNodeCore for CatalogCreateWriteNode {
    fn name(&self) -> &str {
        "CatalogCreateWrite"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.sink.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CatalogCreateWrite: {}", self.table.join("."))
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let mut node = self.clone();
        node.sink.input = inputs.one()?;
        Ok(node)
    }
}
