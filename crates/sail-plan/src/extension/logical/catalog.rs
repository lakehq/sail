use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionContext;
use datafusion_common::DFSchema;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;

use crate::config::PlanConfig;
use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct CatalogCommandNode {
    name: String,
    schema: DFSchemaRef,
    command: CatalogCommand,
    config: Arc<PlanConfig>,
}

#[derive(PartialEq, PartialOrd)]
struct CatalogCommandNodeOrd<'a> {
    name: &'a String,
    command: &'a CatalogCommand,
    config: &'a Arc<PlanConfig>,
}

impl<'a> From<&'a CatalogCommandNode> for CatalogCommandNodeOrd<'a> {
    fn from(node: &'a CatalogCommandNode) -> Self {
        Self {
            name: &node.name,
            command: &node.command,
            config: &node.config,
        }
    }
}

impl PartialOrd for CatalogCommandNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        CatalogCommandNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl CatalogCommandNode {
    pub(crate) fn try_new(command: CatalogCommand, config: Arc<PlanConfig>) -> Result<Self> {
        let schema = command.schema()?;
        Ok(Self {
            name: format!("CatalogCommand: {}", command.name()),
            schema: DFSchemaRef::new(DFSchema::try_from(schema)?),
            command,
            config,
        })
    }
}

impl CatalogCommandNode {
    pub(crate) async fn execute(&self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let manager = CatalogManager::new(ctx, self.config.clone());
        self.command.clone().execute(manager).await
    }
}

impl UserDefinedLogicalNodeCore for CatalogCommandNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}
