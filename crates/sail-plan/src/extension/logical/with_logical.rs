use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{plan_err, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct WithLogicalExecutionNode {
    setup: Vec<Arc<LogicalPlan>>,
    plan: Arc<LogicalPlan>,
}

#[allow(dead_code)]
impl WithLogicalExecutionNode {
    pub fn new(setup: Vec<Arc<LogicalPlan>>, plan: Arc<LogicalPlan>) -> Self {
        Self { setup, plan }
    }

    pub fn setup(&self) -> &[Arc<LogicalPlan>] {
        &self.setup
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
}

impl UserDefinedLogicalNodeCore for WithLogicalExecutionNode {
    fn name(&self) -> &str {
        "WithLogicalExecution"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.setup
            .iter()
            .map(|x| x.as_ref())
            .chain(std::iter::once(self.plan.as_ref()))
            .collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        self.plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WithLogicalExecution")?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        let Some(plan) = inputs.pop() else {
            return plan_err!("WithLogicalExecution requires at least one input");
        };
        Ok(Self {
            setup: inputs.into_iter().map(Arc::new).collect(),
            plan: Arc::new(plan),
        })
    }
}
