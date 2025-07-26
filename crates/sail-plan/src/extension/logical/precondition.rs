use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{plan_err, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct WithPreconditionsNode {
    preconditions: Vec<Arc<LogicalPlan>>,
    plan: Arc<LogicalPlan>,
}

impl WithPreconditionsNode {
    pub fn new(preconditions: Vec<Arc<LogicalPlan>>, plan: Arc<LogicalPlan>) -> Self {
        Self {
            preconditions,
            plan,
        }
    }

    pub fn preconditions(&self) -> &[Arc<LogicalPlan>] {
        &self.preconditions
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
}

impl UserDefinedLogicalNodeCore for WithPreconditionsNode {
    fn name(&self) -> &str {
        "WithPreconditions"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.preconditions
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
        write!(f, "{}", self.name())?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        let Some(plan) = inputs.pop() else {
            return plan_err!("{} requires at least one input", self.name());
        };
        Ok(Self {
            preconditions: inputs.into_iter().map(Arc::new).collect(),
            plan: Arc::new(plan),
        })
    }
}
