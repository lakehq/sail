use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SortWithinPartitionsNode {
    input: Arc<LogicalPlan>,
    expr: Vec<Expr>,
    fetch: Option<usize>,
}

impl SortWithinPartitionsNode {
    pub fn new(input: Arc<LogicalPlan>, expr: Vec<Expr>, fetch: Option<usize>) -> Self {
        Self { input, expr, fetch }
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    pub fn expr(&self) -> &[Expr] {
        &self.expr
    }
}

impl UserDefinedLogicalNodeCore for SortWithinPartitionsNode {
    fn name(&self) -> &str {
        "SortWithinPartitions"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.expr.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SortWithinPartitions: ")?;
        for (i, e) in self.expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{e}")?;
        }
        if let Some(a) = self.fetch {
            write!(f, ", fetch={a}")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            input: Arc::new(inputs.one()?),
            expr: exprs,
            fetch: self.fetch,
        })
    }
}
