use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::expr::{Expr, Sort};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub(crate) struct SortWithinPartitionsNode {
    input: Arc<LogicalPlan>,
    sort_expr: Vec<Sort>,
    fetch: Option<usize>,
}

impl SortWithinPartitionsNode {
    pub fn new(input: Arc<LogicalPlan>, sort_expr: Vec<Sort>, fetch: Option<usize>) -> Self {
        Self {
            input,
            sort_expr,
            fetch,
        }
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    pub fn sort_expr(&self) -> &[Sort] {
        &self.sort_expr
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
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SortWithinPartitions: ")?;
        for (i, e) in self.sort_expr.iter().enumerate() {
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
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            sort_expr: self.sort_expr.clone(),
            fetch: self.fetch,
        })
    }
}
