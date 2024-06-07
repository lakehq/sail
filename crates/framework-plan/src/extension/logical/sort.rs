use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SortWithinPartitionsNode {
    pub input: Arc<LogicalPlan>,
    pub expr: Vec<Expr>,
    pub fetch: Option<usize>,
}

impl SortWithinPartitionsNode {
    pub fn new(input: Arc<LogicalPlan>, expr: Vec<Expr>, fetch: Option<usize>) -> Self {
        Self { input, expr, fetch }
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

    fn from_template(&self, exprs: &[Expr], input: &[LogicalPlan]) -> Self {
        assert_eq!(input.len(), 1);
        Self {
            input: Arc::new(input[0].clone()),
            expr: exprs.to_vec(),
            fetch: self.fetch,
        }
    }
}
