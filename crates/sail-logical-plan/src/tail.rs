use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

/// The last rows of the input, which is evaluated once (see Spark's `Tail`).
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct TailNode {
    input: Arc<LogicalPlan>,
    fetch: usize,
    /// The input columns that determine the input order. They are kept in the input
    /// even if they are not used by the output, so that the order stays visible.
    keys: Vec<Expr>,
}

impl TailNode {
    pub fn new(input: Arc<LogicalPlan>, fetch: usize) -> Self {
        Self {
            input,
            fetch,
            keys: vec![],
        }
    }

    pub fn with_keys(self, keys: Vec<Expr>) -> Self {
        Self { keys, ..self }
    }

    pub fn fetch(&self) -> usize {
        self.fetch
    }
}

impl UserDefinedLogicalNodeCore for TailNode {
    fn name(&self) -> &str {
        "Tail"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.keys.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Tail: fetch={}", self.fetch)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            input: Arc::new(inputs.one()?),
            fetch: self.fetch,
            keys: exprs,
        })
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}
