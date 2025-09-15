use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};

/// A logical plan node that limits the number of retractable rows in a stream
/// while passing through all data flow markers.
/// This turns an unbounded stream query into a bounded one.
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct StreamLimitNode {
    input: Arc<LogicalPlan>,
    skip: usize,
    fetch: Option<usize>,
}

impl StreamLimitNode {
    pub fn new(input: Arc<LogicalPlan>, skip: usize, fetch: Option<usize>) -> Self {
        Self { input, skip, fetch }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn skip(&self) -> usize {
        self.skip
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl UserDefinedLogicalNodeCore for StreamLimitNode {
    fn name(&self) -> &str {
        "StreamLimit"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StreamLimit")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{} does not take any expressions", self.name());
        }
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return plan_err!("{} expects exactly one input", self.name());
        };
        Ok(Self {
            input: Arc::new(input),
            skip: self.skip,
            fetch: self.fetch,
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
