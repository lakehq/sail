use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

/// Marks the input and output boundaries of a partition-dependent filter.
/// Between the boundaries, a standard Filter allows subquery rewriting while
/// Sail's filter pushdown rule preserves predicate placement and evaluation order.
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct PartitionFilterNode {
    input: Arc<LogicalPlan>,
}

impl PartitionFilterNode {
    pub fn new(input: Arc<LogicalPlan>) -> Self {
        Self { input }
    }
}

impl UserDefinedLogicalNodeCore for PartitionFilterNode {
    fn name(&self) -> &str {
        "PartitionFilter"
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
        write!(f, "PartitionFilter")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Ok(Self::new(Arc::new(inputs.one()?)))
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
