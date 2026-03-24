use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

/// A logical plan node that filters a stream of retractable data batches.
///
/// Unlike a regular `Filter` node, this node is used in streaming plan rewriting
/// to avoid DataFusion optimizer rules (e.g. repartition insertion) that can make
/// bounded streaming queries unexpectedly slow.
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct StreamFilterNode {
    input: Arc<LogicalPlan>,
    predicate: Expr,
}

impl StreamFilterNode {
    pub fn new(input: Arc<LogicalPlan>, predicate: Expr) -> Self {
        Self { input, predicate }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn predicate(&self) -> &Expr {
        &self.predicate
    }
}

impl UserDefinedLogicalNodeCore for StreamFilterNode {
    fn name(&self) -> &str {
        "StreamFilter"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.predicate.clone()]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StreamFilter")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let predicate = exprs.one()?;
        let input = inputs.one()?;
        Ok(Self {
            input: Arc::new(input),
            predicate,
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
