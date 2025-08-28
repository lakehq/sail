use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_common::{plan_err, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::field::marker_field;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StreamSourceNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

#[derive(PartialEq, PartialOrd)]
struct StreamSourceNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
}

impl<'a> From<&'a StreamSourceNode> for StreamSourceNodeOrd<'a> {
    fn from(node: &'a StreamSourceNode) -> Self {
        Self { input: &node.input }
    }
}

impl PartialOrd for StreamSourceNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        StreamSourceNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl StreamSourceNode {
    pub fn try_new(input: Arc<LogicalPlan>) -> Result<Self> {
        let schema = DFSchema::try_from(Schema::new(vec![marker_field()]))?.join(input.schema())?;
        Ok(Self {
            input,
            schema: Arc::new(schema),
        })
    }
}

impl UserDefinedLogicalNodeCore for StreamSourceNode {
    fn name(&self) -> &str {
        "StreamSource"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StreamSource")
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
        Self::try_new(Arc::new(input))
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
