use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_common::{plan_err, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::field::is_marker_field;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StreamSinkNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

#[derive(PartialEq, PartialOrd)]
struct StreamSinkNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
}

impl<'a> From<&'a StreamSinkNode> for StreamSinkNodeOrd<'a> {
    fn from(node: &'a StreamSinkNode) -> Self {
        Self { input: &node.input }
    }
}

impl PartialOrd for StreamSinkNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        StreamSinkNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl StreamSinkNode {
    pub fn try_new(input: Arc<LogicalPlan>) -> Result<Self> {
        let mut iter = input.schema().fields().iter();
        if !iter.next().is_some_and(|x| is_marker_field(x.as_ref())) {
            return plan_err!("missing marker field in stream sink input");
        }
        let schema = DFSchema::try_from(Schema::new(iter.cloned().collect::<Vec<_>>()))?;
        Ok(Self {
            input,
            schema: Arc::new(schema),
        })
    }
}

impl UserDefinedLogicalNodeCore for StreamSinkNode {
    fn name(&self) -> &str {
        "StreamSink"
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
        write!(f, "StreamSink")
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
