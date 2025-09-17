use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;

/// A logical plan node that collects a stream of retractable data batches
/// into final data batches.
/// This is a special "streaming sink" that allows returning query results
/// for streaming queries, with the requirement that the query is bounded.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StreamCollectorNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

#[derive(PartialEq, PartialOrd)]
struct StreamCollectorNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
}

impl<'a> From<&'a StreamCollectorNode> for StreamCollectorNodeOrd<'a> {
    fn from(node: &'a StreamCollectorNode) -> Self {
        Self { input: &node.input }
    }
}

impl PartialOrd for StreamCollectorNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        StreamCollectorNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl StreamCollectorNode {
    pub fn try_new(input: Arc<LogicalPlan>) -> Result<Self> {
        let schema = DFSchema::try_from(try_from_flow_event_schema(input.schema().inner())?)?;
        Ok(Self {
            input,
            schema: Arc::new(schema),
        })
    }
}

impl UserDefinedLogicalNodeCore for StreamCollectorNode {
    fn name(&self) -> &str {
        "StreamCollector"
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
        write!(f, "StreamCollector")
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
