use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;

/// A logical plan node that adapts a non-streaming data source
/// to a streaming source so that it can be used in a streaming query.
/// The wrapped source contains retraction flag for each row,
/// and emits data flow markers.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StreamSourceAdapterNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

#[derive(PartialEq, PartialOrd)]
struct StreamSourceAdapterNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
}

impl<'a> From<&'a StreamSourceAdapterNode> for StreamSourceAdapterNodeOrd<'a> {
    fn from(node: &'a StreamSourceAdapterNode) -> Self {
        Self { input: &node.input }
    }
}

impl PartialOrd for StreamSourceAdapterNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        StreamSourceAdapterNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl StreamSourceAdapterNode {
    pub fn try_new(input: Arc<LogicalPlan>) -> Result<Self> {
        let schema = DFSchema::try_from(to_flow_event_schema(input.schema().inner()))?;
        Ok(Self {
            input,
            schema: Arc::new(schema),
        })
    }
}

impl UserDefinedLogicalNodeCore for StreamSourceAdapterNode {
    fn name(&self) -> &str {
        "StreamSourceAdapter"
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
        write!(f, "StreamSourceAdapter")
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
