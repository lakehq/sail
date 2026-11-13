use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result, internal_err};
use datafusion_expr::{Expr, LogicalPlan, Projection, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// An ordered projection whose values cannot be inlined into its consumers.
/// Expressions remain visible to analysis, coercion, and subquery planning.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct EvaluateOnceNode {
    input: Arc<LogicalPlan>,
    expressions: Vec<Expr>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl EvaluateOnceNode {
    pub fn try_new(input: Arc<LogicalPlan>, expressions: Vec<Expr>) -> Result<Self> {
        let projection = Projection::try_new(expressions, input)?;
        Ok(Self {
            input: projection.input,
            expressions: projection.expr,
            schema: projection.schema,
        })
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn expr(&self) -> &[Expr] {
        &self.expressions
    }
}

impl UserDefinedLogicalNodeCore for EvaluateOnceNode {
    fn name(&self) -> &str {
        "EvaluateOnce"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        self.expressions.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "EvaluateOnce: {}",
            self.expressions
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if exprs.len() != self.expressions.len() {
            return internal_err!("EvaluateOnce expression count cannot change");
        }
        let input = inputs.one()?;
        if exprs == self.expressions && input.schema().fields() == self.input.schema().fields() {
            return Ok(Self {
                input: Arc::new(input),
                ..self.clone()
            });
        }
        Self::try_new(Arc::new(input), exprs)
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}

pub fn evaluate_once(input: LogicalPlan, expressions: Vec<Expr>) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Extension(datafusion_expr::Extension {
        node: Arc::new(EvaluateOnceNode::try_new(Arc::new(input), expressions)?),
    }))
}
