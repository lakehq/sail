use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::lakeprocedure::{LakeProcedureCall, LakeProcedureRootPlacement};
use sail_common_datafusion::utils::items::ItemTaker;

/// Engine-owned semantic boundary around a format-planned procedure implementation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct LakeProcedureNode {
    name: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
    call: LakeProcedureCall,
    root_placement: LakeProcedureRootPlacement,
    input: Arc<LogicalPlan>,
}

impl LakeProcedureNode {
    pub fn try_new(
        call: LakeProcedureCall,
        input: LogicalPlan,
        root_placement: LakeProcedureRootPlacement,
    ) -> Result<Self> {
        call.validate()?;
        let schema = DFSchemaRef::new(DFSchema::try_from(call.invocation.procedure.schema())?);
        if input.schema() != &schema {
            return plan_err!("lake procedure implementation schema does not match its descriptor");
        }
        Ok(Self {
            name: format!(
                "LakeProcedure: {}.{}",
                call.lake_source, call.invocation.procedure.name
            ),
            schema,
            call,
            root_placement,
            input: Arc::new(input),
        })
    }

    pub fn call(&self) -> &LakeProcedureCall {
        &self.call
    }

    pub fn root_placement(&self) -> LakeProcedureRootPlacement {
        self.root_placement
    }
}

impl UserDefinedLogicalNodeCore for LakeProcedureNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = inputs.one()?;
        Self::try_new(self.call.clone(), input, self.root_placement)
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
