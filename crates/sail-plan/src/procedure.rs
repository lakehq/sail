use std::fmt::Formatter;

use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::lakeprocedure::LakeProcedureCall;
use sail_common_datafusion::utils::items::ItemTaker;

/// Logical command node for a fully bound lakehouse procedure call.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct LakeProcedureNode {
    name: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
    call: LakeProcedureCall,
}

impl LakeProcedureNode {
    pub fn try_new(call: LakeProcedureCall) -> Result<Self> {
        call.validate()?;
        let schema = DFSchemaRef::new(DFSchema::try_from(call.invocation.procedure.schema())?);
        Ok(Self {
            name: format!(
                "LakeProcedure: {}.{}",
                call.format_id, call.invocation.procedure.name
            ),
            schema,
            call,
        })
    }

    pub fn call(&self) -> &LakeProcedureCall {
        &self.call
    }
}

impl UserDefinedLogicalNodeCore for LakeProcedureNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
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
        inputs.zero()?;
        Ok(self.clone())
    }
}
