use std::fmt::Formatter;

use datafusion::common::{DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::lakeprocedure::LakeProcedureCall;

use super::table::ProcedureTable;

/// A bound Iceberg system procedure call.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub(crate) struct IcebergProcedureNode {
    call: LakeProcedureCall,
    planned_table: Option<ProcedureTable>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl IcebergProcedureNode {
    pub(super) fn try_new(
        call: LakeProcedureCall,
        planned_table: Option<ProcedureTable>,
    ) -> Result<Self> {
        call.validate()?;
        let schema = DFSchemaRef::new(DFSchema::try_from(call.invocation.procedure.schema())?);
        Ok(Self {
            call,
            planned_table,
            schema,
        })
    }

    pub(crate) fn call(&self) -> &LakeProcedureCall {
        &self.call
    }

    pub(crate) fn planned_table(&self) -> Option<&ProcedureTable> {
        self.planned_table.as_ref()
    }
}

impl UserDefinedLogicalNodeCore for IcebergProcedureNode {
    fn name(&self) -> &str {
        "IcebergProcedure"
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
        write!(
            f,
            "IcebergProcedure: procedure={}, invocation_id={}",
            self.call.invocation.procedure.name, self.call.invocation_id.0
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return plan_err!("IcebergProcedure does not accept expressions or inputs");
        }
        Ok(self.clone())
    }
}
