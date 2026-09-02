use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::lakeprocedure::LakeProcedureCall;

use super::rewrite_data_files::RewriteDataFilesPlan;
use super::table::ProcedureTable;

/// A bound Iceberg system procedure call.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub(crate) struct IcebergProcedureNode {
    call: LakeProcedureCall,
    planned_table: Option<ProcedureTable>,
    input: Option<Arc<LogicalPlan>>,
    rewrite_data_files: Option<RewriteDataFilesPlan>,
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
            input: None,
            rewrite_data_files: None,
            schema,
        })
    }

    pub(super) fn try_new_rewrite_data_files(
        call: LakeProcedureCall,
        input: LogicalPlan,
        rewrite_data_files: RewriteDataFilesPlan,
    ) -> Result<Self> {
        call.validate()?;
        let schema = DFSchemaRef::new(DFSchema::try_from(call.invocation.procedure.schema())?);
        Ok(Self {
            call,
            planned_table: None,
            input: Some(Arc::new(input)),
            rewrite_data_files: Some(rewrite_data_files),
            schema,
        })
    }

    pub(crate) fn call(&self) -> &LakeProcedureCall {
        &self.call
    }

    pub(crate) fn planned_table(&self) -> Option<&ProcedureTable> {
        self.planned_table.as_ref()
    }

    pub(crate) fn rewrite_data_files(&self) -> Option<&RewriteDataFilesPlan> {
        self.rewrite_data_files.as_ref()
    }
}

impl UserDefinedLogicalNodeCore for IcebergProcedureNode {
    fn name(&self) -> &str {
        "IcebergProcedure"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.input.iter().map(AsRef::as_ref).collect()
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
        if !exprs.is_empty() {
            return plan_err!("IcebergProcedure does not accept expressions");
        }
        let input = match (&self.input, inputs.as_slice()) {
            (None, []) => None,
            (Some(_), [input]) => Some(Arc::new(input.clone())),
            _ => return plan_err!("IcebergProcedure input count does not match its operation"),
        };
        Ok(Self {
            call: self.call.clone(),
            planned_table: self.planned_table.clone(),
            input,
            rewrite_data_files: self.rewrite_data_files.clone(),
            schema: self.schema.clone(),
        })
    }
}
