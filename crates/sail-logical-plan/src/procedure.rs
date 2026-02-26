use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct ProcedureOptions {
    pub format: String,
    pub procedure_name: Vec<String>,
    pub positional_arguments: Vec<String>,
    pub named_arguments: Vec<(String, String)>,
    pub target_table: Option<Vec<String>>,
    pub target_path: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ProcedureNode {
    options: ProcedureOptions,
    schema: DFSchemaRef,
}

impl ProcedureNode {
    pub fn new(options: ProcedureOptions) -> Self {
        Self {
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &ProcedureOptions {
        &self.options
    }
}

#[derive(PartialEq, PartialOrd)]
struct ProcedureNodeOrd<'a> {
    options: &'a ProcedureOptions,
}

impl<'a> From<&'a ProcedureNode> for ProcedureNodeOrd<'a> {
    fn from(node: &'a ProcedureNode) -> Self {
        Self {
            options: &node.options,
        }
    }
}

impl PartialOrd for ProcedureNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        ProcedureNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl UserDefinedLogicalNodeCore for ProcedureNode {
    fn name(&self) -> &str {
        "Procedure"
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
        write!(f, "Procedure: options={:?}", self.options)?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(Self {
            options: self.options.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}
