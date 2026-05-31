use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::SinkMode;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct PythonWriteNode {
    input: Arc<LogicalPlan>,
    pickled_writer: Vec<u8>,
    is_arrow: bool,
    mode: SinkMode,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl PythonWriteNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        pickled_writer: Vec<u8>,
        is_arrow: bool,
        mode: SinkMode,
    ) -> Self {
        Self {
            input,
            pickled_writer,
            is_arrow,
            mode,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn pickled_writer(&self) -> &[u8] {
        &self.pickled_writer
    }

    pub fn is_arrow(&self) -> bool {
        self.is_arrow
    }

    pub fn mode(&self) -> &SinkMode {
        &self.mode
    }
}

impl UserDefinedLogicalNodeCore for PythonWriteNode {
    fn name(&self) -> &str {
        "PythonWrite"
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
        write!(
            f,
            "PythonWrite: mode={:?}, writer_type={}",
            self.mode,
            if self.is_arrow { "Arrow" } else { "Row" }
        )?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            pickled_writer: self.pickled_writer.clone(),
            is_arrow: self.is_arrow,
            mode: self.mode.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
