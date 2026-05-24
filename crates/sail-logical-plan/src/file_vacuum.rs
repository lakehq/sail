use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::datasource::OptionLayer;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct FileVacuumOptions {
    pub table_name: Vec<String>,
    pub path: String,
    pub format: String,
    pub retention_hours: Option<u64>,
    pub dry_run: bool,
    pub options: Vec<OptionLayer>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct FileVacuumNode {
    options: FileVacuumOptions,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl FileVacuumNode {
    pub fn new(options: FileVacuumOptions) -> Self {
        Self {
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &FileVacuumOptions {
        &self.options
    }
}

impl UserDefinedLogicalNodeCore for FileVacuumNode {
    fn name(&self) -> &str {
        "FileVacuum"
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
        write!(f, "FileVacuum: options={:?}", self.options)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        Ok(self.clone())
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}
