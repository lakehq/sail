use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct FileDeleteOptions {
    pub table_name: Vec<String>,
    pub path: String,
    pub format: String,
    pub condition: Option<ExprWithSource>,
    pub options: Vec<Vec<(String, String)>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FileDeleteNode {
    options: FileDeleteOptions,
    schema: DFSchemaRef,
}

impl FileDeleteNode {
    pub fn new(options: FileDeleteOptions) -> Self {
        Self {
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &FileDeleteOptions {
        &self.options
    }
}

#[derive(PartialEq, PartialOrd)]
struct FileDeleteNodeOrd<'a> {
    options: &'a FileDeleteOptions,
}

impl<'a> From<&'a FileDeleteNode> for FileDeleteNodeOrd<'a> {
    fn from(node: &'a FileDeleteNode) -> Self {
        Self {
            options: &node.options,
        }
    }
}

impl PartialOrd for FileDeleteNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        FileDeleteNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl UserDefinedLogicalNodeCore for FileDeleteNode {
    fn name(&self) -> &str {
        "FileDelete"
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
        write!(f, "FileDelete: options={:?}", self.options)?;
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
