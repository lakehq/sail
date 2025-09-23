use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::datasource::{BucketBy, SinkMode};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct FileWriteOptions {
    pub path: String,
    pub format: String,
    pub mode: SinkMode,
    pub partition_by: Vec<String>,
    pub sort_by: Vec<Sort>,
    pub bucket_by: Option<BucketBy>,
    pub options: Vec<Vec<(String, String)>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FileWriteNode {
    input: Arc<LogicalPlan>,
    options: FileWriteOptions,
    schema: DFSchemaRef,
}

impl FileWriteNode {
    pub fn new(input: Arc<LogicalPlan>, options: FileWriteOptions) -> Self {
        Self {
            input,
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &FileWriteOptions {
        &self.options
    }
}

#[derive(PartialEq, PartialOrd)]
struct FileWriteNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
    options: &'a FileWriteOptions,
}

impl<'a> From<&'a FileWriteNode> for FileWriteNodeOrd<'a> {
    fn from(node: &'a FileWriteNode) -> Self {
        Self {
            input: &node.input,
            options: &node.options,
        }
    }
}

impl PartialOrd for FileWriteNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        FileWriteNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl UserDefinedLogicalNodeCore for FileWriteNode {
    fn name(&self) -> &str {
        "FileWrite"
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
        write!(f, "FileWrite: options={:?}", self.options)?;
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
            options: self.options.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
