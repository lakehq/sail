use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common::utils::object::{arc_ptr_eq, arc_ptr_hash, arc_ptr_partial_cmp};
use sail_common_datafusion::catalog::CatalogPartitionField;
use sail_common_datafusion::utils::items::ItemTaker;
use url::Url;

use crate::listing::source::WriteFormat;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct FileWriteOptions {
    #[educe(
        PartialEq(method(arc_ptr_eq)),
        Hash(method(arc_ptr_hash)),
        PartialOrd(method(arc_ptr_partial_cmp))
    )]
    pub format: Arc<dyn WriteFormat>,
    pub url: Url,
    pub overwrite: bool,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_by: Vec<Sort>,
}

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash, PartialOrd)]
pub struct FileWriteNode {
    input: Arc<LogicalPlan>,
    options: FileWriteOptions,
    #[educe(PartialOrd(ignore))]
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
        self.options
            .sort_by
            .iter()
            .map(|sort| sort.expr.clone())
            .collect()
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
        if exprs.len() != self.options.sort_by.len() {
            return datafusion_common::plan_err!(
                "FileWrite expects {} sort expressions, got {}",
                self.options.sort_by.len(),
                exprs.len()
            );
        }
        let mut options = self.options.clone();
        for (sort, expr) in options.sort_by.iter_mut().zip(exprs) {
            sort.expr = expr;
        }
        Ok(Self {
            input: Arc::new(inputs.one()?),
            options,
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
