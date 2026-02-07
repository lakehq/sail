use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::expr::{Expr, Sort};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct SortWithinPartitionsNode {
    input: Arc<LogicalPlan>,
    sort_expr: Vec<Sort>,
    fetch: Option<usize>,
}

impl SortWithinPartitionsNode {
    pub fn new(input: Arc<LogicalPlan>, sort_expr: Vec<Sort>, fetch: Option<usize>) -> Self {
        Self {
            input,
            sort_expr,
            fetch,
        }
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    pub fn sort_expr(&self) -> &[Sort] {
        &self.sort_expr
    }
}

impl UserDefinedLogicalNodeCore for SortWithinPartitionsNode {
    fn name(&self) -> &str {
        "SortWithinPartitions"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        // Return the inner expression from each Sort so the optimizer can transform them
        self.sort_expr.iter().map(|s| s.expr.clone()).collect()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SortWithinPartitions: ")?;
        for (i, e) in self.sort_expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{e}")?;
        }
        if let Some(a) = self.fetch {
            write!(f, ", fetch={a}")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        // Validate that expressions length matches
        if exprs.len() != self.sort_expr.len() {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "SortWithinPartitionsNode: expected {} expressions, got {}",
                self.sort_expr.len(),
                exprs.len()
            )));
        }

        // Rebuild sort_expr using new expressions but keeping same asc/nulls_first
        let sort_expr = self
            .sort_expr
            .iter()
            .zip(exprs)
            .map(|(old_sort, new_expr)| Sort {
                expr: new_expr,
                asc: old_sort.asc,
                nulls_first: old_sort.nulls_first,
            })
            .collect();

        Ok(Self {
            input: Arc::new(inputs.one()?),
            sort_expr,
            fetch: self.fetch,
        })
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}
