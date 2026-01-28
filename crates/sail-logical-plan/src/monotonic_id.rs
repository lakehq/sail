use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Clone, Debug, Eq, Hash)]
pub struct MonotonicIdNode {
    input: Arc<LogicalPlan>,
    column_name: String,
    schema: DFSchemaRef,
}

impl MonotonicIdNode {
    pub fn try_new(input: Arc<LogicalPlan>, column_name: String) -> Result<Self> {
        let mut fields = input.schema().as_ref().as_arrow().fields().to_vec();
        fields.push(Arc::new(Field::new(
            column_name.clone(),
            DataType::Int64,
            false,
        )));
        let arrow_schema = ArrowSchema::new(fields);
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            arrow_schema.fields().to_vec().into(),
            HashMap::new(),
        )?);
        Ok(Self {
            input,
            column_name,
            schema,
        })
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl PartialEq for MonotonicIdNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.column_name == other.column_name
            && self.schema == other.schema
    }
}

#[derive(PartialEq, PartialOrd)]
struct MonotonicIdNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
    column_name: &'a String,
}

impl<'a> From<&'a MonotonicIdNode> for MonotonicIdNodeOrd<'a> {
    fn from(node: &'a MonotonicIdNode) -> Self {
        Self {
            input: &node.input,
            column_name: &node.column_name,
        }
    }
}

impl PartialOrd for MonotonicIdNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        MonotonicIdNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl UserDefinedLogicalNodeCore for MonotonicIdNode {
    fn name(&self) -> &str {
        "MonotonicId"
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
        write!(f, "MonotonicId")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        Ok(Self::try_new(input, self.column_name.clone())?)
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
