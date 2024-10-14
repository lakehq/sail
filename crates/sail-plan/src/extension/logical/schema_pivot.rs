use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{plan_err, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::utils::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SchemaPivotNode {
    input: Arc<LogicalPlan>,
    names: Vec<String>,
    schema: DFSchemaRef,
    exprs: Vec<Expr>,
}

#[derive(PartialEq, PartialOrd)]
struct SchemaPivotNodeOrd<'a> {
    // names is part of schema so we skip that
    input: &'a Arc<LogicalPlan>,
    exprs: &'a Vec<Expr>,
}

impl<'a> From<&'a SchemaPivotNode> for SchemaPivotNodeOrd<'a> {
    fn from(node: &'a SchemaPivotNode) -> Self {
        Self {
            input: &node.input,
            exprs: &node.exprs,
        }
    }
}

impl PartialOrd for SchemaPivotNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        SchemaPivotNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl SchemaPivotNode {
    #[allow(dead_code)]
    pub fn try_new(
        input: Arc<LogicalPlan>,
        names: Vec<String>,
        output_name: String,
    ) -> Result<Self> {
        let fields = vec![Field::new(output_name, DataType::Utf8, false)];
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            fields.into(),
            HashMap::new(),
        )?);
        let exprs = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        Ok(Self {
            input,
            names,
            schema,
            exprs,
        })
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
}

impl UserDefinedLogicalNodeCore for SchemaPivotNode {
    fn name(&self) -> &str {
        "SchemaPivot"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SchemaPivot")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if exprs.len() != self.exprs.len() {
            return plan_err!(
                "SchemaPivot: expected {} expressions, but got {}",
                self.exprs.len(),
                exprs.len()
            );
        }
        Ok(Self {
            input: Arc::new(inputs.one()?),
            names: self.names.clone(),
            schema: self.schema.clone(),
            exprs,
        })
    }
}
