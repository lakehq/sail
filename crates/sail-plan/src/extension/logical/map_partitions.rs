use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common::udf::MapIterUDF;
use sail_common::utils::rename_schema;

use crate::utils::ItemTaker;

#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Clone, Debug, Eq, Hash)]
pub(crate) struct MapPartitionsNode {
    input: Arc<LogicalPlan>,
    input_names: Vec<String>,
    output_names: Vec<String>,
    udf: Arc<dyn MapIterUDF>,
    schema: DFSchemaRef,
}

impl MapPartitionsNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        input_names: Vec<String>,
        output_names: Vec<String>,
        udf: Arc<dyn MapIterUDF>,
    ) -> Result<Self> {
        let schema = rename_schema(&udf.output_schema(), &output_names)?;
        Ok(Self {
            input,
            input_names,
            output_names,
            udf,
            schema: Arc::new(schema.try_into()?),
        })
    }

    pub fn input_names(&self) -> &[String] {
        &self.input_names
    }

    pub fn udf(&self) -> &Arc<dyn MapIterUDF> {
        &self.udf
    }
}

impl PartialEq for MapPartitionsNode {
    fn eq(&self, other: &Self) -> bool {
        // We have to manually implement `PartialEq` instead of deriving it
        // due to `Arc<dyn ...>`.
        self.input == other.input
            && self.input_names == other.input_names
            && self.output_names == other.output_names
            && self.udf.as_ref() == other.udf.as_ref()
            && self.schema == other.schema
    }
}

#[derive(PartialEq, PartialOrd)]
struct MapPartitionsNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
    input_names: &'a Vec<String>,
    output_names: &'a Vec<String>,
    udf: &'a Arc<dyn MapIterUDF>,
}

impl<'a> From<&'a MapPartitionsNode> for MapPartitionsNodeOrd<'a> {
    fn from(node: &'a MapPartitionsNode) -> Self {
        Self {
            input: &node.input,
            input_names: &node.input_names,
            output_names: &node.output_names,
            udf: &node.udf,
        }
    }
}

impl PartialOrd for MapPartitionsNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        MapPartitionsNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl UserDefinedLogicalNodeCore for MapPartitionsNode {
    fn name(&self) -> &str {
        "MapPartition"
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
        write!(f, "MapPartition")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        Ok(Self {
            input,
            ..self.clone()
        })
    }
}
