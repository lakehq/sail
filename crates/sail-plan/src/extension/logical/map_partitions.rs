use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::udf::StreamUDF;
use sail_common_datafusion::utils::rename_schema;

use crate::utils::ItemTaker;

#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Clone, Debug, Eq, Hash)]
pub(crate) struct MapPartitionsNode {
    input: Arc<LogicalPlan>,
    udf: Arc<dyn StreamUDF>,
    schema: DFSchemaRef,
}

impl MapPartitionsNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        output_names: Vec<String>,
        output_qualifiers: Vec<Option<TableReference>>,
        udf: Arc<dyn StreamUDF>,
    ) -> Result<Self> {
        let schema = rename_schema(&udf.output_schema(), &output_names)?;
        Ok(Self {
            input,
            udf,
            schema: Arc::new(DFSchema::from_field_specific_qualified_schema(
                output_qualifiers,
                &schema,
            )?),
        })
    }

    pub fn udf(&self) -> &Arc<dyn StreamUDF> {
        &self.udf
    }
}

impl PartialEq for MapPartitionsNode {
    fn eq(&self, other: &Self) -> bool {
        // We have to manually implement `PartialEq` instead of deriving it
        // due to `Arc<dyn ...>`.
        self.input == other.input
            && self.udf.as_ref() == other.udf.as_ref()
            && self.schema == other.schema
    }
}

#[derive(PartialEq, PartialOrd)]
struct MapPartitionsNodeOrd<'a> {
    input: &'a Arc<LogicalPlan>,
    udf: &'a Arc<dyn StreamUDF>,
}

impl<'a> From<&'a MapPartitionsNode> for MapPartitionsNodeOrd<'a> {
    fn from(node: &'a MapPartitionsNode) -> Self {
        Self {
            input: &node.input,
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
        "MapPartitions"
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
        write!(f, "MapPartitions")
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
