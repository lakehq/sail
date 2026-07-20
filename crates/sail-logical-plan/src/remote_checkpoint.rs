use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_datafusion_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoteCheckpointRelationNode {
    relation_id: String,
    schema: DFSchemaRef,
}

impl PartialOrd for RemoteCheckpointRelationNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.relation_id.partial_cmp(&other.relation_id)
    }
}

impl RemoteCheckpointRelationNode {
    pub fn try_new(relation_id: String, schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            relation_id,
            schema: Arc::new(DFSchema::try_from(schema.as_ref().clone())?),
        })
    }

    pub fn relation_id(&self) -> &str {
        &self.relation_id
    }
}

impl UserDefinedLogicalNodeCore for RemoteCheckpointRelationNode {
    fn name(&self) -> &str {
        "RemoteCheckpointRelation"
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
        write!(
            f,
            "RemoteCheckpointRelation: relation_id={}",
            self.relation_id
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointRelation does not accept expressions or inputs"
            ));
        }
        Ok(self.clone())
    }
}
