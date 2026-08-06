use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_datafusion_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct RemoteCheckpointRelationNode {
    relation_id: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
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

#[derive(Clone, Debug, Eq, PartialEq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct RemoteCheckpointCommandNode {
    relation_id: String,
    input: Arc<LogicalPlan>,
    #[educe(PartialOrd(ignore))]
    logical_schema: SchemaRef,
}

impl RemoteCheckpointCommandNode {
    pub fn new(relation_id: String, input: Arc<LogicalPlan>, logical_schema: SchemaRef) -> Self {
        Self {
            relation_id,
            input,
            logical_schema,
        }
    }

    pub fn relation_id(&self) -> &str {
        &self.relation_id
    }

    pub fn logical_schema(&self) -> &SchemaRef {
        &self.logical_schema
    }
}

impl UserDefinedLogicalNodeCore for RemoteCheckpointCommandNode {
    fn name(&self) -> &str {
        "RemoteCheckpointCommand"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        DFSchema::empty_ref()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteCheckpointCommand: relation_id={}",
            self.relation_id
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointCommand requires one input and no expressions"
            ));
        }
        Ok(Self {
            input: Arc::new(inputs.into_iter().next().ok_or_else(|| {
                internal_datafusion_err!("RemoteCheckpointCommand input is missing")
            })?),
            ..self.clone()
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
