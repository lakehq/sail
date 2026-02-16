use std::fmt::Formatter;

use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

/// A leaf node representing cached in-memory data.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct InMemoryRelationNode {
    schema: DFSchemaRef,
    cache_id: String,
}

impl InMemoryRelationNode {
    /// Creates a new InMemoryRelation node with the given schema and cache ID.
    pub fn new(schema: DFSchemaRef, cache_id: String) -> Self {
        Self { schema, cache_id }
    }

    /// Returns the cache ID for this relation.
    pub fn cache_id(&self) -> &str {
        &self.cache_id
    }
}

impl PartialOrd for InMemoryRelationNode {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for InMemoryRelationNode {
    fn name(&self) -> &str {
        "InMemoryRelation"
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
        write!(f, "InMemoryRelation [{}]", self.cache_id)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}
