use std::collections::HashMap;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sqlparser::ast::Ident;

use crate::error::SparkResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnresolvedRelationNode {
    multipart_identifier: Vec<Ident>,
    options: HashMap<String, String>,
    is_streaming: bool,
    schema: DFSchemaRef,
}

impl Hash for UnresolvedRelationNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the multipart_identifier and is_streaming fields
        self.multipart_identifier.hash(state);
        self.is_streaming.hash(state);

        // ashMap does not implement Hash, we need to ensure the hashing is order-independent
        // One way is to hash each key-value pair individually after sorting them by key
        let mut options: Vec<_> = self.options.iter().collect();
        options.sort_by_key(|&(key, _)| key);
        for (key, value) in options {
            key.hash(state);
            value.hash(state);
        }
    }
}

impl UnresolvedRelationNode {
    pub fn try_new(
        multipart_identifier: Vec<Ident>,
        options: HashMap<String, String>,
        is_streaming: bool,
    ) -> SparkResult<Self> {
        let schema: DFSchemaRef = Arc::new(DFSchema::empty());
        Ok(Self {
            multipart_identifier,
            options,
            is_streaming,
            schema,
        })
    }

    pub fn multipart_identifier(&self) -> &Vec<Ident> {
        &self.multipart_identifier
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    pub fn is_streaming(&self) -> bool {
        self.is_streaming
    }
}

impl UserDefinedLogicalNodeCore for UnresolvedRelationNode {
    fn name(&self) -> &str {
        "UnresolvedRelation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new() // No inputs as this node is unresolved
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new() // No expressions since it's unresolved
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnresolvedRelation: multipart_identifier={:?}, options={:?}, is_streaming={:?}",
            self.multipart_identifier, self.options, self.is_streaming
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}
