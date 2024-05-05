use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_common::DFField;

use crate::error::SparkResult;
use crate::utils::CaseInsensitiveStringMap;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct UnresolvedRelation {
    pub(crate) multipart_identifier: Vec<String>,
    pub(crate) options: CaseInsensitiveStringMap,
    pub(crate) is_streaming: bool,
}
impl UnresolvedRelation {
    fn new(
        multipart_identifier: Vec<String>,
        options: CaseInsensitiveStringMap,
        is_streaming: bool,
    ) -> Self {
        UnresolvedRelation {
            multipart_identifier,
            options,
            is_streaming,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct UnresolvedRelationNode {
    unresolved_relation: UnresolvedRelation,
    schema: DFSchemaRef,
}

impl UnresolvedRelationNode {
    pub fn try_new(
        multipart_identifier: Vec<String>,
        options: CaseInsensitiveStringMap,
        is_streaming: bool,
    ) -> SparkResult<Self> {
        // TODO: Check to see if schema is accurate.
        // let schema: DFSchemaRef = Arc::new(DFSchema::empty());
        let schema: DFSchemaRef = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new_unqualified("id", DataType::Int64, false)],
            HashMap::new(),
        )?);
        let unresolved_relation: UnresolvedRelation =
            UnresolvedRelation::new(multipart_identifier, options, is_streaming);
        Ok(Self {
            unresolved_relation,
            schema,
        })
    }

    pub fn unresolved_relation(&self) -> &UnresolvedRelation {
        &self.unresolved_relation
    }
}

impl UserDefinedLogicalNodeCore for UnresolvedRelationNode {
    fn name(&self) -> &str {
        "UnresolvedRelation"
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

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnresolvedRelation: multipart_identifier={:?}, options={:?}, is_streaming={:?}",
            self.unresolved_relation.multipart_identifier,
            self.unresolved_relation.options,
            self.unresolved_relation.is_streaming
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}
