use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

/// An unresolved reference to a subquery plan within a WithRelations context.
///
/// This node is a temporary marker used during plan resolution. It holds a `plan_id`
/// that references a subquery plan in the enclosing `WithRelations` node. After
/// resolution, all instances of this node must be replaced with the actual resolved
/// subquery `LogicalPlan`.
///
/// **This node must never reach physical planning or execution.** If DataFusion
/// attempts to create a physical plan from this node, it will fail because no
/// `ExtensionPlanner` is registered for it. This is intentional - the presence of
/// this node in a final plan indicates a bug in the resolver.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UnresolvedSubqueryRef {
    plan_id: i64,
    schema: DFSchemaRef,
}

impl PartialOrd for UnresolvedSubqueryRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.plan_id.partial_cmp(&other.plan_id)
    }
}

impl UnresolvedSubqueryRef {
    /// Creates a new unresolved subquery reference with the given plan_id.
    ///
    /// The placeholder has a dummy schema with one nullable Null column. This is required
    /// because DataFusion's ScalarSubquery accesses schema.field(0) during construction
    /// to determine the output type. The actual schema is irrelevant since this node
    /// will be replaced with the real subquery plan before execution.
    pub fn try_new(plan_id: i64) -> Result<Self> {
        let fields = vec![Field::new("_placeholder", DataType::Null, true)];
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            fields.into(),
            HashMap::new(),
        )?);
        Ok(Self { plan_id, schema })
    }

    /// Returns the plan_id this reference points to.
    pub fn plan_id(&self) -> i64 {
        self.plan_id
    }

    /// Creates a LogicalPlan containing this unresolved reference.
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// Extracts the plan_id from a LogicalPlan if it is an UnresolvedSubqueryRef.
    pub fn extract_plan_id(plan: &LogicalPlan) -> Option<i64> {
        if let LogicalPlan::Extension(ext) = plan {
            if let Some(node) = ext.node.as_any().downcast_ref::<UnresolvedSubqueryRef>() {
                return Some(node.plan_id);
            }
        }
        None
    }
}

impl UserDefinedLogicalNodeCore for UnresolvedSubqueryRef {
    fn name(&self) -> &str {
        "UnresolvedSubqueryRef"
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
            "UnresolvedSubqueryRef: plan_id={} (BUG: should have been resolved)",
            self.plan_id
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        inputs.zero()?;
        Ok(self.clone())
    }
}
