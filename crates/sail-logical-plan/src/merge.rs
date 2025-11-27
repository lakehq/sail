use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, PartialEq)]
pub struct MergeIntoOptions {
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub with_schema_evolution: bool,
    pub on_condition: Expr,
    pub matched_clauses: Vec<MergeMatchedClause>,
    pub not_matched_by_source_clauses: Vec<MergeNotMatchedBySourceClause>,
    pub not_matched_by_target_clauses: Vec<MergeNotMatchedByTargetClause>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergeMatchedClause {
    pub condition: Option<Expr>,
    pub action: MergeMatchedAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MergeMatchedAction {
    Delete,
    UpdateAll,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergeNotMatchedBySourceClause {
    pub condition: Option<Expr>,
    pub action: MergeNotMatchedBySourceAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MergeNotMatchedBySourceAction {
    Delete,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergeNotMatchedByTargetClause {
    pub condition: Option<Expr>,
    pub action: MergeNotMatchedByTargetAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MergeNotMatchedByTargetAction {
    InsertAll,
    InsertColumns {
        columns: Vec<String>,
        values: Vec<Expr>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergeAssignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergeIntoNode {
    target: Arc<LogicalPlan>,
    source: Arc<LogicalPlan>,
    options: MergeIntoOptions,
    schema: DFSchemaRef,
}

impl MergeIntoNode {
    pub fn new(
        target: Arc<LogicalPlan>,
        source: Arc<LogicalPlan>,
        options: MergeIntoOptions,
    ) -> Self {
        Self {
            target,
            source,
            options,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn options(&self) -> &MergeIntoOptions {
        &self.options
    }

    pub fn target(&self) -> &Arc<LogicalPlan> {
        &self.target
    }

    pub fn source(&self) -> &Arc<LogicalPlan> {
        &self.source
    }
}

#[derive(PartialEq, PartialOrd)]
struct MergeIntoNodeOrd<'a> {
    target: &'a Arc<LogicalPlan>,
    source: &'a Arc<LogicalPlan>,
    options: &'a MergeIntoOptions,
}

impl<'a> From<&'a MergeIntoNode> for MergeIntoNodeOrd<'a> {
    fn from(node: &'a MergeIntoNode) -> Self {
        Self {
            target: &node.target,
            source: &node.source,
            options: &node.options,
        }
    }
}

impl PartialOrd for MergeIntoNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        MergeIntoNodeOrd::from(self).partial_cmp(&other.into())
    }
}

impl UserDefinedLogicalNodeCore for MergeIntoNode {
    fn name(&self) -> &str {
        "MergeInto"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.target.as_ref(), self.source.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MergeInto: options={:?}", self.options)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        let (target, source) = inputs.two()?;
        Ok(Self {
            target: Arc::new(target),
            source: Arc::new(source),
            options: self.options.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}
