use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, PartialEq)]
pub struct MergeIntoOptions {
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub target: MergeTargetInfo,
    pub with_schema_evolution: bool,
    pub on_condition: Expr,
    pub matched_clauses: Vec<MergeMatchedClause>,
    pub not_matched_by_source_clauses: Vec<MergeNotMatchedBySourceClause>,
    pub not_matched_by_target_clauses: Vec<MergeNotMatchedByTargetClause>,
    /// Pre-analyzed join equality keys extracted from the ON condition (target, source)
    pub join_key_pairs: Vec<(Expr, Expr)>,
    /// Residual predicates from the ON condition that are not equality join keys
    pub residual_predicates: Vec<Expr>,
    /// Predicates from ON that only touch target columns (useful for early pruning)
    pub target_only_predicates: Vec<Expr>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct MergeTargetInfo {
    pub table_name: Vec<String>,
    pub format: String,
    pub location: String,
    pub partition_by: Vec<String>,
    pub options: Vec<Vec<(String, String)>>,
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

#[derive(Clone, Debug)]
pub struct MergeIntoNode {
    target: Arc<LogicalPlan>,
    source: Arc<LogicalPlan>,
    options: MergeIntoOptions,
    schema: DFSchemaRef,
    input_schema: DFSchemaRef,
}

impl MergeIntoNode {
    pub fn new(
        target: Arc<LogicalPlan>,
        source: Arc<LogicalPlan>,
        options: MergeIntoOptions,
        input_schema: DFSchemaRef,
    ) -> Self {
        Self {
            target,
            source,
            options,
            schema: Arc::new(DFSchema::empty()),
            input_schema,
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

    pub fn input_schema(&self) -> &DFSchemaRef {
        &self.input_schema
    }
}

impl PartialEq for MergeIntoNode {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for MergeIntoNode {}

impl Hash for MergeIntoNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as *const Self as usize).hash(state);
    }
}

impl PartialOrd for MergeIntoNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let a = self as *const Self as usize;
        let b = other as *const Self as usize;
        a.partial_cmp(&b)
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
            input_schema: self.input_schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }
}
