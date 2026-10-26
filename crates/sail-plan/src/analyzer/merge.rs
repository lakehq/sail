use sail_common_datafusion::datasource::RowLevelTarget;

use super::model::*;
use super::row_level::{CheckConstraint, Condition};

#[derive(Debug)]
pub struct MergeOperation {
    pub target: Query,
    pub source: Query,
    pub metadata: RowLevelTarget,
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub with_schema_evolution: bool,
    pub case_sensitive: bool,
    pub condition: Condition,
    pub matched: Vec<MergeClause>,
    pub source_missing: Vec<MergeClause>,
    pub target_missing: Vec<MergeClause>,
    pub generated: Vec<(String, Expr)>,
    pub defaults: Vec<(String, Expr)>,
    pub constraints: Vec<CheckConstraint>,
    pub join_keys: Vec<(Expr, Expr)>,
    pub residual: Vec<Expr>,
    pub target_predicates: Vec<Expr>,
}

#[derive(Debug)]
pub struct MergeClause {
    pub condition: Option<Condition>,
    pub action: MergeAction,
}

#[derive(Debug)]
pub enum MergeAction {
    Delete,
    Update(Vec<(AttributeId, Expr)>),
    Insert(Vec<(AttributeId, Expr)>),
}
