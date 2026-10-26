use sail_common_datafusion::datasource::{DeltaConstraintViolation, RowLevelTarget};

use super::model::*;

#[derive(Debug)]
pub struct Condition {
    pub expression: Expr,
    pub source: Option<String>,
}

#[derive(Debug)]
pub struct CheckConstraint {
    pub name: String,
    pub source: String,
    pub expression: Expr,
    pub violation: DeltaConstraintViolation,
}

#[derive(Debug)]
pub struct RowOperation {
    pub input: Query,
    pub target: RowLevelTarget,
    pub condition: Option<Condition>,
    pub case_sensitive: bool,
    pub action: RowAction,
}

#[derive(Debug)]
pub enum RowAction {
    Delete,
    Update {
        assignments: Vec<(String, Expr)>,
        generated: Vec<(String, Expr)>,
        constraints: Vec<CheckConstraint>,
    },
}
