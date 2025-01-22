use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::expression::Expr;
use crate::ast::query::Query;
use crate::ast::statement::explain::ExplainStatement;

pub mod explain;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Statement, Query, Expr, DataType)")]
pub enum Statement {
    Query(#[parser(function = |(_, q, e, _)| Query::parser((q, e)))] Query),
    Explain(#[parser(function = |(s, _, _, _)| ExplainStatement::parser(s))] ExplainStatement),
}
