use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::expression::Expr;
use crate::ast::query::Query;
use crate::ast::statement::explain::ExplainStatement;
use crate::container::compose;

pub mod explain;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Statement, Query, Expr, DataType)")]
pub enum Statement {
    Query(#[parser(function = |(_, q, _, _), _| q)] Query),
    Explain(#[parser(function = |(s, _, _, _), o| compose(s, o))] ExplainStatement),
}
