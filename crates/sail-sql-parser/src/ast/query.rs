use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::expression::Expr;
use crate::ast::keywords::{Select, With};
use crate::ast::operator::Comma;
use crate::combinator::{compose, sequence, unit};
use crate::Sequence;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr, DataType)")]
pub struct Query {
    pub with_clause: Option<WithClause>,
    #[parser(function = |(q, e, t), o| compose((q, e, t), o))]
    pub select_clause: SelectClause,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct WithClause {
    pub with: With,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr, DataType)")]
pub struct SelectClause {
    pub select: Select,
    #[parser(function = |(_, e, _), o| sequence(e, unit(o)))]
    pub expressions: Sequence<Expr, Comma>,
}
