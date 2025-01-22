use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::expression::Expr;
use crate::ast::identifier::Ident;
use crate::ast::keywords::{As, Select, With};
use crate::ast::operator::{Comma, LeftParenthesis, RightParenthesis};
use crate::combinator::{compose, sequence, unit};
use crate::Sequence;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr, DataType)")]
pub struct Query {
    #[parser(function = |(q, _, _), o| compose(q, o))]
    pub with: Option<WithClause>,
    #[parser(function = |(q, e, t), o| compose((q, e, t), o))]
    pub select: SelectClause,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Query")]
pub struct WithClause {
    pub with: With,
    #[parser(function = |q, o| sequence(compose(q, o), unit(o)))]
    pub ctes: Sequence<NamedQuery, Comma>,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Query")]
pub struct NamedQuery {
    pub name: Ident,
    pub columns: Option<(LeftParenthesis, Sequence<Ident, Comma>, RightParenthesis)>,
    pub r#as: Option<As>,
    pub left: LeftParenthesis,
    #[parser(function = |q, _| q)]
    pub query: Query,
    pub right: RightParenthesis,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr, DataType)")]
pub struct SelectClause {
    pub select: Select,
    #[parser(function = |(_, e, _), o| sequence(e, unit(o)))]
    pub expressions: Sequence<Expr, Comma>,
}
