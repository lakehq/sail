use sail_sql_macro::TreeParser;

use crate::ast::expression::Expr;
use crate::ast::keywords::{Select, With};
use crate::ast::operator::Comma;
use crate::container::sequence;
use crate::Sequence;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub struct Query {
    pub with_clause: Option<WithClause>,
    #[parser(function = |(q, e), o| SelectClause::parser((q, e), o))]
    pub select_clause: SelectClause,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct WithClause {
    pub with: With,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub struct SelectClause {
    pub select: Select,
    #[parser(function = |(q, e), o| sequence(Expr::parser((e, q), o), Comma::parser((), o)))]
    pub expressions: Sequence<Expr, Comma>,
}
