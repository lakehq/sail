use sail_sql_macro::TreeParser;

use crate::ast::identifier::ObjectName;
use crate::ast::literal::{NumberLiteral, StringLiteral};
use crate::ast::operator::{LeftParenthesis, RightParenthesis};
use crate::ast::query::Query;
use crate::container::boxed;

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, Query)")]
pub enum Expr {
    StringLiteral(StringLiteral),
    NumberLiteral(NumberLiteral),
    ObjectName(ObjectName),
    Parenthesized(
        LeftParenthesis,
        #[parser(function = |(e, _)| boxed(e))] Box<Expr>,
        RightParenthesis,
    ),
}
