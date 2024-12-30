use chumsky::error::EmptyErr;
use chumsky::prelude::any;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::operator::Comma;
use crate::ast::whitespace::whitespace;
use crate::container::Sequence;
use crate::token::{Token, TokenSpan, TokenValue};
use crate::tree::TreeParser;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Ident {
    pub span: TokenSpan,
    pub value: String,
}

impl<'a> TreeParser<'a> for Ident {
    fn parser(_: ()) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
        any()
            .try_map(|t: Token<'a>, _| match t {
                Token {
                    value: TokenValue::Word { keyword: _, raw },
                    span,
                } => Ok(Ident {
                    span,
                    // FIXME: handle delimited identifiers
                    // FIXME: handle escape strings
                    value: raw.to_string(),
                }),
                _ => Err(EmptyErr::default()),
            })
            .then_ignore(whitespace().repeated())
    }
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct ObjectName(pub Sequence<Ident, Comma>);
