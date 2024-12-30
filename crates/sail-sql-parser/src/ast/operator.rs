use chumsky::error::EmptyErr;
use chumsky::prelude::custom;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::token::{Punctuation, Token, TokenSpan, TokenValue};
use crate::tree::TreeParser;

fn operator_parser<'a, O, F>(
    punctuations: &'static [Punctuation],
    builder: F,
) -> impl Parser<'a, &'a [Token<'a>], O> + Clone
where
    F: Fn(TokenSpan) -> O + Clone + 'static,
{
    custom(move |input| {
        let mut span = TokenSpan::default();
        for punctuation in punctuations {
            match input.next() {
                Some(Token {
                    value: TokenValue::Punctuation(p),
                    span: s,
                }) if p == *punctuation => {
                    span = span.union(&s);
                }
                _ => return Err(EmptyErr::default()),
            }
        }
        Ok(span)
    })
    .then_ignore(whitespace())
    .map(builder)
}

macro_rules! define_operator {
    ($identifier:ident, [$($punctuation:ident),*]) => {
        #[allow(unused)]
        #[derive(Debug, Clone)]
        pub struct $identifier {
            pub span: TokenSpan,
        }

        impl $identifier {
            pub const fn punctuations() -> &'static [Punctuation] {
                &[$(Punctuation::$punctuation),*]
            }
        }

        impl<'a> TreeParser<'a> for $identifier {
            fn parser(_: ()) -> impl Parser<'a, &'a [Token<'a>], Self> + Clone {
                operator_parser(Self::punctuations(), |span| Self { span })
            }
        }
    };
}

define_operator!(LeftParenthesis, [LeftParenthesis]);
define_operator!(RightParenthesis, [RightParenthesis]);
define_operator!(LessThan, [LessThan]);
define_operator!(GreaterThan, [GreaterThan]);
define_operator!(Comma, [Comma]);
define_operator!(Period, [Period]);
define_operator!(Colon, [Colon]);
define_operator!(Semicolon, [Semicolon]);
