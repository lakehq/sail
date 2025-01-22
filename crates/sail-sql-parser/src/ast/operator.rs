use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::prelude::custom;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::token::{Punctuation, Token, TokenSpan, TokenValue};
use crate::tree::TreeParser;
use crate::ParserOptions;

fn operator_parser<'a, O, F, E>(
    punctuations: &'static [Punctuation],
    builder: F,
) -> impl Parser<'a, &'a [Token<'a>], O, E> + Clone
where
    F: Fn(TokenSpan) -> O + Clone + 'static,
    E: ParserExtra<'a, &'a [Token<'a>]>,
{
    custom(move |input| {
        let mut span = TokenSpan::default();
        for punctuation in punctuations {
            let offset = input.offset();
            match input.next() {
                Some(Token {
                    value: TokenValue::Punctuation(p),
                    span: s,
                }) if p == *punctuation => {
                    span = span.union(&s);
                }
                x => {
                    return Err(Error::expected_found(
                        vec![Some(
                            Token::new(TokenValue::Punctuation(*punctuation), TokenSpan::default())
                                .into(),
                        )],
                        x.map(std::convert::From::from),
                        input.span_since(offset),
                    ))
                }
            }
        }
        Ok(span)
    })
    .then_ignore(whitespace().repeated())
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

        impl<'a, E> TreeParser<'a, E> for $identifier
        where
            E: ParserExtra<'a, &'a [Token<'a>]>,
        {
            fn parser(
                _args: (),
                _options: &ParserOptions,
            ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
                operator_parser(Self::punctuations(), |span| Self { span })
            }
        }
    };
}

define_operator!(LeftParenthesis, [LeftParenthesis]);
define_operator!(RightParenthesis, [RightParenthesis]);
define_operator!(LeftBracket, [LeftBracket]);
define_operator!(RightBracket, [RightBracket]);
define_operator!(LessThan, [LessThan]);
define_operator!(GreaterThan, [GreaterThan]);
define_operator!(Comma, [Comma]);
define_operator!(Period, [Period]);
define_operator!(Colon, [Colon]);
define_operator!(Semicolon, [Semicolon]);
define_operator!(Plus, [Plus]);
define_operator!(Minus, [Minus]);
define_operator!(Asterisk, [Asterisk]);
define_operator!(Slash, [Slash]);
define_operator!(Percent, [Percent]);
define_operator!(Equals, [Equals]);
define_operator!(VerticalBar, [VerticalBar]);
define_operator!(Ampersand, [Ampersand]);
define_operator!(Caret, [Caret]);
define_operator!(Tilde, [Tilde]);
define_operator!(ExclamationMark, [ExclamationMark]);
define_operator!(DoubleEquals, [Equals, Equals]);
define_operator!(DoubleVerticalBar, [VerticalBar, VerticalBar]);
define_operator!(DoubleColon, [Colon, Colon]);
define_operator!(DoubleLessThan, [LessThan, LessThan]);
define_operator!(DoubleGreaterThan, [GreaterThan, GreaterThan]);
define_operator!(GreaterThanEquals, [GreaterThan, Equals]);
define_operator!(LessThanEquals, [LessThan, Equals]);
define_operator!(LessThanGreaterThan, [LessThan, GreaterThan]);
define_operator!(NotEquals, [ExclamationMark, Equals]);
define_operator!(Arrow, [Minus, GreaterThan]);
define_operator!(FatArrow, [Equals, GreaterThan]);
define_operator!(TripleGreaterThan, [GreaterThan, GreaterThan, GreaterThan]);
