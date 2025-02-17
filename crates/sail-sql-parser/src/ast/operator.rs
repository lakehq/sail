use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;
use chumsky::Parser;

use crate::ast::whitespace::whitespace;
use crate::span::TokenSpan;
use crate::token::{Punctuation, Token, TokenLabel};
use crate::tree::TreeParser;

fn operator_parser<'a, I, O, F, E>(
    punctuations: &'static [Punctuation],
    builder: F,
) -> impl Parser<'a, I, O, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    F: Fn(TokenSpan) -> O + Clone + 'static,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    custom(move |input| {
        let mut all = TokenSpan::default();
        for punctuation in punctuations {
            let offset = input.offset();
            let token = input.next();
            let span: I::Span = input.span_since(offset);
            match token {
                Some(Token::Punctuation(p)) if p == *punctuation => {
                    all = all.union(&span.into());
                }
                x => return Err(Error::expected_found(vec![], x.map(From::from), span)),
            }
        }
        Ok(all)
    })
    .map(builder)
    .then_ignore(whitespace().repeated())
    .labelled(TokenLabel::Operator(punctuations))
}

macro_rules! define_operator {
    ($name:ident, [$($punctuation:ident),*]) => {
        #[derive(Debug, Clone)]
        pub struct $name {
            pub span: TokenSpan,
        }

        impl $name {
            const fn punctuations() -> &'static [Punctuation] {
                &[$(Punctuation::$punctuation),*]
            }
        }

        impl<'a, I, E> TreeParser<'a, I, E> for $name
        where
            I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
            I::Span: Into<TokenSpan>,
            E: ParserExtra<'a, I>,
            E::Error: LabelError<'a, I, TokenLabel>,
        {
            fn parser(_args: ()) -> impl Parser<'a, I, Self, E> + Clone {
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
define_operator!(Spaceship, [LessThan, Equals, GreaterThan]);
define_operator!(NotEquals, [ExclamationMark, Equals]);
define_operator!(Arrow, [Minus, GreaterThan]);
define_operator!(FatArrow, [Equals, GreaterThan]);
define_operator!(TripleGreaterThan, [GreaterThan, GreaterThan, GreaterThan]);
