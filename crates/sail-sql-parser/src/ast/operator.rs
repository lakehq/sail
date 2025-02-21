use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;
use chumsky::Parser;

use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Punctuation, Token, TokenLabel};
use crate::tree::TreeParser;
use crate::utils::{labelled_error, skip_whitespace};

fn parse_operator<'a, I, E>(
    input: &mut InputRef<'a, '_, I, E>,
    punctuations: &'static [Punctuation],
) -> Result<TokenSpan, E::Error>
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let before = input.offset();
    for punctuation in punctuations {
        match input.next() {
            Some(Token::Punctuation(p)) if p == *punctuation => {
                continue;
            }
            x => {
                return Err(labelled_error::<I, E>(
                    x,
                    input.span_since(before),
                    TokenLabel::Operator(punctuations),
                ));
            }
        }
    }
    let span = input.span_since(before).into();
    skip_whitespace(input);
    Ok(span)
}

macro_rules! define_operator {
    ($name:ident, [$($punctuation:ident),*]) => {
        #[derive(Debug, Clone)]
        pub struct $name {
            pub span: TokenSpan,
        }

        impl $name {
            pub fn new(span: TokenSpan) -> Self {
                Self { span }
            }

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
            fn parser(
                _args: (),
                _options: &'a ParserOptions
            ) -> impl Parser<'a, I, Self, E> + Clone {
                custom(move |input| parse_operator(input, Self::punctuations()).map(Self::new))
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
define_operator!(NotLessThan, [ExclamationMark, LessThan]);
define_operator!(NotGreaterThan, [ExclamationMark, GreaterThan]);
define_operator!(Arrow, [Minus, GreaterThan]);
define_operator!(FatArrow, [Equals, GreaterThan]);
define_operator!(TripleGreaterThan, [GreaterThan, GreaterThan, GreaterThan]);
