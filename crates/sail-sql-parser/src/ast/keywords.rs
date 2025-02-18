use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;
use chumsky::Parser;

use crate::ast::whitespace::skip_whitespace;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Keyword, Token, TokenLabel};
use crate::tree::TreeParser;

fn parse<'a, I, E>(
    keyword: Keyword,
) -> impl (Fn(&mut InputRef<'a, '_, I, E>) -> Result<TokenSpan, E::Error>) + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: std::convert::Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    move |input| {
        let before = input.offset();
        let span = match input.next() {
            Some(Token::Word {
                keyword: Some(k), ..
            }) if k == keyword => {
                <I::Span as std::convert::Into<TokenSpan>>::into(input.span_since(before))
            }
            x => {
                let mut e = E::Error::expected_found(
                    vec![],
                    x.map(std::convert::From::from),
                    input.span_since(before),
                );
                e.label_with(TokenLabel::Keyword(keyword));
                return Err(e);
            }
        };
        skip_whitespace(input);
        Ok(span)
    }
}

macro_rules! keyword_types {
    ([$(($_:expr, $name:ident),)* $(,)?]) => {
        $(
            #[allow(unused)]
            #[derive(Debug, Clone)]
            pub struct $name {
                pub span: TokenSpan,
            }

            impl $name {
                pub fn new(span: TokenSpan) -> Self {
                    Self { span }
                }

                pub const fn keyword() -> Keyword {
                    Keyword::$name
                }
            }

            impl<'a, I, E> TreeParser<'a, I, E> for $name
            where
                I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
                I::Span: std::convert::Into<TokenSpan>,
                E: ParserExtra<'a, I>,
                E::Error: LabelError<'a, I, TokenLabel>,
            {
                fn parser(
                    _args: (),
                    _options: &'a ParserOptions
                ) -> impl Parser<'a, I, Self, E> + Clone {
                    let f = parse(Self::keyword());
                    custom(move |input| f(input).map(Self::new))
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
