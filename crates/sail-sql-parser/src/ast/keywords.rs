use chumsky::extra::ParserExtra;
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::custom;
use chumsky::Parser;

use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Keyword, Token, TokenLabel};
use crate::tree::TreeParser;
use crate::utils::{labelled_error, skip_whitespace};

fn parse_keyword<'a, I, E>(
    input: &mut InputRef<'a, '_, I, E>,
    keyword: Keyword,
) -> Result<TokenSpan, E::Error>
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: std::convert::Into<TokenSpan>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let before = input.offset();
    match input.next() {
        Some(Token::Word {
            keyword: Some(k), ..
        }) if k == keyword => {
            let span = <I::Span as std::convert::Into<TokenSpan>>::into(input.span_since(before));
            skip_whitespace(input);
            Ok(span)
        }
        x => Err(labelled_error::<I, E>(
            x,
            input.span_since(before),
            TokenLabel::Keyword(keyword),
        )),
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
                    custom(move |input| parse_keyword(input, Self::keyword()).map(Self::new))
                }
            }
        )*
    }
}

for_all_keywords!(keyword_types);
