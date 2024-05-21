use crate::error::{SparkError, SparkResult};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

pub(crate) mod data_type;
pub(crate) mod expression;
pub(crate) mod function;
pub(crate) mod literal;
pub(crate) mod parser;
pub(crate) mod plan;

pub(crate) fn fail_on_extra_token(parser: &mut Parser, kind: &str) -> SparkResult<()> {
    if parser.peek_token() != Token::EOF {
        let token = parser.next_token();
        Err(SparkError::invalid(format!(
            "extra tokens after {kind}: {token}"
        )))
    } else {
        Ok(())
    }
}
