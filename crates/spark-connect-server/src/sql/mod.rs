use crate::error::{SparkError, SparkResult};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

pub(crate) mod data_type;
pub(crate) mod expression;
pub(crate) mod function;
pub(crate) mod literal;
pub(crate) mod parser;
pub(crate) mod plan;
pub(crate) mod session_catalog;
pub(crate) mod utils;

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
>>>>>>> 50cb0623c0457f55b1ea4ec5bc3dc99d000a13cb
