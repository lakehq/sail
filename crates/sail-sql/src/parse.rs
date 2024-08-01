use sqlparser::ast;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Word};

use crate::error::{SqlError, SqlResult};

/// [Credit]: <https://github.com/apache/datafusion/blob/13cb65e44136711befb87dd75fb8b41f814af16f/datafusion/sql/src/parser.rs#L483>
pub fn parse_option_key(parser: &mut Parser) -> SqlResult<String> {
    let next_token = parser.next_token();
    match next_token.token {
        Token::Word(Word { value, .. }) => {
            let mut parts = vec![value];
            while parser.consume_token(&Token::Period) {
                let next_token = parser.next_token();
                if let Token::Word(Word { value, .. }) = next_token.token {
                    parts.push(value);
                } else {
                    // Unquoted namespaced keys have to conform to the syntax
                    // "<WORD>[\.<WORD>]*". If we have a key that breaks this
                    // pattern, error out:
                    return Err(SqlError::invalid(format!(
                        "Expected key name, found: {next_token}"
                    )));
                }
            }
            Ok(parts.join("."))
        }
        Token::SingleQuotedString(s) => Ok(s),
        Token::DoubleQuotedString(s) => Ok(s),
        Token::EscapedStringLiteral(s) => Ok(s),
        _ => Err(SqlError::invalid(format!(
            "Expected key name, found: {next_token}"
        ))),
    }
}

/// [Credit]: <https://github.com/apache/datafusion/blob/13cb65e44136711befb87dd75fb8b41f814af16f/datafusion/sql/src/parser.rs#L514>
pub fn parse_option_value(parser: &mut Parser) -> SqlResult<String> {
    let next_token = parser.next_token();
    let value = match next_token.token {
        // e.g. things like "snappy" or "gzip" that may be keywords
        Token::Word(word) => Ok(ast::Value::SingleQuotedString(word.value)),
        Token::SingleQuotedString(s) => Ok(ast::Value::SingleQuotedString(s)),
        Token::DoubleQuotedString(s) => Ok(ast::Value::DoubleQuotedString(s)),
        Token::EscapedStringLiteral(s) => Ok(ast::Value::EscapedStringLiteral(s)),
        Token::Number(ref n, l) => match n.parse() {
            Ok(n) => Ok(ast::Value::Number(n, l)),
            // The tokenizer should have ensured `n` is a number
            // so this should not be possible
            Err(e) => Err(SqlError::invalid(format!(
                "Unexpected error: could not parse '{n}' as number: {e}"
            ))),
        },
        _ => Err(SqlError::invalid(format!(
            "Expected string or numeric value, found: {next_token}"
        ))),
    }?;
    Ok(value.to_string())
}

/// [Credit]: <https://github.com/apache/datafusion/blob/13cb65e44136711befb87dd75fb8b41f814af16f/datafusion/sql/src/parser.rs#L849>
pub fn parse_value_options(parser: &mut Parser) -> SqlResult<Vec<(String, String)>> {
    let mut options = vec![];
    parser.expect_token(&Token::LParen)?;
    loop {
        let key = parse_option_key(parser)?;
        let value = parse_option_value(parser)?;
        options.push((key, value));
        let comma = parser.consume_token(&Token::Comma);
        if parser.consume_token(&Token::RParen) {
            // allow a trailing comma, even though it's not in standard
            break;
        } else if !comma {
            return Err(SqlError::invalid(format!(
                "Expected ',' or ')' after option definition, found: {}",
                parser.peek_token()
            )));
        }
    }
    Ok(options)
}

pub fn parse_comment(parser: &mut Parser) -> SqlResult<Option<String>> {
    let _ = parser.consume_token(&Token::Eq);
    let next_token = parser.next_token();
    match next_token.token {
        Token::SingleQuotedString(str) => Ok(Some(str)),
        _ => Err(SqlError::invalid(format!(
            "Expected comment, found: {next_token}"
        ))),
    }
}

pub fn parse_file_format(parser: &mut Parser) -> SqlResult<String> {
    let token = parser.next_token();
    match &token.token {
        Token::Word(w) => Ok(w.value.to_uppercase()),
        _ => Err(SqlError::invalid(format!(
            "Expected file format as one of ARROW, PARQUET, AVRO, CSV, etc, found: {token}"
        ))),
    }
}
