use sail_common::spec;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithLocation, Word};
use sqlparser::{ast, keywords};

use crate::error::{SqlError, SqlResult};
use crate::utils::normalize_ident;

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
        Token::SingleQuotedString(s) => Ok(s.to_uppercase()),
        Token::DoubleQuotedString(s) => Ok(s.to_uppercase()),
        _ => Err(SqlError::invalid(format!(
            "Expected file format as one of ARROW, PARQUET, AVRO, CSV, etc, found: {token}"
        ))),
    }
}

pub fn parse_normalized_object_name(parser: &mut Parser) -> SqlResult<spec::ObjectName> {
    let mut idents: Vec<String> = vec![];
    loop {
        idents.push(String::from(parse_normalized_identifier(parser)?));
        if !parser.consume_token(&Token::Period) {
            break;
        }
    }
    if idents.iter().any(|ident| ident.contains('.')) {
        idents = idents
            .into_iter()
            .flat_map(|ident| ident.split('.').map(String::from).collect::<Vec<String>>())
            .collect()
    }
    Ok(spec::ObjectName::from(idents))
}

pub fn parse_normalized_identifier(parser: &mut Parser) -> SqlResult<spec::Identifier> {
    let next_token = parser.next_token();
    let ast_ident = match next_token.token {
        Token::Word(word) => {
            let mut ident = word.to_ident();
            maybe_append_number_identifier(parser, &mut ident);
            Ok(ident)
        }
        Token::Number(number, Some(postfix)) => {
            let mut ident = ast::Ident::with_quote('\"', number);
            ident.value.push_str(&postfix);
            maybe_append_number_identifier(parser, &mut ident);
            Ok(ident)
        }
        Token::SingleQuotedString(s) => {
            let mut ident = ast::Ident::with_quote('\'', s);
            maybe_append_number_identifier(parser, &mut ident);
            Ok(ident)
        }
        Token::DoubleQuotedString(s) => {
            let mut ident = ast::Ident::with_quote('\"', s);
            maybe_append_number_identifier(parser, &mut ident);
            Ok(ident)
        }
        _ => Err(SqlError::invalid(format!(
            "Expected identifier, found: {next_token}"
        ))),
    };
    Ok(normalize_ident(&ast_ident?).into())
}

fn maybe_append_number_identifier(parser: &mut Parser, ident: &mut ast::Ident) {
    while let Token::Number(ref peek_number, Some(_)) = parser.peek_token_no_skip().token {
        // This logic handles identifiers like "1m.2g", which are parsed as two tokens:
        // Token::Number("1", Some("m")) and Token::Number(".2", Some("g")).
        if !peek_number.starts_with('.') {
            break;
        }
        let token = parser
            .next_token_no_skip()
            .cloned()
            .unwrap_or(TokenWithLocation::wrap(Token::EOF));
        match token.token {
            Token::Number(next_number, Some(next_postfix)) => {
                ident.value.push_str(&next_number);
                ident.value.push_str(&next_postfix);
            }
            _ => unreachable!("parsing identifier expected number with postfix"),
        }
    }
}

/// [Credit]: <https://github.com/sqlparser-rs/sqlparser-rs/blob/v0.48.0/src/parser/mod.rs#L3363-L3390>
/// Parse a comma-separated list of 1+ items accepted by `F`
pub fn parse_comma_separated<T, F>(parser: &mut Parser, mut f: F) -> SqlResult<Vec<T>>
where
    F: FnMut(&mut Parser) -> SqlResult<T>,
{
    let mut values = vec![];
    loop {
        values.push(f(parser)?);
        if !parser.consume_token(&Token::Comma) {
            break;
        } else {
            // We decide to allow trailing commas because we don't have access to
            // `parser.options.trailing_commas` and it's an easy thing to allow.
            match parser.peek_token().token {
                Token::Word(kw) if keywords::RESERVED_FOR_COLUMN_ALIAS.contains(&kw.keyword) => {
                    break;
                }
                Token::RParen | Token::SemiColon | Token::EOF | Token::RBracket | Token::RBrace => {
                    break
                }
                _ => continue,
            }
        }
    }
    Ok(values)
}

pub fn parse_partition_column_name(parser: &mut Parser) -> SqlResult<spec::Identifier> {
    let name = spec::Identifier::from(normalize_ident(&parser.parse_identifier(false)?));
    Ok(name)
}
