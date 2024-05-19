use sqlparser::ast::UnaryOperator;
use sqlparser::ast::{BinaryOperator, Expr};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;
use std::any::TypeId;

#[derive(Debug)]
pub(crate) struct SparkDialect {}

impl Dialect for SparkDialect {
    fn dialect(&self) -> TypeId {
        GenericDialect {}.dialect()
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        matches!(ch, '`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '_')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_')
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_lambda_functions(&self) -> bool {
        true
    }

    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        let token = parser.peek_token().token;
        if token == Token::ExclamationMark {
            parser.next_token();
            let expr = match parser.parse_subexpr(Parser::UNARY_NOT_PREC) {
                Ok(expr) => expr,
                e @ Err(_) => return Some(e),
            };
            return Some(Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            }));
        }
        None
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        if parser.parse_keyword(Keyword::DIV) {
            return Some(Ok(Expr::BinaryOp {
                left: Box::new(expr.clone()),
                op: BinaryOperator::MyIntegerDivide,
                right: Box::new(parser.parse_expr().unwrap()),
            }));
        }
        None
    }
}
