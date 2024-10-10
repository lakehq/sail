use std::any::TypeId;

use sqlparser::ast;
use sqlparser::dialect::{Dialect, GenericDialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};

#[derive(Debug)]
pub struct SparkDialect {}

impl SparkDialect {
    fn parse_exclamation_mark_unary(&self, parser: &mut Parser) -> Result<ast::Expr, ParserError> {
        parser.expect_token(&Token::ExclamationMark)?;
        let expr = parser.parse_subexpr(self.prec_value(Precedence::UnaryNot))?;
        Ok(ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr: Box::new(expr),
        })
    }

    fn parse_array_function(&self, parser: &mut Parser) -> Result<ast::Expr, ParserError> {
        parser.expect_keyword(Keyword::ARRAY)?;
        if parser.peek_token().token == Token::LParen {
            parser.parse_function(ast::ObjectName(vec![ast::Ident::new("array")]))
        } else {
            Ok(ast::Expr::Identifier(ast::Ident::new("array")))
        }
    }

    fn parse_current_user_function(&self, parser: &mut Parser) -> Result<ast::Expr, ParserError> {
        parser.expect_keyword(Keyword::CURRENT_USER)?;
        parser.expect_token(&Token::LParen)?;
        parser.expect_token(&Token::RParen)?;
        Ok(ast::Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ast::Ident::new("current_user")]),
            parameters: ast::FunctionArguments::None,
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        }))
    }

    fn parse_struct_function(&self, parser: &mut Parser) -> Result<ast::Expr, ParserError> {
        parser.expect_keyword(Keyword::STRUCT)?;
        parser.expect_token(&Token::LParen)?;
        let args = parser
            .parse_comma_separated(Parser::parse_select_item)?
            .into_iter()
            .map(|x| -> Result<_, ParserError> {
                match x {
                    ast::SelectItem::UnnamedExpr(expr) => {
                        Ok(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)))
                    }
                    ast::SelectItem::ExprWithAlias { expr, alias } => Ok(
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Named {
                            name: alias,
                            expr: Box::new(expr),
                        })),
                    ),
                    ast::SelectItem::QualifiedWildcard(name, _) => Ok(ast::FunctionArg::Unnamed(
                        ast::FunctionArgExpr::QualifiedWildcard(name),
                    )),
                    ast::SelectItem::Wildcard(_) => {
                        Ok(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        parser.expect_token(&Token::RParen)?;
        Ok(ast::Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ast::Ident::new("struct")]),
            parameters: ast::FunctionArguments::None,
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        }))
    }
}

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

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<ast::Expr, ParserError>> {
        match parser.peek_token().token {
            Token::ExclamationMark => Some(self.parse_exclamation_mark_unary(parser)),
            Token::Word(w) if w.keyword == Keyword::ARRAY => {
                Some(self.parse_array_function(parser))
            }
            Token::Word(w) if w.keyword == Keyword::CURRENT_USER => {
                Some(self.parse_current_user_function(parser))
            }
            Token::Word(w) if w.keyword == Keyword::STRUCT => {
                Some(self.parse_struct_function(parser))
            }
            _ => None,
        }
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &ast::Expr,
        _precedence: u8,
    ) -> Option<Result<ast::Expr, ParserError>> {
        if parser.parse_keyword(Keyword::DIV) {
            return Some(Ok(ast::Expr::BinaryOp {
                left: Box::new(expr.clone()),
                op: ast::BinaryOperator::MyIntegerDivide,
                right: Box::new(parser.parse_expr().unwrap()),
            }));
        }
        None
    }
}

pub fn fail_on_extra_token(parser: &mut Parser, kind: &str) -> SqlResult<()> {
    if parser.peek_token().token != Token::EOF {
        let token = parser.next_token();
        Err(SqlError::invalid(format!(
            "extra tokens after {kind}: {token}"
        )))
    } else {
        Ok(())
    }
}
