use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, SelectItem,
};
use sqlparser::ast::{ObjectName, UnaryOperator};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;
use std::any::TypeId;

#[derive(Debug)]
pub(crate) struct SparkDialect {}

impl SparkDialect {
    fn parse_exclamation_mark_unary(&self, parser: &mut Parser) -> Result<Expr, ParserError> {
        parser.expect_token(&Token::ExclamationMark)?;
        let expr = parser.parse_subexpr(Parser::UNARY_NOT_PREC)?;
        Ok(Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        })
    }

    fn parse_array_function(&self, parser: &mut Parser) -> Result<Expr, ParserError> {
        parser.expect_keyword(Keyword::ARRAY)?;
        parser.parse_function(ObjectName(vec![Ident::new("array")]))
    }

    fn parse_struct_function(&self, parser: &mut Parser) -> Result<Expr, ParserError> {
        parser.expect_keyword(Keyword::STRUCT)?;
        parser.expect_token(&Token::LParen)?;
        let args = parser
            .parse_comma_separated(Parser::parse_select_item)?
            .into_iter()
            .map(|x| -> Result<_, ParserError> {
                match x {
                    SelectItem::UnnamedExpr(expr) => {
                        Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Named {
                            name: alias,
                            expr: Box::new(expr),
                        })))
                    }
                    SelectItem::QualifiedWildcard(name, _) => Ok(FunctionArg::Unnamed(
                        FunctionArgExpr::QualifiedWildcard(name),
                    )),
                    SelectItem::Wildcard(_) => Ok(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        parser.expect_token(&Token::RParen)?;
        Ok(Expr::Function(Function {
            name: ObjectName(vec![Ident::new("struct")]),
            args: FunctionArguments::List(FunctionArgumentList {
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

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        match parser.peek_token().token {
            Token::ExclamationMark => Some(self.parse_exclamation_mark_unary(parser)),
            Token::Word(w) if w.keyword == Keyword::ARRAY => {
                Some(self.parse_array_function(parser))
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
