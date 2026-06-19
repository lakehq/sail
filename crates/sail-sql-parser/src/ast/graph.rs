use sail_sql_macro::{TreeParser, TreeSyntax, TreeText};

use crate::ast::expression::{Expr, OrderByExpr};
use crate::ast::identifier::Ident;
use crate::ast::keywords::{By, Limit, Match, Order, Return, Skip, Where};
use crate::ast::operator::{
    Arrow, Colon, Comma, LeftArrow, LeftBrace, LeftBracket, LeftParenthesis, Minus, RightBrace,
    RightBracket, RightParenthesis,
};
use crate::ast::query::LimitValue;
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;
use crate::token::TokenLabel;

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr", label = TokenLabel::Statement)]
pub struct GraphQuery {
    pub r#match: Match,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub patterns: Sequence<GraphPathPattern, Comma>,
    #[parser(function = |e, o| compose(e, o))]
    pub r#where: Option<GraphWhereClause>,
    #[parser(function = |e, o| compose(e, o))]
    pub r#return: GraphReturnClause,
    #[parser(function = |e, o| compose(e, o))]
    pub order_by: Option<GraphOrderByClause>,
    #[parser(function = |e, o| compose(e, o))]
    pub skip: Option<GraphSkipClause>,
    #[parser(function = |e, o| compose(e, o))]
    pub limit: Option<GraphLimitClause>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphWhereClause {
    pub r#where: Where,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphReturnClause {
    pub r#return: Return,
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub items: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphOrderByClause {
    pub order_by: (Order, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<OrderByExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphSkipClause {
    pub skip: Skip,
    #[parser(function = |e, _| boxed(e))]
    pub value: Box<Expr>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphLimitClause {
    pub limit: Limit,
    #[parser(function = |e, o| compose(e, o))]
    pub value: LimitValue,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphPathPattern {
    #[parser(function = |e, o| compose(e, o))]
    pub start: GraphNodePattern,
    #[parser(function = |e, o| compose(e, o))]
    pub steps: Vec<GraphPatternStep>,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub enum GraphPatternStep {
    Outgoing(#[parser(function = |e, o| compose(e, o))] GraphOutgoingPattern),
    Incoming(#[parser(function = |e, o| compose(e, o))] GraphIncomingPattern),
    Undirected(#[parser(function = |e, o| compose(e, o))] GraphUndirectedPattern),
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphOutgoingPattern {
    pub left: Minus,
    #[parser(function = |e, o| compose(e, o))]
    pub edge: Option<GraphEdgePattern>,
    pub right: Arrow,
    #[parser(function = |e, o| compose(e, o))]
    pub target: GraphNodePattern,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphIncomingPattern {
    pub left: LeftArrow,
    #[parser(function = |e, o| compose(e, o))]
    pub edge: Option<GraphEdgePattern>,
    pub right: Minus,
    #[parser(function = |e, o| compose(e, o))]
    pub target: GraphNodePattern,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphUndirectedPattern {
    pub left: Minus,
    #[parser(function = |e, o| compose(e, o))]
    pub edge: Option<GraphEdgePattern>,
    pub right: Minus,
    #[parser(function = |e, o| compose(e, o))]
    pub target: GraphNodePattern,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphNodePattern {
    pub left: LeftParenthesis,
    pub variable: Option<Ident>,
    pub label: Option<(Colon, Ident)>,
    #[parser(function = |e, o| compose(e, o))]
    pub properties: Option<GraphPropertyMap>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphEdgePattern {
    pub left: LeftBracket,
    pub variable: Option<Ident>,
    pub label: Option<(Colon, Ident)>,
    #[parser(function = |e, o| compose(e, o))]
    pub properties: Option<GraphPropertyMap>,
    pub right: RightBracket,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphPropertyMap {
    pub left: LeftBrace,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)).or_not())]
    pub entries: Option<Sequence<GraphPropertyEntry, Comma>>,
    pub right: RightBrace,
}

#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "Expr")]
pub struct GraphPropertyEntry {
    pub key: Ident,
    pub colon: Colon,
    #[parser(function = |e, _| boxed(e))]
    pub value: Box<Expr>,
}
