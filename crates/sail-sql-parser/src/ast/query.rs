use std::iter::Iterator;

use chumsky::extra::ParserExtra;
use chumsky::label::LabelError;
use chumsky::pratt::{infix, left};
use chumsky::prelude::choice;
use chumsky::{IterParser, Parser};
use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::expression::{
    DuplicateTreatment, Expr, FunctionArgument, GroupingExpr, OrderByExpr, WindowSpec,
};
use crate::ast::identifier::{ColumnIdent, Ident, ObjectName, TableIdent};
use crate::ast::keywords::{
    All, Anti, As, By, Cluster, Cross, Cube, Distinct, Except, Exclude, For, From, Full, Group,
    Having, In, Include, Inner, Intersect, Join, Lateral, Left, Limit, Minus, Name, Natural, Nulls,
    Offset, On, Order, Outer, Pivot, Recursive, Right, Rollup, Select, Semi, Sort, Union, Unpivot,
    Using, Values, View, Where, Window, With,
};
use crate::ast::operator::{Comma, LeftParenthesis, RightParenthesis};
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::token::{Token, TokenLabel};
use crate::tree::TreeParser;

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr, DataType)", label = TokenLabel::Query)]
pub struct Query {
    #[parser(function = |(q, _, _), o| compose(q, o))]
    pub with: Option<WithClause>,
    #[parser(function = |(q, e, _), o| boxed(compose((q, e), o)))]
    pub body: Box<QueryBody>,
    #[parser(function = |(_, e, _), o| compose(e, o))]
    pub modifiers: Vec<QueryModifier>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum QueryModifier {
    Window(#[parser(function = |e, o| compose(e, o))] WindowClause),
    OrderBy(#[parser(function = |e, o| compose(e, o))] OrderByClause),
    SortBy(#[parser(function = |e, o| compose(e, o))] SortByClause),
    ClusterBy(#[parser(function = |e, o| compose(e, o))] ClusterByClause),
    DistributeBy(#[parser(function = |e, o| compose(e, o))] DistributeByClause),
    Limit(#[parser(function = |e, o| compose(e, o))] LimitClause),
    Offset(#[parser(function = |e, o| compose(e, o))] OffsetClause),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Query")]
pub struct WithClause {
    pub with: With,
    pub recursive: Option<Recursive>,
    #[parser(function = |q, o| sequence(compose(q, o), unit(o)))]
    pub ctes: Sequence<NamedQuery, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Query")]
pub struct NamedQuery {
    pub name: Ident,
    pub columns: Option<IdentList>,
    pub r#as: Option<As>,
    pub left: LeftParenthesis,
    #[parser(function = |q, _| q)]
    pub query: Query,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
pub struct IdentList {
    pub left: LeftParenthesis,
    pub columns: Sequence<Ident, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone)]
pub enum QueryBody {
    Term(QueryTerm),
    SetOperation {
        left: Box<QueryBody>,
        operator: SetOperator,
        quantifier: Option<SetQuantifier>,
        right: Box<QueryBody>,
    },
}

impl<'a, 'opt, E, P1, P2> TreeParser<'a, 'opt, &'a [Token<'a>], E, (P1, P2)> for QueryBody
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    E::Error: LabelError<'a, &'a [Token<'a>], TokenLabel>,
    P1: Parser<'a, &'a [Token<'a>], Query, E> + Clone + 'a,
    P2: Parser<'a, &'a [Token<'a>], Expr, E> + Clone + 'a,
{
    fn parser(
        (query, expr): (P1, P2),
        options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        let quantifier = SetQuantifier::parser((), options).or_not();
        let term = QueryTerm::parser((query, expr), options).map(QueryBody::Term);
        term.pratt((
            infix(
                left(2),
                Intersect::parser((), options)
                    .map(SetOperator::Intersect)
                    .then(quantifier.clone()),
                |left, (operator, quantifier), right| QueryBody::SetOperation {
                    left: Box::new(left),
                    operator,
                    quantifier,
                    right: Box::new(right),
                },
            ),
            infix(
                left(1),
                choice((
                    Union::parser((), options).map(SetOperator::Union),
                    Except::parser((), options).map(SetOperator::Except),
                    Minus::parser((), options).map(SetOperator::Minus),
                ))
                .then(quantifier),
                |left, (operator, quantifier), right| QueryBody::SetOperation {
                    left: Box::new(left),
                    operator,
                    quantifier,
                    right: Box::new(right),
                },
            ),
        ))
    }
}

#[derive(Debug, Clone, TreeParser)]
pub enum SetOperator {
    Union(Union),
    Except(Except),
    Minus(Minus),
    Intersect(Intersect),
}

#[derive(Debug, Clone, TreeParser)]
pub enum SetQuantifier {
    Distinct(Distinct),
    DistinctByName(Distinct, By, Name),
    All(All),
    AllByName(All, By, Name),
    ByName(By, Name),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub enum QueryTerm {
    Select(#[parser(function = |(q, e), o| compose((q, e), o))] QuerySelect),
    Values(#[parser(function = |(_, e), o| compose(e, o))] ValuesClause),
    Nested(
        LeftParenthesis,
        #[parser(function = |(q, _), _| q)] Query,
        RightParenthesis,
    ),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub struct QuerySelect {
    #[parser(function = |(_, e), o| compose(e, o))]
    pub select: SelectClause,
    #[parser(function = |(q, e), o| compose((q, e), o))]
    pub from: Option<FromClause>,
    #[parser(function = |(_, e), o| compose(e, o))]
    pub lateral_views: Vec<LateralViewClause>,
    #[parser(function = |(_, e), o| compose(e, o))]
    pub r#where: Option<WhereClause>,
    #[parser(function = |(_, e), o| compose(e, o))]
    pub group_by: Option<GroupByClause>,
    #[parser(function = |(_, e), o| compose(e, o))]
    pub having: Option<HavingClause>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct ValuesClause {
    pub values: Values,
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub expressions: Sequence<Expr, Comma>,
    pub alias: Option<AliasClause>,
}

#[derive(Debug, Clone, TreeParser)]
pub struct AliasClause {
    pub r#as: Option<As>,
    pub table: TableIdent,
    pub columns: Option<IdentList>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct SelectClause {
    pub select: Select,
    pub quantifier: Option<DuplicateTreatment>,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub projection: Sequence<SelectExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct SelectExpr {
    #[parser(function = |e, _| e)]
    pub expr: Expr,
    pub alias: Option<(Option<As>, ColumnIdent)>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct NamedExpr {
    #[parser(function = |e, _| e)]
    pub expr: Expr,
    pub alias: Option<(Option<As>, Ident)>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct NamedExprList {
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<NamedExpr, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub struct FromClause {
    pub from: From,
    #[parser(function = |(q, e), o| sequence(compose((q, e), o), unit(o)))]
    pub tables: Sequence<TableWithJoins, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub struct TableWithJoins {
    pub lateral: Option<Lateral>,
    #[parser(function = |(q, e), o| compose((q, e), o))]
    pub table: TableFactor,
    #[parser(function = |(q, e), o| compose((q, e), o))]
    pub joins: Vec<TableJoin>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub enum TableFactor {
    Values {
        #[parser(function = |(_, e), o| compose(e, o))]
        values: ValuesClause,
        alias: Option<AliasClause>,
    },
    Query {
        left: LeftParenthesis,
        #[parser(function = |(q, _), _| q)]
        query: Query,
        right: RightParenthesis,
        #[parser(function = |(_, e), o| compose(e, o))]
        modifier: Option<TableModifier>,
        alias: Option<AliasClause>,
    },
    TableFunction {
        #[parser(function = |(_, e), o| compose(e, o))]
        function: TableFunction,
        alias: Option<AliasClause>,
    },
    Name {
        name: ObjectName,
        #[parser(function = |(_, e), o| compose(e, o))]
        modifier: Option<TableModifier>,
        alias: Option<AliasClause>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum TableModifier {
    Pivot(#[parser(function = |e, o| compose(e, o))] PivotClause),
    Unpivot(UnpivotClause),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct PivotClause {
    pub pivot: Pivot,
    pub left: LeftParenthesis,
    #[parser(function = |e, o| PivotAggregateSequence::parser(e, o).map(|x| x.into()))]
    pub aggregates: Sequence<NamedExpr, Comma>,
    pub r#for: For,
    pub columns: IdentList,
    pub r#in: In,
    #[parser(function = |e, o| compose(e, o))]
    pub values: NamedExprList,
    pub right: RightParenthesis,
}

/// A comma-separated sequence of named expressions to be used in the `PIVOT` clause,
/// where the last expression alias cannot be the `FOR` keyword.
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
struct PivotAggregateSequence {
    #[parser(function = |e, o| compose(e, o).then(unit(o)).repeated().collect())]
    items: Vec<(NamedExpr, Comma)>,
    #[parser(function = |e, _| e)]
    last: Expr,
    alias: Option<PivotAggregateLastAlias>,
}

#[derive(Debug, Clone, TreeParser)]
struct PivotAggregateLastAlias(
    Option<As>,
    #[parser(function = |(), o| For::parser((), o).not().rewind())] (),
    Ident,
);

impl std::convert::From<PivotAggregateSequence> for Sequence<NamedExpr, Comma> {
    fn from(value: PivotAggregateSequence) -> Self {
        let PivotAggregateSequence { items, last, alias } = value;
        let reminder = NamedExpr {
            expr: last,
            alias: alias.map(|PivotAggregateLastAlias(r#as, (), ident)| (r#as, ident)),
        };
        let (head, tail) =
            items
                .into_iter()
                .rfold((reminder, Vec::new()), |(head, mut tail), (expr, comma)| {
                    tail.push((comma, head));
                    (expr, tail)
                });
        Self {
            head: Box::new(head),
            tail: tail.into_iter().rev().collect(),
        }
    }
}

#[derive(Debug, Clone, TreeParser)]
pub struct UnpivotClause {
    pub unpivot: Unpivot,
    pub nulls: Option<UnpivotNulls>,
    pub left: LeftParenthesis,
    pub columns: UnpivotColumns,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
pub enum UnpivotNulls {
    IncludeNulls(Include, Nulls),
    ExcludeNulls(Exclude, Nulls),
}

#[derive(Debug, Clone, TreeParser)]
pub enum UnpivotColumns {
    SingleValue {
        values: Ident,
        r#for: For,
        name: Ident,
        r#in: In,
        left: LeftParenthesis,
        #[allow(clippy::type_complexity)]
        columns: Sequence<(Ident, Option<(Option<As>, Ident)>), Comma>,
        right: RightParenthesis,
    },
    MultiValue {
        values: IdentList,
        r#for: For,
        name: Ident,
        r#in: In,
        left: LeftParenthesis,
        #[allow(clippy::type_complexity)]
        columns: Sequence<(IdentList, Option<(Option<As>, Ident)>), Comma>,
        right: RightParenthesis,
    },
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct TableFunction {
    pub name: ObjectName,
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub arguments: Sequence<FunctionArgument, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Query, Expr)")]
pub struct TableJoin {
    // The join criteria must be absent for natural joins.
    // But we defer the enforcement of this to later stages of SQL analysis.
    pub natural: Option<Natural>,
    pub operator: Option<JoinOperator>,
    pub join: Join,
    pub lateral: Option<Lateral>,
    #[parser(function = |(q, e), o| compose((q, e), o))]
    pub other: TableFactor,
    #[parser(function = |(_, e), o| compose(e, o))]
    pub criteria: Option<JoinCriteria>,
}

#[derive(Debug, Clone, TreeParser)]
pub enum JoinOperator {
    Inner(Inner),
    Cross(Cross),
    Outer(Outer),
    Semi(Semi),
    Anti(Anti),
    LeftOuter(Left, Outer),
    LeftSemi(Left, Semi),
    LeftAnti(Left, Anti),
    Left(Left),
    RightOuter(Right, Outer),
    RightSemi(Right, Semi),
    RightAnti(Right, Anti),
    Right(Right),
    FullOuter(Full, Outer),
    Full(Full),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum JoinCriteria {
    On(On, #[parser(function = |e, _| e)] Expr),
    Using(Using, IdentList),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct LateralViewClause {
    pub lateral_view: (Lateral, View),
    pub outer: Option<Outer>,
    pub function: ObjectName,
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub arguments: Sequence<Expr, Comma>,
    pub right: RightParenthesis,
    #[parser(function = |_, o| unit(o).and_is(As::parser((), o).not()).or_not())]
    pub table: Option<ObjectName>,
    pub columns: Option<(Option<As>, Sequence<Ident, Comma>)>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct WhereClause {
    pub r#where: Where,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct GroupByClause {
    pub group_by: (Group, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub expressions: Sequence<GroupingExpr, Comma>,
    pub modifier: Option<GroupByModifier>,
}

#[derive(Debug, Clone, TreeParser)]
pub enum GroupByModifier {
    WithRollup(With, Rollup),
    WithCube(With, Cube),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct HavingClause {
    pub having: Having,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct WindowClause {
    pub window: Window,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<NamedWindow, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct NamedWindow {
    pub name: Ident,
    pub r#as: As,
    #[parser(function = |e, o| compose(e, o))]
    pub window: WindowSpec,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OrderByClause {
    pub order_by: (Order, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<OrderByExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct SortByClause {
    pub sort_by: (Sort, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub items: Sequence<OrderByExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct ClusterByClause {
    pub cluster_by: (Cluster, By),
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub items: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct DistributeByClause {
    pub distribute_by: (Cluster, By),
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub items: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct LimitClause {
    pub limit: Limit,
    #[parser(function = |e, o| compose(e, o))]
    pub value: LimitValue,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum LimitValue {
    All(All),
    Value(#[parser(function = |e, _| e)] Expr),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OffsetClause {
    pub offset: Offset,
    #[parser(function = |e, _| e)]
    pub value: Expr,
}
