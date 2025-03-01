use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, MapExtra, ValueInput};
use chumsky::label::LabelError;
use chumsky::pratt::{infix, left, postfix, prefix};
use chumsky::prelude::{any, choice};
use chumsky::Parser;
use either::Either;
use sail_sql_macro::TreeParser;

use crate::ast::data_type::{DataType, IntervalDayTimeUnit, IntervalYearMonthUnit};
use crate::ast::identifier::{Ident, ObjectName, Variable};
use crate::ast::keywords::{
    All, And, Any, As, Asc, Between, Both, By, Case, Cast, Cube, Current, CurrentDate,
    CurrentTimestamp, CurrentUser, Date, Day, Days, Desc, Distinct, Div, Else, End, Escape, Exists,
    Extract, False, Filter, First, Following, For, From, Group, Grouping, Hour, Hours, Ignore,
    Ilike, In, Interval, Is, Last, Leading, Like, Microsecond, Microseconds, Millisecond,
    Milliseconds, Minute, Minutes, Month, Months, Not, Null, Nulls, Or, Order, Over, Overlay,
    Placing, Position, Preceding, Range, Regexp, Respect, Rlike, Rollup, Row, Rows, Second,
    Seconds, Sets, Similar, Struct, Substr, Substring, Table, Then, Timestamp, TimestampLtz,
    TimestampNtz, To, Trailing, Trim, True, Unbounded, Unknown, Week, Weeks, When, Where, Within,
    Year, Years,
};
use crate::ast::literal::{NumberLiteral, StringLiteral};
use crate::ast::operator;
use crate::ast::operator::{
    Comma, DoubleColon, LeftBracket, LeftParenthesis, Period, RightBracket, RightParenthesis,
};
use crate::ast::query::{
    ClusterByClause, DistributeByClause, NamedExpr, OrderByClause, PartitionByClause, Query,
    SortByClause,
};
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Token, TokenLabel};
use crate::tree::TreeParser;

#[derive(Debug, Clone)]
pub enum Expr {
    Atom(AtomExpr),
    UnaryOperator(UnaryOperator, Box<Expr>),
    BinaryOperator(Box<Expr>, BinaryOperator, Box<Expr>),
    Wildcard(Box<Expr>, Period, operator::Asterisk),
    Field(Box<Expr>, Period, Ident),
    Subscript(Box<Expr>, LeftBracket, Box<Expr>, RightBracket),
    Cast(Box<Expr>, DoubleColon, DataType),
    IsFalse(Box<Expr>, Is, Option<Not>, False),
    IsTrue(Box<Expr>, Is, Option<Not>, True),
    IsUnknown(Box<Expr>, Is, Option<Not>, Unknown),
    IsNull(Box<Expr>, Is, Option<Not>, Null),
    IsDistinctFrom(Box<Expr>, Is, Option<Not>, Distinct, From, Box<Expr>),
    InList(
        Box<Expr>,
        Option<Not>,
        In,
        LeftParenthesis,
        Sequence<Expr, Comma>,
        RightParenthesis,
    ),
    InSubquery(
        Box<Expr>,
        Option<Not>,
        In,
        LeftParenthesis,
        Query,
        RightParenthesis,
    ),
    Between(Box<Expr>, Option<Not>, Between, Box<Expr>, And, Box<Expr>),
    Like(
        Box<Expr>,
        Option<Not>,
        Like,
        Option<PatternQuantifier>,
        Box<Expr>,
        Option<PatternEscape>,
    ),
    ILike(
        Box<Expr>,
        Option<Not>,
        Ilike,
        Option<PatternQuantifier>,
        Box<Expr>,
        Option<PatternEscape>,
    ),
    RLike(Box<Expr>, Option<Not>, Rlike, Box<Expr>),
    RegExp(Box<Expr>, Option<Not>, Regexp, Box<Expr>),
    SimilarTo(
        Box<Expr>,
        Option<Not>,
        Similar,
        To,
        Box<Expr>,
        Option<PatternEscape>,
    ),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, Query, DataType)")]
pub enum AtomExpr {
    Subquery(
        LeftParenthesis,
        #[parser(function = |(_, q, _), _| q)] Query,
        RightParenthesis,
    ),
    Exists(
        Exists,
        LeftParenthesis,
        #[parser(function = |(_, q, _), _| q)] Query,
        RightParenthesis,
    ),
    Table(
        Table,
        #[parser(function = |(_, q, _), o| compose(q, o))] TableExpr,
    ),
    LambdaFunction {
        params: LambdaFunctionParameters,
        arrow: operator::Arrow,
        #[parser(function = |(e, _, _), _| boxed(e))]
        body: Box<Expr>,
    },
    Nested(
        LeftParenthesis,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        RightParenthesis,
    ),
    Tuple(
        LeftParenthesis,
        #[parser(function = |(e, _, _), o| sequence(compose((e, unit(o)), o), unit(o)))]
        Sequence<NamedExpr, Comma>,
        RightParenthesis,
    ),
    Struct(
        Struct,
        LeftParenthesis,
        #[parser(function = |(e, _, _), o| sequence(compose((e, unit(o)), o), unit(o)))]
        Sequence<NamedExpr, Comma>,
        RightParenthesis,
    ),
    Case {
        case: Case,
        #[parser(function = |(e, _, _), o| boxed(e).and_is(When::parser((), o).not()).or_not())]
        operand: Option<Box<Expr>>,
        #[parser(function = |(e, _, _), o| boxed(compose(e, o)))]
        first_condition: Box<CaseWhen>,
        #[parser(function = |(e, _, _), o| compose(e, o))]
        other_conditions: Vec<CaseWhen>,
        #[parser(function = |(e, _, _), o| boxed(compose(e, o)).or_not())]
        r#else: Option<Box<CaseElse>>,
        end: End,
    },
    Cast(
        Cast,
        LeftParenthesis,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        As,
        #[parser(function = |(_, _, d), _| d)] DataType,
        RightParenthesis,
    ),
    Extract(
        Extract,
        LeftParenthesis,
        Ident,
        From,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        RightParenthesis,
    ),
    Substring(
        Either<Substring, Substr>,
        LeftParenthesis,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        #[parser(function = |(e, _, _), o| unit(o).then(boxed(e)).or_not())]
        Option<(From, Box<Expr>)>,
        #[parser(function = |(e, _, _), o| unit(o).then(boxed(e)).or_not())]
        Option<(For, Box<Expr>)>,
        RightParenthesis,
    ),
    Trim(
        Trim,
        LeftParenthesis,
        #[parser(function = |(e, _, _), o| boxed(compose(e, o)))] Box<TrimExpr>,
        RightParenthesis,
    ),
    Overlay(
        Overlay,
        LeftParenthesis,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        Placing,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        #[parser(function = |(e, _, _), o| unit(o).then(boxed(e)).or_not())]
        Option<(For, Box<Expr>)>,
        RightParenthesis,
    ),
    Position(
        Position,
        LeftParenthesis,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        In,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        RightParenthesis,
    ),
    CurrentUser(CurrentUser, Option<(LeftParenthesis, RightParenthesis)>),
    CurrentTimestamp(
        CurrentTimestamp,
        Option<(LeftParenthesis, RightParenthesis)>,
    ),
    CurrentDate(CurrentDate, Option<(LeftParenthesis, RightParenthesis)>),
    // TODO: handle `timestamp(value)` and `date(value)` as normal functions in the plan resolver
    Timestamp(Timestamp, LeftParenthesis, StringLiteral, RightParenthesis),
    Date(Date, LeftParenthesis, StringLiteral, RightParenthesis),
    Function(#[parser(function = |(e, _, _), o| boxed(compose(e, o)))] Box<FunctionExpr>),
    Wildcard(operator::Asterisk),
    StringLiteral(StringLiteral, Vec<StringLiteral>),
    NumberLiteral(NumberLiteral),
    BooleanLiteral(BooleanLiteral),
    TimestampLiteral(Timestamp, StringLiteral),
    TimestampLtzLiteral(TimestampLtz, StringLiteral),
    TimestampNtzLiteral(TimestampNtz, StringLiteral),
    DateLiteral(Date, StringLiteral),
    Null(Null),
    Interval(
        Interval,
        #[parser(function = |(e, _, _), o| boxed(compose(e, o)))] Box<IntervalExpr>,
    ),
    Placeholder(Variable),
    Identifier(Ident),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Query")]
pub enum TableExpr {
    Name(ObjectName),
    NestedName(LeftParenthesis, ObjectName, RightParenthesis),
    Query(
        LeftParenthesis,
        #[parser(function = |q, _| q)] Query,
        RightParenthesis,
    ),
}

#[derive(Debug, Clone, TreeParser)]
pub enum BooleanLiteral {
    True(True),
    False(False),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct IntervalLiteral {
    pub interval: Option<Interval>,
    #[parser(function = |e, o| compose(e, o))]
    pub value: IntervalExpr,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum IntervalExpr {
    // The multi-unit pattern must be defined before the standard pattern,
    // since the multi-unit pattern can match longer inputs.
    // Some interval expressions such as `1 day` match both multi-unit and standard
    // patterns. They are parsed as multi-unit intervals.
    // Note that interval expressions such as `1 millisecond` can only be parsed as
    // multi-unit intervals since the unit is not recognized in the standard pattern.
    MultiUnit {
        #[parser(function = |e, o| compose(e, o))]
        head: IntervalValueWithUnit,
        // If the unit is followed by `TO` (e.g. `'1 1' DAY TO HOUR`), it must be parsed
        // as a standard interval,
        #[parser(function = |e, o| compose(e, o).and_is(To::parser((), o).not()))]
        tail: Vec<IntervalValueWithUnit>,
    },
    Standard {
        #[parser(function = |e, _| e)]
        value: Expr,
        qualifier: IntervalQualifier,
    },
    Literal(StringLiteral),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct IntervalValueWithUnit {
    #[parser(function = |e, _| e)]
    pub value: Expr,
    pub unit: IntervalUnit,
}

#[derive(Debug, Clone, TreeParser)]
pub enum IntervalUnit {
    Year(Year),
    Years(Years),
    Month(Month),
    Months(Months),
    Week(Week),
    Weeks(Weeks),
    Day(Day),
    Days(Days),
    Hour(Hour),
    Hours(Hours),
    Minute(Minute),
    Minutes(Minutes),
    Second(Second),
    Seconds(Seconds),
    Millisecond(Millisecond),
    Milliseconds(Milliseconds),
    Microsecond(Microsecond),
    Microseconds(Microseconds),
}

#[derive(Debug, Clone, TreeParser)]
pub enum IntervalQualifier {
    YearMonth(IntervalYearMonthUnit, Option<(To, IntervalYearMonthUnit)>),
    DayTime(IntervalDayTimeUnit, Option<(To, IntervalDayTimeUnit)>),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum TrimExpr {
    LeadingSpace(Leading, From, #[parser(function = |e, _| e)] Expr),
    Leading(
        Leading,
        #[parser(function = |e, _| e)] Expr,
        From,
        #[parser(function = |e, _| e)] Expr,
    ),
    TrailingSpace(Trailing, From, #[parser(function = |e, _| e)] Expr),
    Trailing(
        Trailing,
        #[parser(function = |e, _| e)] Expr,
        From,
        #[parser(function = |e, _| e)] Expr,
    ),
    BothSpace(Both, From, #[parser(function = |e, _| e)] Expr),
    Both(
        Option<Both>,
        #[parser(function = |e, _| e)] Expr,
        From,
        #[parser(function = |e, _| e)] Expr,
    ),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct FunctionExpr {
    pub name: ObjectName,
    #[parser(function = |e, o| compose(e, o))]
    pub arguments: FunctionArgumentList,
    // The null treatment can be inside or outside the function argument list.
    // The SQL analyzer should handle conflicts between the two.
    pub null_treatment: Option<NullTreatment>,
    #[parser(function = |e, o| compose(e, o))]
    pub within_group: Option<WithinGroupClause>,
    #[parser(function = |e, o| compose(e, o))]
    pub filter: Option<FilterClause>,
    #[parser(function = |e, o| compose(e, o))]
    pub over_clause: Option<OverClause>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct FunctionArgumentList {
    pub left: LeftParenthesis,
    pub duplicate_treatment: Option<DuplicateTreatment>,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)).or_not())]
    pub arguments: Option<Sequence<FunctionArgument, Comma>>,
    pub null_treatment: Option<NullTreatment>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum FunctionArgument {
    Named(
        Ident,
        operator::FatArrow,
        #[parser(function = |e, _| e)] Expr,
    ),
    Unnamed(#[parser(function = |e, _| e)] Expr),
}

#[derive(Debug, Clone, TreeParser)]
pub enum DuplicateTreatment {
    All(All),
    Distinct(Distinct),
}

#[derive(Debug, Clone, TreeParser)]
pub enum NullTreatment {
    RespectNulls(Respect, Nulls),
    IgnoreNulls(Ignore, Nulls),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct WithinGroupClause {
    pub within_group: (Within, Group),
    pub left: LeftParenthesis,
    pub order_by: (Order, By),
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub expressions: Sequence<OrderByExpr, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct FilterClause {
    pub filter: Filter,
    pub left: LeftParenthesis,
    pub r#where: Where,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OverClause {
    pub over: Over,
    #[parser(function = |e, o| compose(e, o))]
    pub window: WindowSpec,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum WindowSpec {
    Named(Ident),
    Unnamed {
        left: LeftParenthesis,
        #[parser(function = |e, o| compose(e, o))]
        modifiers: Vec<WindowModifier>,
        #[parser(function = |e, o| compose(e, o))]
        window_frame: Option<WindowFrame>,
        right: RightParenthesis,
    },
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum WindowModifier {
    ClusterBy(#[parser(function = |e, o| compose(e, o))] ClusterByClause),
    PartitionBy(#[parser(function = |e, o| compose(e, o))] PartitionByClause),
    DistributeBy(#[parser(function = |e, o| compose(e, o))] DistributeByClause),
    OrderBy(#[parser(function = |e, o| compose(e, o))] OrderByClause),
    SortBy(#[parser(function = |e, o| compose(e, o))] SortByClause),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OrderByExpr {
    #[parser(function = |e, _| e)]
    pub expr: Expr,
    pub direction: Option<OrderDirection>,
    pub nulls: Option<OrderNulls>,
}

#[derive(Debug, Clone, TreeParser)]
pub enum OrderDirection {
    Asc(Asc),
    Desc(Desc),
}

#[derive(Debug, Clone, TreeParser)]
pub enum OrderNulls {
    First(Nulls, First),
    Last(Nulls, Last),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum WindowFrame {
    RangeBetween(
        Range,
        Between,
        #[parser(function = |e, o| compose(e, o))] WindowFrameBound,
        And,
        #[parser(function = |e, o| compose(e, o))] WindowFrameBound,
    ),
    Range(
        Range,
        #[parser(function = |e, o| compose(e, o))] WindowFrameBound,
    ),
    RowsBetween(
        Rows,
        Between,
        #[parser(function = |e, o| compose(e, o))] WindowFrameBound,
        And,
        #[parser(function = |e, o| compose(e, o))] WindowFrameBound,
    ),
    Rows(
        Rows,
        #[parser(function = |e, o| compose(e, o))] WindowFrameBound,
    ),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum WindowFrameBound {
    UnboundedPreceding(Unbounded, Preceding),
    Preceding(#[parser(function = |e, _| e)] Expr, Preceding),
    CurrentRow(Current, Row),
    UnboundedFollowing(Unbounded, Following),
    Following(#[parser(function = |e, _| e)] Expr, Following),
}

#[derive(Debug, Clone, TreeParser)]
pub enum UnaryOperator {
    Plus(operator::Plus),
    Minus(operator::Minus),
    Not(Not),
    BitwiseNot(operator::Tilde),
    LogicalNot(operator::ExclamationMark),
}

#[derive(Debug, Clone, TreeParser)]
pub enum BinaryOperator {
    Plus(operator::Plus),
    Minus(operator::Minus),
    Multiply(operator::Asterisk),
    Divide(operator::Slash),
    IntegerDivide(Div),
    Modulo(operator::Percent),
    StringConcat(operator::DoubleVerticalBar),
    And(And),
    Or(Or),
    Eq(operator::Equals),
    EqEq(operator::DoubleEquals),
    NotEq(operator::NotEquals),
    NotLt(operator::NotLessThan),
    NotGt(operator::NotGreaterThan),
    LtGt(operator::LessThanGreaterThan),
    Lt(operator::LessThan),
    LtEq(operator::LessThanEquals),
    Gt(operator::GreaterThan),
    GtEq(operator::GreaterThanEquals),
    Spaceship(operator::Spaceship),
    BitwiseShiftLeft(operator::DoubleLessThan),
    BitwiseShiftRight(operator::DoubleGreaterThan),
    BitwiseShiftRightUnsigned(operator::TripleGreaterThan),
    BitwiseAnd(operator::Ampersand),
    BitwiseXor(operator::Caret),
    BitwiseOr(operator::VerticalBar),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct CaseWhen {
    pub when: When,
    #[parser(function = |e, _| e)]
    pub condition: Expr,
    pub then: Then,
    #[parser(function = |e, _| e)]
    pub result: Expr,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct CaseElse {
    pub r#else: Else,
    #[parser(function = |e, _| e)]
    pub result: Expr,
}

#[derive(Debug, Clone, TreeParser)]
pub enum LambdaFunctionParameters {
    Single(Ident),
    Multiple(LeftParenthesis, Sequence<Ident, Comma>, RightParenthesis),
}

#[derive(Debug, Clone, TreeParser)]
pub enum PatternQuantifier {
    All(All),
    Any(Any),
    Some(crate::ast::keywords::Some),
}

#[derive(Debug, Clone, TreeParser)]
pub struct PatternEscape {
    pub escape: Escape,
    pub value: StringLiteral,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum GroupingExpr {
    GroupingSets(
        Grouping,
        Sets,
        LeftParenthesis,
        #[parser(function = |e, o| sequence(compose(e, o), unit(o)))] Sequence<GroupingSet, Comma>,
        RightParenthesis,
    ),
    Cube(Cube, #[parser(function = |e, o| compose(e, o))] GroupingSet),
    Rollup(
        Rollup,
        #[parser(function = |e, o| compose(e, o))] GroupingSet,
    ),
    Default(#[parser(function = |e, _| e)] Expr),
}

// TODO: support nested grouping sets
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct GroupingSet {
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(e, unit(o)).or_not())]
    pub expressions: Option<Sequence<Expr, Comma>>,
    pub right: RightParenthesis,
}

// All private `struct`s or `enum`s are "internal" AST nodes used to parse expressions.
// They are not part of the final AST.

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
enum ExprModifier {
    Wildcard(Period, operator::Asterisk),
    Field(Period, Ident),
    Subscript(
        LeftBracket,
        #[parser(function = |(e, _), _| e)] Expr,
        RightBracket,
    ),
    Cast(DoubleColon, #[parser(function = |(_, d), _| d)] DataType),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, Query)")]
enum ExprPostfixPredicate {
    IsFalse(Is, Option<Not>, False),
    IsTrue(Is, Option<Not>, True),
    IsUnknown(Is, Option<Not>, Unknown),
    IsNull(Is, Option<Not>, Null),
    InList(
        Option<Not>,
        In,
        LeftParenthesis,
        #[parser(function = |(e, _), o| sequence(e, unit(o)))] Sequence<Expr, Comma>,
        RightParenthesis,
    ),
    InSubquery(
        Option<Not>,
        In,
        LeftParenthesis,
        #[parser(function = |(_, q), _| q)] Query,
        RightParenthesis,
    ),
}

#[derive(Debug, Clone, TreeParser)]
enum ExprInfixPredicate {
    IsDistinctFrom(Is, Option<Not>, Distinct, From),
    Between(Option<Not>, Between),
    Like(Option<Not>, Like, Option<PatternQuantifier>),
    ILike(Option<Not>, Ilike, Option<PatternQuantifier>),
    RLike(Option<Not>, Rlike),
    RegExp(Option<Not>, Regexp),
    SimilarTo(Option<Not>, Similar, To),
}

/// An expression fragment that can be parsed by a Pratt parser.
///
/// This is helpful for parsing certain ternary predicates:
///   * `a BETWEEN b AND c` is parsed as `((a BETWEEN b) AND c)`
///     since the `BETWEEN` operator has a higher binding power than `AND`.
///   * `a LIKE b ESCAPE c` is parsed as `(a LIKE (b ESCAPE c))`
///     since the `ESCAPE` postfix has a higher binding power than `LIKE`.
///     The same applies to `ILIKE` and `SIMILAR TO`.
///
/// The fragments are then built into an [`Expr`] at the end of parsing.
/// Any unmatched fragments of ternary predicates will result in a parser error.
#[derive(Debug, Clone)]
enum ExprFragment<T, S> {
    Singleton(Expr),
    UnaryOperator {
        op: UnaryOperator,
        expr: Box<ExprFragment<T, S>>,
    },
    BinaryOperator {
        left: Box<ExprFragment<T, S>>,
        op: BinaryOperator,
        right: Box<ExprFragment<T, S>>,
    },
    Modifier {
        expr: Box<ExprFragment<T, S>>,
        modifier: ExprModifier,
    },
    PostfixPredicate {
        expr: Box<ExprFragment<T, S>>,
        predicate: ExprPostfixPredicate,
    },
    InfixPredicate {
        token: T,
        span: S,
        left: Box<ExprFragment<T, S>>,
        predicate: ExprInfixPredicate,
        right: Box<ExprFragment<T, S>>,
    },
    Escape {
        token: T,
        span: S,
        expr: Box<ExprFragment<T, S>>,
        escape: PatternEscape,
    },
}

impl<'a, T, S> ExprFragment<T, S>
where
    T: Clone + 'a,
    S: Clone,
{
    fn build<I, E>(self) -> Result<Expr, E::Error>
    where
        I: Input<'a, Token = T, Span = S>,
        E: ParserExtra<'a, I>,
        E::Error: LabelError<'a, I, TokenLabel>,
    {
        match self {
            ExprFragment::Singleton(expr) => Ok(expr),
            ExprFragment::UnaryOperator { op, expr } => {
                Ok(Expr::UnaryOperator(op, Box::new(expr.build::<I, E>()?)))
            }
            ExprFragment::BinaryOperator { left, op, right } => match op {
                BinaryOperator::And(and) => left.build_logical_and::<I, E>(and, *right),
                _ => Ok(Expr::BinaryOperator(
                    Box::new(left.build::<I, E>()?),
                    op,
                    Box::new(right.build::<I, E>()?),
                )),
            },
            ExprFragment::Modifier { expr, modifier } => {
                let expr = expr.build::<I, E>()?;
                match modifier {
                    ExprModifier::Wildcard(x1, x2) => Ok(Expr::Wildcard(Box::new(expr), x1, x2)),
                    ExprModifier::Field(x1, x2) => Ok(Expr::Field(Box::new(expr), x1, x2)),
                    ExprModifier::Subscript(x1, x2, x3) => {
                        Ok(Expr::Subscript(Box::new(expr), x1, Box::new(x2), x3))
                    }
                    ExprModifier::Cast(x1, x2) => Ok(Expr::Cast(Box::new(expr), x1, x2)),
                }
            }
            ExprFragment::PostfixPredicate { expr, predicate } => {
                let expr = expr.build::<I, E>()?;
                match predicate {
                    ExprPostfixPredicate::IsFalse(x1, x2, x3) => {
                        Ok(Expr::IsFalse(Box::new(expr), x1, x2, x3))
                    }
                    ExprPostfixPredicate::IsTrue(x1, x2, x3) => {
                        Ok(Expr::IsTrue(Box::new(expr), x1, x2, x3))
                    }
                    ExprPostfixPredicate::IsUnknown(x1, x2, x3) => {
                        Ok(Expr::IsUnknown(Box::new(expr), x1, x2, x3))
                    }
                    ExprPostfixPredicate::IsNull(x1, x2, x3) => {
                        Ok(Expr::IsNull(Box::new(expr), x1, x2, x3))
                    }
                    ExprPostfixPredicate::InList(x1, x2, x3, x4, x5) => {
                        Ok(Expr::InList(Box::new(expr), x1, x2, x3, x4, x5))
                    }
                    ExprPostfixPredicate::InSubquery(x1, x2, x3, x4, x5) => {
                        Ok(Expr::InSubquery(Box::new(expr), x1, x2, x3, x4, x5))
                    }
                }
            }
            ExprFragment::InfixPredicate {
                token,
                span,
                left,
                predicate,
                right,
            } => match (*left, predicate, *right) {
                (left, ExprInfixPredicate::IsDistinctFrom(x1, x2, x3, x4), right) => {
                    Ok(Expr::IsDistinctFrom(
                        Box::new(left.build::<I, E>()?),
                        x1,
                        x2,
                        x3,
                        x4,
                        Box::new(right.build::<I, E>()?),
                    ))
                }
                (_, ExprInfixPredicate::Between(_, _), _) => {
                    Err(E::Error::expected_found(vec![], Some(token.into()), span))
                }
                (left, ExprInfixPredicate::Like(x3, x4, x5), right) => {
                    let (pattern, escape) = right.build_pattern_and_escape::<I, E>()?;
                    Ok(Expr::Like(
                        Box::new(left.build::<I, E>()?),
                        x3,
                        x4,
                        x5,
                        Box::new(pattern),
                        escape,
                    ))
                }
                (left, ExprInfixPredicate::ILike(x1, x2, x3), right) => {
                    let (pattern, escape) = right.build_pattern_and_escape::<I, E>()?;
                    Ok(Expr::ILike(
                        Box::new(left.build::<I, E>()?),
                        x1,
                        x2,
                        x3,
                        Box::new(pattern),
                        escape,
                    ))
                }
                (left, ExprInfixPredicate::RLike(x1, x2), right) => Ok(Expr::RLike(
                    Box::new(left.build::<I, E>()?),
                    x1,
                    x2,
                    Box::new(right.build::<I, E>()?),
                )),
                (left, ExprInfixPredicate::RegExp(x1, x2), right) => Ok(Expr::RegExp(
                    Box::new(left.build::<I, E>()?),
                    x1,
                    x2,
                    Box::new(right.build::<I, E>()?),
                )),
                (left, ExprInfixPredicate::SimilarTo(x1, x2, x3), right) => {
                    let (pattern, escape) = right.build_pattern_and_escape::<I, E>()?;
                    Ok(Expr::SimilarTo(
                        Box::new(left.build::<I, E>()?),
                        x1,
                        x2,
                        x3,
                        Box::new(pattern),
                        escape,
                    ))
                }
            },
            ExprFragment::Escape {
                token,
                span,
                expr: _,
                escape: _,
            } => Err(E::Error::expected_found(vec![], Some(token.into()), span)),
        }
    }

    fn build_pattern_and_escape<I, E>(self) -> Result<(Expr, Option<PatternEscape>), E::Error>
    where
        I: Input<'a, Token = T, Span = S>,
        E: ParserExtra<'a, I>,
        E::Error: LabelError<'a, I, TokenLabel>,
    {
        match self {
            ExprFragment::Escape {
                token: _,
                span: _,
                expr,
                escape,
            } => Ok((expr.build::<I, E>()?, Some(escape))),
            _ => Ok((self.build::<I, E>()?, None)),
        }
    }

    fn build_logical_and<I, E>(mut self, and: And, other: Self) -> Result<Expr, E::Error>
    where
        I: Input<'a, Token = T, Span = S>,
        E: ParserExtra<'a, I>,
        E::Error: LabelError<'a, I, TokenLabel>,
    {
        // Find the right-most leaf in the left expression tree,
        // and rewrite the expression if the leaf is the `BETWEEN` operator.
        // This would allow expressions such as `a AND b BETWEEN c AND d` to be parsed correctly.
        // The Pratt parser returns `((a AND (b BETWEEN c)) AND d)` for such a case,
        // which needs to be rewritten as `(a AND (b BETWEEN c AND d))`.
        let mut current = &mut self;
        loop {
            match current {
                ExprFragment::InfixPredicate {
                    left: expr,
                    predicate: ExprInfixPredicate::Between(not, between),
                    right: low,
                    ..
                } => {
                    *current = ExprFragment::Singleton(Expr::Between(
                        Box::new(expr.clone().build::<I, E>()?),
                        not.clone(),
                        between.clone(),
                        Box::new(low.clone().build::<I, E>()?),
                        and,
                        Box::new(other.build::<I, E>()?),
                    ));
                    return self.build::<I, E>();
                }
                ExprFragment::UnaryOperator { expr: next, .. }
                | ExprFragment::BinaryOperator { right: next, .. }
                | ExprFragment::InfixPredicate { right: next, .. } => {
                    current = next;
                }
                ExprFragment::Singleton(_)
                | ExprFragment::Modifier { .. }
                | ExprFragment::PostfixPredicate { .. }
                | ExprFragment::Escape { .. } => break,
            }
        }
        Ok(Expr::BinaryOperator(
            Box::new(self.build::<I, E>()?),
            BinaryOperator::And(and),
            Box::new(other.build::<I, E>()?),
        ))
    }
}

impl<'a, I, E, P1, P2, P3> TreeParser<'a, I, E, (P1, P2, P3)> for Expr
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, TokenLabel>,
    P1: Parser<'a, I, Expr, E> + Clone + 'a,
    P2: Parser<'a, I, Query, E> + Clone + 'a,
    P3: Parser<'a, I, DataType, E> + Clone + 'a,
{
    fn parser(
        (expr, query, data_type): (P1, P2, P3),
        options: &'a ParserOptions,
    ) -> impl Parser<'a, I, Self, E> + Clone {
        let atom = AtomExpr::parser((expr.clone(), query.clone(), data_type.clone()), options)
            .map(|atom| <ExprFragment<Token<'a>, I::Span>>::Singleton(Expr::Atom(atom)));
        atom.pratt((
            postfix(
                26,
                ExprModifier::parser((expr.clone(), data_type.clone()), options),
                |e, op| ExprFragment::Modifier {
                    expr: Box::new(e),
                    modifier: op,
                },
            ),
            prefix(
                25,
                choice((
                    operator::Plus::parser((), options).map(UnaryOperator::Plus),
                    operator::Minus::parser((), options).map(UnaryOperator::Minus),
                    operator::Tilde::parser((), options).map(UnaryOperator::BitwiseNot),
                )),
                |op, e| ExprFragment::UnaryOperator {
                    op,
                    expr: Box::new(e),
                },
            ),
            infix(
                left(24),
                choice((
                    operator::Asterisk::parser((), options).map(BinaryOperator::Multiply),
                    operator::Slash::parser((), options).map(BinaryOperator::Divide),
                    operator::Percent::parser((), options).map(BinaryOperator::Modulo),
                    Div::parser((), options).map(BinaryOperator::IntegerDivide),
                )),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(23),
                choice((
                    operator::Plus::parser((), options).map(BinaryOperator::Plus),
                    operator::Minus::parser((), options).map(BinaryOperator::Minus),
                    operator::DoubleVerticalBar::parser((), options)
                        .map(BinaryOperator::StringConcat),
                )),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(22),
                choice((
                    operator::TripleGreaterThan::parser((), options)
                        .map(BinaryOperator::BitwiseShiftRightUnsigned),
                    operator::DoubleGreaterThan::parser((), options)
                        .map(BinaryOperator::BitwiseShiftRight),
                    operator::DoubleLessThan::parser((), options)
                        .map(BinaryOperator::BitwiseShiftLeft),
                )),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(21),
                operator::Ampersand::parser((), options).map(BinaryOperator::BitwiseAnd),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(20),
                operator::Caret::parser((), options).map(BinaryOperator::BitwiseXor),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(19),
                operator::VerticalBar::parser((), options).map(BinaryOperator::BitwiseOr),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(18),
                choice((
                    operator::NotEquals::parser((), options).map(BinaryOperator::NotEq),
                    operator::NotGreaterThan::parser((), options).map(BinaryOperator::NotGt),
                    operator::NotLessThan::parser((), options).map(BinaryOperator::NotLt),
                    operator::DoubleEquals::parser((), options).map(BinaryOperator::EqEq),
                    operator::Equals::parser((), options).map(BinaryOperator::Eq),
                    operator::GreaterThanEquals::parser((), options).map(BinaryOperator::GtEq),
                    operator::GreaterThan::parser((), options).map(BinaryOperator::Gt),
                    operator::Spaceship::parser((), options).map(BinaryOperator::Spaceship),
                    operator::LessThanEquals::parser((), options).map(BinaryOperator::LtEq),
                    operator::LessThanGreaterThan::parser((), options).map(BinaryOperator::LtGt),
                    operator::LessThan::parser((), options).map(BinaryOperator::Lt),
                )),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            prefix(
                17,
                choice((
                    Not::parser((), options).map(UnaryOperator::Not),
                    operator::ExclamationMark::parser((), options).map(UnaryOperator::LogicalNot),
                )),
                |op, e| ExprFragment::UnaryOperator {
                    op,
                    expr: Box::new(e),
                },
            ),
            postfix(
                16,
                any()
                    .map_with(|t, extra: &mut MapExtra<'a, '_, _, _>| (t, extra.span()))
                    .rewind()
                    .then(PatternEscape::parser((), options)),
                |e, ((token, span), op)| ExprFragment::Escape {
                    token,
                    span,
                    expr: Box::new(e),
                    escape: op,
                },
            ),
            // The "postfix" predicates and "infix" predicates are allowed to have the same binding power.
            postfix(
                15,
                ExprPostfixPredicate::parser((expr.clone(), query.clone()), options),
                |e, op| ExprFragment::PostfixPredicate {
                    expr: Box::new(e),
                    predicate: op,
                },
            ),
            infix(
                left(15),
                any()
                    .map_with(|t, extra: &mut MapExtra<'a, '_, _, _>| (t, extra.span()))
                    .rewind()
                    .then(ExprInfixPredicate::parser((), options)),
                |l, ((token, span), op), r| ExprFragment::InfixPredicate {
                    token,
                    span,
                    left: Box::new(l),
                    predicate: op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(14),
                And::parser((), options).map(BinaryOperator::And),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(13),
                Or::parser((), options).map(BinaryOperator::Or),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
        ))
        .try_map(|e, _| e.build::<I, E>())
        .labelled(TokenLabel::Expression)
    }
}
