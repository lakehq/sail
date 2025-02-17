use chumsky::error::Error;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, MapExtra};
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
    CurrentTimestamp, CurrentUser, Date, Day, Days, Desc, Distinct, Distribute, Div, Else, End,
    Escape, Exists, Extract, False, First, Following, For, From, Grouping, Hour, Hours, Ilike, In,
    Interval, Is, Last, Leading, Like, Microsecond, Microseconds, Millisecond, Milliseconds,
    Minute, Minutes, Month, Months, Not, Null, Nulls, Or, Order, Over, Overlay, Partition, Placing,
    Position, Preceding, Range, Rlike, Rollup, Row, Rows, Second, Seconds, Sets, Similar, Sort,
    Struct, Substr, Substring, Table, Then, Timestamp, TimestampLtz, TimestampNtz, To, Trailing,
    Trim, True, Unbounded, Unknown, Week, Weeks, When, Year, Years,
};
use crate::ast::literal::{NumberLiteral, StringLiteral};
use crate::ast::operator;
use crate::ast::operator::{
    Comma, DoubleColon, LeftBracket, LeftParenthesis, Period, RightBracket, RightParenthesis,
};
use crate::ast::query::{NamedExpr, Query};
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;
use crate::span::{TokenInput, TokenInputSpan, TokenParserExtra};
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
        #[parser(function = |(_, q, _)| q)] Query,
        RightParenthesis,
    ),
    Exists(
        Exists,
        LeftParenthesis,
        #[parser(function = |(_, q, _)| q)] Query,
        RightParenthesis,
    ),
    Table(
        Table,
        #[parser(function = |(_, q, _)| compose(q))] TableExpr,
    ),
    LambdaFunction {
        params: LambdaFunctionParameters,
        arrow: operator::Arrow,
        #[parser(function = |(e, _, _)| boxed(e))]
        body: Box<Expr>,
    },
    Nested(
        LeftParenthesis,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        RightParenthesis,
    ),
    Tuple(
        LeftParenthesis,
        #[parser(function = |(e, _, _)| sequence(compose(e), unit()))] Sequence<NamedExpr, Comma>,
        RightParenthesis,
    ),
    Struct(
        Struct,
        LeftParenthesis,
        #[parser(function = |(e, _, _)| sequence(compose(e), unit()))] Sequence<NamedExpr, Comma>,
        RightParenthesis,
    ),
    Case {
        case: Case,
        #[parser(function = |(e, _, _)| When::parser(()).not().rewind().ignore_then(boxed(e)).or_not())]
        operand: Option<Box<Expr>>,
        #[parser(function = |(e, _, _)| compose(e))]
        conditions: (CaseWhen, Vec<CaseWhen>),
        #[parser(function = |(e, _, _)| compose(e))]
        r#else: Option<CaseElse>,
        end: End,
    },
    Cast(
        Cast,
        LeftParenthesis,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        As,
        #[parser(function = |(_, _, t)| t)] DataType,
        RightParenthesis,
    ),
    Extract(
        Extract,
        LeftParenthesis,
        Ident,
        From,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        RightParenthesis,
    ),
    Substring(
        Either<Substring, Substr>,
        LeftParenthesis,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        #[parser(function = |(e, _, _)| unit().then(boxed(e)).or_not())] Option<(From, Box<Expr>)>,
        #[parser(function = |(e, _, _)| unit().then(boxed(e)).or_not())] Option<(For, Box<Expr>)>,
        RightParenthesis,
    ),
    Trim(
        Trim,
        LeftParenthesis,
        #[parser(function = |(e, _, _)| compose(e))] TrimExpr,
        RightParenthesis,
    ),
    Overlay(
        Overlay,
        LeftParenthesis,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        Placing,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        #[parser(function = |(e, _, _)| unit().then(boxed(e)).or_not())] Option<(For, Box<Expr>)>,
        RightParenthesis,
    ),
    Position(
        Position,
        LeftParenthesis,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
        In,
        #[parser(function = |(e, _, _)| boxed(e))] Box<Expr>,
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
    Function(#[parser(function = |(e, _, _)| compose(e))] FunctionExpr),
    Wildcard(operator::Asterisk),
    StringLiteral(StringLiteral),
    NumberLiteral(NumberLiteral),
    BooleanLiteral(BooleanLiteral),
    TimestampLiteral(Timestamp, StringLiteral),
    TimestampLtzLiteral(TimestampLtz, StringLiteral),
    TimestampNtzLiteral(TimestampNtz, StringLiteral),
    DateLiteral(Date, StringLiteral),
    Null(Null),
    Interval(
        Interval,
        #[parser(function = |(e, _, _)| compose(e))] IntervalExpr,
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
        #[parser(function = |q| q)] Query,
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
    #[parser(function = |e| compose(e))]
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
        #[parser(function = |e| boxed(compose(e)))]
        head: Box<IntervalValueWithUnit>,
        // If the unit is followed by `TO` (e.g. `'1 1' DAY TO HOUR`), it must be parsed
        // as a standard interval,
        #[parser(function = |_| To::parser(()).not().rewind())]
        barrier: (),
        #[parser(function = |e| compose(e))]
        tail: Vec<IntervalValueWithUnit>,
    },
    Standard {
        #[parser(function = |e| boxed(e))]
        value: Box<Expr>,
        qualifier: IntervalQualifier,
    },
    Literal(StringLiteral),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct IntervalValueWithUnit {
    #[parser(function = |e| e)]
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
    LeadingSpace(Leading, From, #[parser(function = |e| boxed(e))] Box<Expr>),
    Leading(
        Leading,
        #[parser(function = |e| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |e| boxed(e))] Box<Expr>,
    ),
    TrailingSpace(Trailing, From, #[parser(function = |e| boxed(e))] Box<Expr>),
    Trailing(
        Trailing,
        #[parser(function = |e| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |e| boxed(e))] Box<Expr>,
    ),
    BothSpace(Both, From, #[parser(function = |e| boxed(e))] Box<Expr>),
    Both(
        Option<Both>,
        #[parser(function = |e| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |e| boxed(e))] Box<Expr>,
    ),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct FunctionExpr {
    pub name: ObjectName,
    pub left: LeftParenthesis,
    pub duplicate_treatment: Option<DuplicateTreatment>,
    #[parser(function = |e| sequence(compose(e), unit()).or_not())]
    pub arguments: Option<Sequence<FunctionArgument, Comma>>,
    pub right: RightParenthesis,
    #[parser(function = |e| compose(e))]
    pub over_clause: Option<OverClause>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum FunctionArgument {
    Named(Ident, operator::FatArrow, #[parser(function = |e| e)] Expr),
    Unnamed(#[parser(function = |e| e)] Expr),
}

#[derive(Debug, Clone, TreeParser)]
pub enum DuplicateTreatment {
    All(All),
    Distinct(Distinct),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OverClause {
    pub over: Over,
    #[parser(function = |e| compose(e))]
    pub window: WindowSpec,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum WindowSpec {
    Named(Ident),
    Detailed {
        left: LeftParenthesis,
        #[parser(function = |e| compose(e))]
        partition_by: Option<PartitionBy>,
        #[parser(function = |e| compose(e))]
        order_by: Option<OrderBy>,
        #[parser(function = |e| compose(e))]
        window_frame: Option<WindowFrame>,
        right: RightParenthesis,
    },
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct PartitionBy {
    pub partition: Either<Partition, Distribute>,
    pub by: By,
    #[parser(function = |e| sequence(e, unit()))]
    pub columns: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OrderBy {
    pub order: Either<Order, Sort>,
    pub by: By,
    #[parser(function = |e| sequence(compose(e), unit()))]
    pub expressions: Sequence<OrderByExpr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OrderByExpr {
    #[parser(function = |e| e)]
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
        #[parser(function = |e| compose(e))] WindowFrameBound,
        And,
        #[parser(function = |e| compose(e))] WindowFrameBound,
    ),
    Range(Range, #[parser(function = |e| compose(e))] WindowFrameBound),
    RowsBetween(
        Rows,
        Between,
        #[parser(function = |e| compose(e))] WindowFrameBound,
        And,
        #[parser(function = |e| compose(e))] WindowFrameBound,
    ),
    Rows(Rows, #[parser(function = |e| compose(e))] WindowFrameBound),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum WindowFrameBound {
    UnboundedPreceding(Unbounded, Preceding),
    Preceding(#[parser(function = |e| boxed(e))] Box<Expr>, Preceding),
    CurrentRow(Current, Row),
    UnboundedFollowing(Unbounded, Following),
    Following(#[parser(function = |e| boxed(e))] Box<Expr>, Following),
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
    #[parser(function = |e| boxed(e))]
    pub condition: Box<Expr>,
    pub then: Then,
    #[parser(function = |e| boxed(e))]
    pub result: Box<Expr>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct CaseElse {
    pub r#else: Else,
    #[parser(function = |e| boxed(e))]
    pub result: Box<Expr>,
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
        #[parser(function = |e| sequence(compose(e), unit()))] Sequence<GroupingSet, Comma>,
        RightParenthesis,
    ),
    Cube(Cube, #[parser(function = |e| compose(e))] GroupingSet),
    Rollup(Rollup, #[parser(function = |e| compose(e))] GroupingSet),
    Default(#[parser(function = |e| e)] Expr),
}

// TODO: support nested grouping sets
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct GroupingSet {
    pub left: LeftParenthesis,
    #[parser(function = |e| sequence(e, unit()).or_not())]
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
        #[parser(function = |(e, _)| e)] Expr,
        RightBracket,
    ),
    Cast(DoubleColon, #[parser(function = |(_, t)| t)] DataType),
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
        #[parser(function = |(e, _)| sequence(e, unit()))] Sequence<Expr, Comma>,
        RightParenthesis,
    ),
    InSubquery(
        Option<Not>,
        In,
        LeftParenthesis,
        #[parser(function = |(_, q)| q)] Query,
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

impl<'a, P1, P2, P3> TreeParser<'a, TokenInput<'a>, TokenParserExtra<'a>, (P1, P2, P3)> for Expr
where
    P1: Parser<'a, TokenInput<'a>, Expr, TokenParserExtra<'a>> + Clone + 'a,
    P2: Parser<'a, TokenInput<'a>, Query, TokenParserExtra<'a>> + Clone + 'a,
    P3: Parser<'a, TokenInput<'a>, DataType, TokenParserExtra<'a>> + Clone + 'a,
{
    fn parser(
        (expr, query, data_type): (P1, P2, P3),
    ) -> impl Parser<'a, TokenInput<'a>, Self, TokenParserExtra<'a>> + Clone {
        let atom = AtomExpr::parser((expr.clone(), query.clone(), data_type.clone()))
            .map(|atom| <ExprFragment<Token<'a>, TokenInputSpan<'a>>>::Singleton(Expr::Atom(atom)));
        atom.pratt((
            postfix(
                26,
                ExprModifier::parser((expr.clone(), data_type.clone())),
                |e, op| ExprFragment::Modifier {
                    expr: Box::new(e),
                    modifier: op,
                },
            ),
            prefix(
                25,
                choice((
                    operator::Plus::parser(()).map(UnaryOperator::Plus),
                    operator::Minus::parser(()).map(UnaryOperator::Minus),
                    operator::Tilde::parser(()).map(UnaryOperator::BitwiseNot),
                )),
                |op, e| ExprFragment::UnaryOperator {
                    op,
                    expr: Box::new(e),
                },
            ),
            infix(
                left(24),
                choice((
                    operator::Asterisk::parser(()).map(BinaryOperator::Multiply),
                    operator::Slash::parser(()).map(BinaryOperator::Divide),
                    operator::Percent::parser(()).map(BinaryOperator::Modulo),
                    Div::parser(()).map(BinaryOperator::IntegerDivide),
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
                    operator::Plus::parser(()).map(BinaryOperator::Plus),
                    operator::Minus::parser(()).map(BinaryOperator::Minus),
                    operator::DoubleVerticalBar::parser(()).map(BinaryOperator::StringConcat),
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
                    operator::TripleGreaterThan::parser(())
                        .map(BinaryOperator::BitwiseShiftRightUnsigned),
                    operator::DoubleGreaterThan::parser(()).map(BinaryOperator::BitwiseShiftRight),
                    operator::DoubleLessThan::parser(()).map(BinaryOperator::BitwiseShiftLeft),
                )),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(21),
                operator::Ampersand::parser(()).map(BinaryOperator::BitwiseAnd),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(20),
                operator::Caret::parser(()).map(BinaryOperator::BitwiseXor),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(19),
                operator::VerticalBar::parser(()).map(BinaryOperator::BitwiseOr),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(18),
                choice((
                    operator::NotEquals::parser(()).map(BinaryOperator::NotEq),
                    operator::DoubleEquals::parser(()).map(BinaryOperator::EqEq),
                    operator::Equals::parser(()).map(BinaryOperator::Eq),
                    operator::GreaterThanEquals::parser(()).map(BinaryOperator::GtEq),
                    operator::GreaterThan::parser(()).map(BinaryOperator::Gt),
                    operator::Spaceship::parser(()).map(BinaryOperator::Spaceship),
                    operator::LessThanEquals::parser(()).map(BinaryOperator::LtEq),
                    operator::LessThanGreaterThan::parser(()).map(BinaryOperator::LtGt),
                    operator::LessThan::parser(()).map(BinaryOperator::Lt),
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
                    Not::parser(()).map(UnaryOperator::Not),
                    operator::ExclamationMark::parser(()).map(UnaryOperator::LogicalNot),
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
                    .then(PatternEscape::parser(())),
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
                ExprPostfixPredicate::parser((expr.clone(), query.clone())),
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
                    .then(ExprInfixPredicate::parser(())),
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
                And::parser(()).map(BinaryOperator::And),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
            infix(
                left(13),
                Or::parser(()).map(BinaryOperator::Or),
                |l, op, r| ExprFragment::BinaryOperator {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                },
            ),
        ))
        .try_map(|e, _| e.build::<TokenInput<'a>, TokenParserExtra<'a>>())
        .labelled(TokenLabel::Expression)
    }
}
