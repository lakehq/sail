use chumsky::extra::ParserExtra;
use chumsky::pratt::{infix, left, postfix, prefix};
use chumsky::prelude::choice;
use chumsky::Parser;
use either::Either;
use sail_sql_macro::TreeParser;

use crate::ast::data_type::{DataType, IntervalDayTimeUnit, IntervalYearMonthUnit};
use crate::ast::identifier::{Ident, ObjectName, Variable};
use crate::ast::keywords::{
    All, And, Any, As, Asc, Between, Both, By, Case, Cast, Cube, Current, CurrentDate,
    CurrentTimestamp, Date, Day, Days, Desc, Distinct, Distribute, Div, Else, End, Escape, Exists,
    Extract, False, First, Following, For, From, Grouping, Hour, Hours, Ilike, In, Interval, Is,
    Last, Leading, Like, Microsecond, Microseconds, Millisecond, Milliseconds, Minute, Minutes,
    Month, Months, Not, Null, Nulls, Or, Order, Over, Overlay, Partition, Placing, Position,
    Preceding, Range, Rlike, Rollup, Row, Rows, Second, Seconds, Sets, Similar, Sort, Struct,
    Substr, Substring, Table, Then, Timestamp, To, Trailing, Trim, True, Unbounded, Unknown, Week,
    Weeks, When, Year, Years,
};
use crate::ast::literal::{NumberLiteral, StringLiteral};
use crate::ast::operator;
use crate::ast::operator::{
    Comma, DoubleColon, LeftBracket, LeftParenthesis, Period, RightBracket, RightParenthesis,
};
use crate::ast::query::{NamedExpr, Query};
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;
use crate::options::ParserOptions;
use crate::token::Token;
use crate::tree::TreeParser;

#[derive(Debug, Clone)]
pub enum Expr {
    Atom(AtomExpr),
    Modifier(Box<Expr>, ExprModifier),
    UnaryOperator(UnaryOperator, Box<Expr>),
    BinaryOperator(Box<Expr>, BinaryOperator, Box<Expr>),
    Predicate(Box<Expr>, ExprPredicate),
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
        #[parser(function = |(e, _, _), o| sequence(compose(e, o), unit(o)))]
        Sequence<NamedExpr, Comma>,
        RightParenthesis,
    ),
    Struct(
        Struct,
        LeftParenthesis,
        #[parser(function = |(e, _, _), o| sequence(compose(e, o), unit(o)))]
        Sequence<NamedExpr, Comma>,
        RightParenthesis,
    ),
    Case {
        case: Case,
        #[parser(function = |(e, _, _), o| When::parser((), o).not().rewind().ignore_then(boxed(e)).or_not())]
        operand: Option<Box<Expr>>,
        #[parser(function = |(e, _, _), o| compose(e, o))]
        conditions: (CaseWhen, Vec<CaseWhen>),
        #[parser(function = |(e, _, _), o| compose(e, o))]
        r#else: Option<CaseElse>,
        end: End,
    },
    GroupingSets(
        Grouping,
        Sets,
        LeftParenthesis,
        #[parser(function = |(e, _, _), o| sequence(compose(e, o), unit(o)))]
        Sequence<ExprList, Comma>,
        RightParenthesis,
    ),
    Cube(
        Cube,
        #[parser(function = |(e, _, _), o| compose(e, o))] ExprList,
    ),
    Rollup(
        Rollup,
        #[parser(function = |(e, _, _), o| compose(e, o))] ExprList,
    ),
    Cast(
        Cast,
        LeftParenthesis,
        #[parser(function = |(e, _, _), _| boxed(e))] Box<Expr>,
        As,
        #[parser(function = |(_, _, t), _| t)] DataType,
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
        #[parser(function = |(e, _, _), o| compose(e, o))] TrimExpr,
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
    CurrentTimestamp(CurrentTimestamp),
    CurrentDate(CurrentDate),
    Timestamp(Timestamp, LeftParenthesis, StringLiteral, RightParenthesis),
    Date(Date, LeftParenthesis, StringLiteral, RightParenthesis),
    Function(#[parser(function = |(e, _, _), o| compose(e, o))] FunctionExpr),
    Wildcard(operator::Asterisk),
    StringLiteral(StringLiteral),
    NumberLiteral(NumberLiteral),
    BooleanLiteral(BooleanLiteral),
    TimestampLiteral(Timestamp, StringLiteral),
    DateLiteral(Date, StringLiteral),
    Null(Null),
    Interval(
        Interval,
        #[parser(function = |(e, _, _), o| compose(e, o))] IntervalExpr,
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
        #[parser(function = |e, o| boxed(compose(e, o)))]
        head: Box<IntervalValueWithUnit>,
        // If the unit is followed by `TO` (e.g. `'1 1' DAY TO HOUR`), it must be parsed
        // as a standard interval,
        #[parser(function = |_, o| To::parser((), o).not().rewind())]
        barrier: (),
        #[parser(function = |e, o| compose(e, o))]
        tail: Vec<IntervalValueWithUnit>,
    },
    Standard {
        #[parser(function = |e, _| boxed(e))]
        value: Box<Expr>,
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
pub struct ExprList {
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub expressions: Sequence<Expr, Comma>,
    pub right: RightParenthesis,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub enum TrimExpr {
    LeadingSpace(
        Leading,
        From,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
    ),
    Leading(
        Leading,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
    ),
    TrailingSpace(
        Trailing,
        From,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
    ),
    Trailing(
        Trailing,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
    ),
    BothSpace(Both, From, #[parser(function = |e, _| boxed(e))] Box<Expr>),
    Both(
        Option<Both>,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
        From,
        #[parser(function = |e, _| boxed(e))] Box<Expr>,
    ),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct FunctionExpr {
    pub name: ObjectName,
    pub left: LeftParenthesis,
    pub duplicate_treatment: Option<DuplicateTreatment>,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)).or_not())]
    pub arguments: Option<Sequence<FunctionArgument, Comma>>,
    pub right: RightParenthesis,
    #[parser(function = |e, o| compose(e, o))]
    pub over_clause: Option<OverClause>,
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
    Detailed {
        left: LeftParenthesis,
        #[parser(function = |e, o| compose(e, o))]
        partition_by: Option<PartitionBy>,
        #[parser(function = |e, o| compose(e, o))]
        order_by: Option<OrderBy>,
        #[parser(function = |e, o| compose(e, o))]
        window_frame: Option<WindowFrame>,
        right: RightParenthesis,
    },
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct PartitionBy {
    pub partition: Either<Partition, Distribute>,
    pub by: By,
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub columns: Sequence<Expr, Comma>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OrderBy {
    pub order: Either<Order, Sort>,
    pub by: By,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub expressions: Sequence<OrderByExpr, Comma>,
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
    Preceding(#[parser(function = |e, _| boxed(e))] Box<Expr>, Preceding),
    CurrentRow(Current, Row),
    UnboundedFollowing(Unbounded, Following),
    Following(#[parser(function = |e, _| boxed(e))] Box<Expr>, Following),
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
    #[parser(function = |e, _| boxed(e))]
    pub condition: Box<Expr>,
    pub then: Then,
    #[parser(function = |e, _| boxed(e))]
    pub result: Box<Expr>,
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct CaseElse {
    pub r#else: Else,
    #[parser(function = |e, _| boxed(e))]
    pub result: Box<Expr>,
}

#[derive(Debug, Clone, TreeParser)]
pub enum LambdaFunctionParameters {
    Single(Ident),
    Multiple(LeftParenthesis, Sequence<Ident, Comma>, RightParenthesis),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, DataType)")]
pub enum ExprModifier {
    Wildcard(Period, operator::Asterisk),
    FieldAccess(Period, Ident),
    SubscriptAccess(
        LeftBracket,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
        RightBracket,
    ),
    Cast(DoubleColon, #[parser(function = |(_, t), _| t)] DataType),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, Query)")]
pub enum ExprPredicate {
    IsFalse(Is, Option<Not>, False),
    IsTrue(Is, Option<Not>, True),
    IsUnknown(Is, Option<Not>, Unknown),
    IsNull(Is, Option<Not>, Null),
    IsDistinctFrom(
        Is,
        Option<Not>,
        Distinct,
        From,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
    ),
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
    Between(
        Option<Not>,
        Between,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
        And,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
    ),
    Like(
        Option<Not>,
        Like,
        Option<PatternQuantifier>,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
        Option<PatternEscape>,
    ),
    ILike(
        Option<Not>,
        Ilike,
        Option<PatternQuantifier>,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
        Option<PatternEscape>,
    ),
    RLike(
        Option<Not>,
        Rlike,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
    ),
    SimilarTo(
        Option<Not>,
        Similar,
        To,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
        Option<PatternEscape>,
    ),
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

impl<'a, 'opt, E, P1, P2, P3> TreeParser<'a, 'opt, &'a [Token<'a>], E, (P1, P2, P3)> for Expr
where
    'opt: 'a,
    E: ParserExtra<'a, &'a [Token<'a>]>,
    P1: Parser<'a, &'a [Token<'a>], Expr, E> + Clone + 'a,
    P2: Parser<'a, &'a [Token<'a>], Query, E> + Clone + 'a,
    P3: Parser<'a, &'a [Token<'a>], DataType, E> + Clone + 'a,
{
    fn parser(
        (expr, query, data_type): (P1, P2, P3),
        options: &'opt ParserOptions,
    ) -> impl Parser<'a, &'a [Token<'a>], Self, E> + Clone {
        let atom = AtomExpr::parser((expr.clone(), query.clone(), data_type.clone()), options)
            .map(Expr::Atom);
        atom.pratt((
            postfix(
                26,
                ExprModifier::parser((expr.clone(), data_type.clone()), options),
                |e, op| Expr::Modifier(Box::new(e), op),
            ),
            prefix(
                25,
                choice((
                    operator::Plus::parser((), options).map(UnaryOperator::Plus),
                    operator::Minus::parser((), options).map(UnaryOperator::Minus),
                    operator::Tilde::parser((), options).map(UnaryOperator::BitwiseNot),
                )),
                |op, e| Expr::UnaryOperator(op, Box::new(e)),
            ),
            infix(
                left(24),
                choice((
                    operator::Asterisk::parser((), options).map(BinaryOperator::Multiply),
                    operator::Slash::parser((), options).map(BinaryOperator::Divide),
                    operator::Percent::parser((), options).map(BinaryOperator::Modulo),
                    Div::parser((), options).map(BinaryOperator::IntegerDivide),
                )),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            infix(
                left(23),
                choice((
                    operator::Plus::parser((), options).map(BinaryOperator::Plus),
                    operator::Minus::parser((), options).map(BinaryOperator::Minus),
                    operator::DoubleVerticalBar::parser((), options)
                        .map(BinaryOperator::StringConcat),
                )),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
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
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            infix(
                left(21),
                operator::Ampersand::parser((), options).map(BinaryOperator::BitwiseAnd),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            infix(
                left(20),
                operator::Caret::parser((), options).map(BinaryOperator::BitwiseXor),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            infix(
                left(19),
                operator::VerticalBar::parser((), options).map(BinaryOperator::BitwiseOr),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            infix(
                left(18),
                choice((
                    operator::NotEquals::parser((), options).map(BinaryOperator::NotEq),
                    operator::DoubleEquals::parser((), options).map(BinaryOperator::EqEq),
                    operator::Equals::parser((), options).map(BinaryOperator::Eq),
                    operator::GreaterThanEquals::parser((), options).map(BinaryOperator::GtEq),
                    operator::GreaterThan::parser((), options).map(BinaryOperator::Gt),
                    operator::Spaceship::parser((), options).map(BinaryOperator::Spaceship),
                    operator::LessThanEquals::parser((), options).map(BinaryOperator::LtEq),
                    operator::LessThanGreaterThan::parser((), options).map(BinaryOperator::LtGt),
                    operator::LessThan::parser((), options).map(BinaryOperator::Lt),
                )),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            prefix(
                17,
                choice((
                    Not::parser((), options).map(UnaryOperator::Not),
                    operator::ExclamationMark::parser((), options).map(UnaryOperator::LogicalNot),
                )),
                |op, e| Expr::UnaryOperator(op, Box::new(e)),
            ),
            postfix(
                16,
                ExprPredicate::parser((expr.clone(), query.clone()), options),
                |e, op| Expr::Predicate(Box::new(e), op),
            ),
            infix(
                left(15),
                And::parser((), options).map(BinaryOperator::And),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            infix(
                left(14),
                Or::parser((), options).map(BinaryOperator::Or),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
        ))
    }
}
