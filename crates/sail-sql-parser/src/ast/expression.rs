use chumsky::extra::ParserExtra;
use chumsky::pratt::{infix, left, postfix, prefix};
use chumsky::prelude::choice;
use chumsky::Parser;
use either::Either;
use sail_sql_macro::TreeParser;

use crate::ast::data_type::{DataType, IntervalDayTimeUnit, IntervalYearMonthUnit};
use crate::ast::identifier::{Ident, ObjectName, Variable};
use crate::ast::keywords::{
    All, And, Any, As, Asc, Between, By, Case, Cube, Current, Day, Days, Desc, Distinct,
    Distribute, Div, Else, End, Escape, Exists, False, First, Following, From, Grouping, Hour,
    Hours, Ilike, In, Interval, Is, Last, Like, Microsecond, Microseconds, Millisecond,
    Milliseconds, Minute, Minutes, Month, Months, Not, Null, Nulls, Or, Order, Over, Partition,
    Preceding, Range, Rlike, Rollup, Row, Rows, Second, Seconds, Sets, Similar, Sort, Then, To,
    True, Unbounded, Unknown, Week, Weeks, When, Year, Years,
};
use crate::ast::literal::{NumberLiteral, StringLiteral};
use crate::ast::operator;
use crate::ast::operator::{
    Comma, DoubleColon, LeftBracket, LeftParenthesis, Period, RightBracket, RightParenthesis,
};
use crate::ast::query::Query;
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
    Named(Box<Expr>, As, Ident),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Expr, Query)")]
pub enum AtomExpr {
    SubqueryExpr(
        LeftParenthesis,
        #[parser(function = |(_, q), _| q)] Query,
        RightParenthesis,
    ),
    ExistsExpr(
        Exists,
        LeftParenthesis,
        #[parser(function = |(_, q), _| q)] Query,
        RightParenthesis,
    ),
    Nested(#[parser(function = |(e, _), o| compose(e, o))] NestedExpr),
    Case {
        case: Case,
        #[parser(function = |(e, _), _| boxed(e).or_not())]
        operand: Option<Box<Expr>>,
        #[parser(function = |(e, _), o| compose(e, o))]
        conditions: (CaseWhen, Vec<CaseWhen>),
        #[parser(function = |(e, _), o| compose(e, o))]
        r#else: Option<CaseElse>,
        end: End,
    },
    GroupingSets(
        Grouping,
        Sets,
        LeftParenthesis,
        #[parser(function = |(e, _), o| sequence(compose(e, o), unit(o)))]
        Sequence<NestedExpr, Comma>,
        RightParenthesis,
    ),
    Cube(
        Cube,
        #[parser(function = |(e, _), o| compose(e, o))] NestedExpr,
    ),
    Rollup(
        Rollup,
        #[parser(function = |(e, _), o| compose(e, o))] NestedExpr,
    ),
    Function(#[parser(function = |(e, _), o| compose(e, o))] FunctionExpr),
    LambdaFunction {
        params: LambdaFunctionParameters,
        arrow: operator::Arrow,
        #[parser(function = |(e, _), _| boxed(e))]
        body: Box<Expr>,
    },
    Wildcard(operator::Asterisk),
    StringLiteral(StringLiteral),
    NumberLiteral(NumberLiteral),
    BooleanLiteral(BooleanLiteral),
    Null(Null),
    Interval(
        Interval,
        #[parser(function = |(e, _), o| compose(e, o))] IntervalExpr,
    ),
    Placeholder(Variable),
    Identifier(Ident),
}

#[derive(Debug, Clone, TreeParser)]
pub enum BooleanLiteral {
    True(True),
    False(False),
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

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct NestedExpr {
    pub left: LeftParenthesis,
    #[parser(function = |e, o| sequence(e, unit(o)))]
    pub expressions: Sequence<Expr, Comma>,
    pub right: RightParenthesis,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct FunctionExpr {
    pub name: ObjectName,
    pub left: LeftParenthesis,
    pub duplicate_treatment: Option<DuplicateTreatment>,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    pub arguments: Sequence<FunctionArgument, Comma>,
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

#[allow(unused)]
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
        partition_by: Option<PartitionBy>,
        #[parser(function = |e, o| compose(e, o))]
        order_by: Option<OrderBy>,
        #[parser(function = |e, o| compose(e, o))]
        window_frame: WindowFrame,
        right: RightParenthesis,
    },
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub struct PartitionBy {
    pub partition: Either<Partition, Distribute>,
    pub by: By,
    pub columns: Sequence<Ident, Comma>,
}

#[allow(unused)]
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
    Following(#[parser(function = |e, _| boxed(e))] Box<Expr>, Following),
    UnboundedFollowing(Unbounded, Following),
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

#[allow(unused)]
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

#[allow(unused)]
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
        let atom = AtomExpr::parser((expr.clone(), query.clone()), options).map(Expr::Atom);
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
                    operator::DoubleLessThan::parser((), options)
                        .map(BinaryOperator::BitwiseShiftLeft),
                    operator::DoubleGreaterThan::parser((), options)
                        .map(BinaryOperator::BitwiseShiftRight),
                    operator::TripleGreaterThan::parser((), options)
                        .map(BinaryOperator::BitwiseShiftRightUnsigned),
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
                    operator::Equals::parser((), options).map(BinaryOperator::Eq),
                    operator::DoubleEquals::parser((), options).map(BinaryOperator::EqEq),
                    operator::NotEquals::parser((), options).map(BinaryOperator::NotEq),
                    operator::LessThanGreaterThan::parser((), options).map(BinaryOperator::LtGt),
                    operator::LessThan::parser((), options).map(BinaryOperator::Lt),
                    operator::LessThanEquals::parser((), options).map(BinaryOperator::LtEq),
                    operator::GreaterThan::parser((), options).map(BinaryOperator::Gt),
                    operator::GreaterThanEquals::parser((), options).map(BinaryOperator::GtEq),
                    operator::Spaceship::parser((), options).map(BinaryOperator::Spaceship),
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
            postfix(
                1,
                As::parser((), options).then(Ident::parser((), options)),
                |e, (r#as, ident)| Expr::Named(Box::new(e), r#as, ident),
            ),
        ))
    }
}
