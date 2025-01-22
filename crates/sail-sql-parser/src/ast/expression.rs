use chumsky::extra::ParserExtra;
use chumsky::pratt::{infix, left, postfix, prefix};
use chumsky::prelude::choice;
use chumsky::{IterParser, Parser};
use either::Either;
use sail_sql_macro::TreeParser;

use crate::ast::data_type::DataType;
use crate::ast::identifier::{Ident, ObjectName};
use crate::ast::keywords::{
    All, And, Asc, Between, By, Case, Cube, Current, Desc, Distinct, Distribute, Div, Else, End,
    Exists, False, First, Following, From, Grouping, Ilike, In, Is, Last, Like, Not, Null, Nulls,
    Or, Order, Over, Partition, Preceding, Range, Rlike, Rollup, Row, Rows, Sets, Sort, Then, True,
    Unbounded, Unknown, When,
};
use crate::ast::literal::{NumberLiteral, StringLiteral};
use crate::ast::operator;
use crate::ast::operator::{
    Comma, DoubleColon, LeftBracket, LeftParenthesis, Period, RightBracket, RightParenthesis,
};
use crate::ast::query::Query;
use crate::container::{boxed, compose, sequence, unit};
use crate::token::Token;
use crate::tree::TreeParser;
use crate::{ParserOptions, Sequence};

#[derive(Debug, Clone)]
pub enum Expr {
    Atom(AtomExpr),
    Modifier(Box<Expr>, ExprModifier),
    UnaryOperator(UnaryOperator, Box<Expr>),
    BinaryOperator(Box<Expr>, BinaryOperator, Box<Expr>),
    Predicate(Box<Expr>, ExprPredicate),
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
        #[parser(function = |(e, _), o| compose(e, o).repeated().collect())]
        conditions: Vec<CaseWhen>,
        #[parser(function = |(e, _), o| compose(e, o))]
        r#else: Option<CaseElse>,
        end: End,
    },
    GroupingSets(
        Grouping,
        Sets,
        #[parser(function = |(e, _), o| compose(e, o))] NestedExpr,
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
    Identifier(Ident),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct NestedExpr(
    LeftParenthesis,
    #[parser(function = |e, o| sequence(e, unit(o)))] Sequence<Expr, Comma>,
    RightParenthesis,
);

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct FunctionExpr {
    name: ObjectName,
    left: LeftParenthesis,
    duplicate_treatment: Option<DuplicateTreatment>,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    arguments: Sequence<FunctionArgument, Comma>,
    right: RightParenthesis,
    #[parser(function = |e, o| compose(e, o))]
    over_clause: Option<OverClause>,
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
    over: Over,
    #[parser(function = |e, o| compose(e, o))]
    window: WindowSpec,
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
    partition: Either<Partition, Distribute>,
    by: By,
    columns: Sequence<Ident, Comma>,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OrderBy {
    order: Either<Order, Sort>,
    by: By,
    #[parser(function = |e, o| sequence(compose(e, o), unit(o)))]
    expressions: Sequence<OrderByExpr, Comma>,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct OrderByExpr {
    #[parser(function = |e, _| e)]
    expression: Expr,
    asc: Option<Either<Asc, Desc>>,
    nulls: Option<(Nulls, Either<First, Last>)>,
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
    when: When,
    #[parser(function = |e, _| boxed(e))]
    condition: Box<Expr>,
    then: Then,
    #[parser(function = |e, _| boxed(e))]
    result: Box<Expr>,
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Expr")]
pub struct CaseElse {
    r#else: Else,
    #[parser(function = |e, _| boxed(e))]
    result: Box<Expr>,
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
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
    ),
    ILike(
        Option<Not>,
        Ilike,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
    ),
    RLike(
        Option<Not>,
        Rlike,
        #[parser(function = |(e, _), _| boxed(e))] Box<Expr>,
    ),
}

impl<'a, E, P1, P2, P3> TreeParser<'a, E, (P1, P2, P3)> for Expr
where
    E: ParserExtra<'a, &'a [Token<'a>]>,
    P1: Parser<'a, &'a [Token<'a>], Expr, E> + Clone,
    P2: Parser<'a, &'a [Token<'a>], Query, E> + Clone,
    P3: Parser<'a, &'a [Token<'a>], DataType, E> + Clone,
{
    fn parser(
        (expr, query, data_type): (P1, P2, P3),
        options: &ParserOptions,
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
                left(2),
                And::parser((), options).map(BinaryOperator::And),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
            infix(
                left(1),
                Or::parser((), options).map(BinaryOperator::Or),
                |l, op, r| Expr::BinaryOperator(Box::new(l), op, Box::new(r)),
            ),
        ))
    }
}
