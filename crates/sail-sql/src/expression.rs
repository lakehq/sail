use std::iter::once;

use sail_common::spec;
use sail_sql_parser::ast::expression::{
    AtomExpr, BinaryOperator, CaseElse, CaseWhen, Expr, ExprModifier, ExprPredicate,
    FunctionArgument, NestedExpr, OrderByExpr, OrderDirection, OrderNulls, PatternEscape,
    UnaryOperator, WindowFrame, WindowFrameBound,
};
use sail_sql_parser::ast::identifier::ObjectName;

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::literal::{LiteralValue, Signed};
use crate::query::from_ast_query;
use crate::value::{from_ast_boolean_literal, from_ast_number_literal, from_ast_string_literal};

struct Function {
    name: String,
    args: Vec<spec::Expr>,
}

impl From<Function> for spec::Expr {
    fn from(function: Function) -> spec::Expr {
        spec::Expr::UnresolvedFunction {
            function_name: function.name,
            arguments: function.args,
            is_distinct: false,
            is_user_defined_function: false,
        }
    }
}

fn negated(expr: spec::Expr) -> spec::Expr {
    spec::Expr::from(Function {
        name: "not".to_string(),
        args: vec![expr],
    })
}

pub(crate) fn from_ast_function_argument(arg: FunctionArgument) -> SqlResult<spec::Expr> {
    match arg {
        // TODO: Support named argument names.
        FunctionArgument::Unnamed(arg) | FunctionArgument::Named(_, _, arg) => {
            from_ast_expression(arg)
        }
    }
}

pub(crate) fn from_ast_object_name(name: ObjectName) -> SqlResult<spec::ObjectName> {
    Ok(name
        .0
        .into_items()
        .map(|i| i.value)
        .collect::<Vec<_>>()
        .into())
}

fn from_ast_unary_operator(op: UnaryOperator) -> SqlResult<String> {
    match op {
        UnaryOperator::Plus(_) => Ok("+".to_string()),
        UnaryOperator::Minus(_) => Ok("-".to_string()),
        UnaryOperator::Not(_) => Ok("not".to_string()),
        UnaryOperator::BitwiseNot(_) => Ok("~".to_string()),
        UnaryOperator::LogicalNot(_) => Ok("!".to_string()),
    }
}

fn from_ast_binary_operator(op: BinaryOperator) -> SqlResult<String> {
    match op {
        BinaryOperator::Plus(_) => Ok("+".to_string()),
        BinaryOperator::Minus(_) => Ok("-".to_string()),
        BinaryOperator::Multiply(_) => Ok("*".to_string()),
        BinaryOperator::Divide(_) => Ok("/".to_string()),
        BinaryOperator::Modulo(_) => Ok("%".to_string()),
        BinaryOperator::StringConcat(_) => Ok("concat".to_string()),
        BinaryOperator::Gt(_) => Ok(">".to_string()),
        BinaryOperator::Lt(_) => Ok("<".to_string()),
        BinaryOperator::GtEq(_) => Ok(">=".to_string()),
        BinaryOperator::LtEq(_) => Ok("<=".to_string()),
        BinaryOperator::Spaceship(_) => Ok("<=>".to_string()),
        BinaryOperator::EqEq(_) => Ok("==".to_string()),
        BinaryOperator::NotEq(_) => Ok("!=".to_string()),
        BinaryOperator::And(_) => Ok("and".to_string()),
        BinaryOperator::Or(_) => Ok("or".to_string()),
        BinaryOperator::BitwiseOr(_) => Ok("|".to_string()),
        BinaryOperator::BitwiseAnd(_) => Ok("&".to_string()),
        BinaryOperator::BitwiseXor(_) => Ok("^".to_string()),
        BinaryOperator::IntegerDivide(_) => Ok("div".to_string()),
        BinaryOperator::Eq(_) => Ok("==".to_string()),
        BinaryOperator::LtGt(_) => Ok("!=".to_string()),
        BinaryOperator::BitwiseShiftLeft(_) => Ok("<<".to_string()),
        BinaryOperator::BitwiseShiftRight(_) => Ok(">>".to_string()),
        BinaryOperator::BitwiseShiftRightUnsigned(_) => Ok(">>>".to_string()),
    }
}

pub(crate) fn from_ast_order_by(order_by: OrderByExpr) -> SqlResult<spec::SortOrder> {
    let OrderByExpr {
        expr,
        direction,
        nulls,
    } = order_by;
    let direction = match direction {
        None => spec::SortDirection::Unspecified,
        Some(OrderDirection::Asc(_)) => spec::SortDirection::Ascending,
        Some(OrderDirection::Desc(_)) => spec::SortDirection::Descending,
    };
    let null_ordering = match nulls {
        None => spec::NullOrdering::Unspecified,
        Some(OrderNulls::First(_, _)) => spec::NullOrdering::NullsFirst,
        Some(OrderNulls::Last(_, _)) => spec::NullOrdering::NullsLast,
    };
    Ok(spec::SortOrder {
        child: Box::new(from_ast_expression(expr)?),
        direction,
        null_ordering,
    })
}

#[allow(unused)]
fn from_ast_window_frame(frame: WindowFrame) -> SqlResult<spec::WindowFrame> {
    let (frame_type, start_bound, end_bound) = match frame {
        WindowFrame::Rows(_, start) => (spec::WindowFrameType::Row, start, None),
        WindowFrame::Range(_, start) => (spec::WindowFrameType::Range, start, None),
        WindowFrame::RangeBetween(_, _, start, _, end) => {
            (spec::WindowFrameType::Range, start, Some(end))
        }
        WindowFrame::RowsBetween(_, _, start, _, end) => {
            (spec::WindowFrameType::Row, start, Some(end))
        }
    };
    let lower = from_ast_window_frame_bound(start_bound)?;
    let upper = end_bound
        .map(from_ast_window_frame_bound)
        .transpose()?
        .unwrap_or(spec::WindowFrameBoundary::CurrentRow);
    Ok(spec::WindowFrame {
        frame_type,
        lower,
        upper,
    })
}

#[allow(unused)]
fn from_ast_window_frame_bound(bound: WindowFrameBound) -> SqlResult<spec::WindowFrameBoundary> {
    match bound {
        WindowFrameBound::CurrentRow(_, _) => Ok(spec::WindowFrameBoundary::CurrentRow),
        WindowFrameBound::UnboundedPreceding(_, _) | WindowFrameBound::UnboundedFollowing(_, _) => {
            Ok(spec::WindowFrameBoundary::Unbounded)
        }
        WindowFrameBound::Preceding(e, _) | WindowFrameBound::Following(e, _) => Ok(
            spec::WindowFrameBoundary::Value(Box::new(from_ast_expression(*e)?)),
        ),
    }
}

fn from_ast_nested_expr(expr: NestedExpr) -> SqlResult<Vec<spec::Expr>> {
    let NestedExpr {
        left: _,
        expressions,
        right: _,
    } = expr;
    expressions
        .into_items()
        .map(from_ast_expression)
        .collect::<SqlResult<Vec<_>>>()
}

pub(crate) fn from_ast_expression(expr: Expr) -> SqlResult<spec::Expr> {
    match expr {
        Expr::Atom(atom) => from_ast_atom_expr(atom),
        Expr::Modifier(expr, modifier) => from_ast_expr_modifier(*expr, modifier),
        Expr::UnaryOperator(op, expr) => Ok(spec::Expr::from(Function {
            name: from_ast_unary_operator(op)?,
            args: vec![from_ast_expression(*expr)?],
        })),
        Expr::BinaryOperator(left, op, right) => {
            let op = from_ast_binary_operator(op)?;
            Ok(spec::Expr::from(Function {
                name: op,
                args: vec![from_ast_expression(*left)?, from_ast_expression(*right)?],
            }))
        }
        Expr::Predicate(expr, predicate) => from_ast_expr_predicate(*expr, predicate),
        Expr::Named(expr, _, alias) => Ok(spec::Expr::Alias {
            expr: Box::new(from_ast_expression(*expr)?),
            name: vec![alias.value.into()],
            metadata: None,
        }),
    }
}

pub(crate) fn from_ast_atom_expr(atom: AtomExpr) -> SqlResult<spec::Expr> {
    match atom {
        AtomExpr::SubqueryExpr(_, query, _) => Ok(spec::Expr::ScalarSubquery {
            subquery: Box::new(from_ast_query(query)?),
        }),
        AtomExpr::ExistsExpr(_, _, query, _) => Ok(spec::Expr::Exists {
            subquery: Box::new(from_ast_query(query)?),
            negated: false,
        }),
        AtomExpr::Nested(NestedExpr {
            left: _,
            expressions,
            right: _,
        }) => {
            if expressions.tail.is_empty() {
                from_ast_expression(*expressions.head)
            } else {
                todo!()
            }
        }
        AtomExpr::Case {
            case: _,
            operand,
            conditions: (head, tail),
            r#else,
            end: _,
        } => {
            let operand = operand.map(|x| from_ast_expression(*x)).transpose()?;
            let mut args = vec![];
            once(head)
                .chain(tail.into_iter())
                .try_for_each::<_, SqlResult<_>>(|x| {
                    let CaseWhen {
                        when: _,
                        condition,
                        then: _,
                        result,
                    } = x;
                    let condition = from_ast_expression(*condition)?;
                    let condition = if let Some(ref operand) = operand {
                        spec::Expr::from(Function {
                            name: "==".to_string(),
                            args: vec![operand.clone(), condition],
                        })
                    } else {
                        condition
                    };
                    args.push(condition);
                    args.push(from_ast_expression(*result)?);
                    Ok(())
                })?;
            if let Some(r#else) = r#else {
                let CaseElse { r#else: _, result } = r#else;
                args.push(from_ast_expression(*result)?);
            }
            Ok(spec::Expr::from(Function {
                name: "when".to_string(),
                args,
            }))
        }
        AtomExpr::GroupingSets(_, _, _, expr, _) => {
            let expr = expr
                .into_items()
                .map(from_ast_nested_expr)
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::GroupingSets(expr))
        }
        AtomExpr::Cube(_, expr) => {
            let expr = from_ast_nested_expr(expr)?;
            Ok(spec::Expr::Cube(expr))
        }
        AtomExpr::Rollup(_, expr) => {
            let expr = from_ast_nested_expr(expr)?;
            Ok(spec::Expr::Rollup(expr))
        }
        AtomExpr::Function(_) => todo!(),
        AtomExpr::LambdaFunction { .. } => todo!(),
        AtomExpr::Wildcard(_) => todo!(),
        AtomExpr::StringLiteral(value) => from_ast_string_literal(value),
        AtomExpr::NumberLiteral(value) => from_ast_number_literal(value),
        AtomExpr::BooleanLiteral(value) => from_ast_boolean_literal(value),
        AtomExpr::Null(_) => Ok(spec::Expr::Literal(spec::Literal::Null)),
        AtomExpr::Interval(_, value) => Ok(spec::Expr::Literal(spec::Literal::try_from(
            LiteralValue(Signed::Positive(value)),
        )?)),
        AtomExpr::Placeholder(variable) => {
            Ok(spec::Expr::Placeholder(format!("${}", variable.value)))
        }
        AtomExpr::Identifier(x) => Ok(spec::Expr::UnresolvedAttribute {
            name: spec::ObjectName::new_unqualified(x.value.into()),
            plan_id: None,
        }),
    }
}

pub(crate) fn from_ast_expr_modifier(expr: Expr, modifier: ExprModifier) -> SqlResult<spec::Expr> {
    let expr = Box::new(from_ast_expression(expr)?);
    match modifier {
        ExprModifier::FieldAccess(_, _field) => todo!(),
        ExprModifier::SubscriptAccess(_, _subscript, _) => todo!(),
        ExprModifier::Cast(_, data_type) => Ok(spec::Expr::Cast {
            expr,
            cast_to_type: from_ast_data_type(data_type)?,
        }),
    }
}

pub(crate) fn from_ast_expr_predicate(
    expr: Expr,
    predicate: ExprPredicate,
) -> SqlResult<spec::Expr> {
    let expr = from_ast_expression(expr)?;
    match predicate {
        ExprPredicate::IsFalse(_, not, _) => {
            if not.is_some() {
                Ok(spec::Expr::IsNotFalse(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsFalse(Box::new(expr)))
            }
        }
        ExprPredicate::IsTrue(_, not, _) => {
            if not.is_some() {
                Ok(spec::Expr::IsNotTrue(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsTrue(Box::new(expr)))
            }
        }
        ExprPredicate::IsUnknown(_, not, _) => {
            if not.is_some() {
                Ok(spec::Expr::IsNotUnknown(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsUnknown(Box::new(expr)))
            }
        }
        ExprPredicate::IsNull(_, not, _) => {
            if not.is_some() {
                Ok(spec::Expr::IsNotNull(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsNull(Box::new(expr)))
            }
        }
        ExprPredicate::IsDistinctFrom(_, not, _, _, other) => {
            let other = from_ast_expression(*other)?;
            if not.is_some() {
                Ok(spec::Expr::IsNotDistinctFrom {
                    left: Box::new(expr),
                    right: Box::new(other),
                })
            } else {
                Ok(spec::Expr::IsDistinctFrom {
                    left: Box::new(expr),
                    right: Box::new(other),
                })
            }
        }
        ExprPredicate::InList(not, _, _, list, _) => Ok(spec::Expr::InList {
            expr: Box::new(expr),
            list: list
                .into_items()
                .map(from_ast_expression)
                .collect::<SqlResult<Vec<_>>>()?,
            negated: not.is_some(),
        }),
        ExprPredicate::InSubquery(not, _, _, query, _) => Ok(spec::Expr::InSubquery {
            expr: Box::new(expr),
            subquery: Box::new(from_ast_query(query)?),
            negated: not.is_some(),
        }),
        ExprPredicate::Between(not, _, low, _, high) => Ok(spec::Expr::Between {
            expr: Box::new(expr),
            negated: not.is_some(),
            low: Box::new(from_ast_expression(*low)?),
            high: Box::new(from_ast_expression(*high)?),
        }),
        ExprPredicate::Like(not, _, quantifier, pattern, escape) => {
            if quantifier.is_some() {
                return Err(SqlError::unsupported("LIKE quantifier"));
            }
            let mut args = vec![expr, from_ast_expression(*pattern)?];
            if let Some(PatternEscape { escape: _, value }) = escape {
                args.push(LiteralValue(value.value).try_into()?);
            };
            let expr = spec::Expr::from(Function {
                name: "like".to_string(),
                args,
            });
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        ExprPredicate::ILike(not, _, quantifier, pattern, escape) => {
            if quantifier.is_some() {
                return Err(SqlError::unsupported("ILIKE quantifier"));
            }
            let mut args = vec![expr, from_ast_expression(*pattern)?];
            if let Some(PatternEscape { escape: _, value }) = escape {
                args.push(LiteralValue(value.value).try_into()?);
            };
            let expr = spec::Expr::from(Function {
                name: "ilike".to_string(),
                args,
            });
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        ExprPredicate::RLike(not, _, pattern) => {
            let expr = spec::Expr::from(Function {
                name: "rlike".to_string(),
                args: vec![expr, from_ast_expression(*pattern)?],
            });
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        ExprPredicate::SimilarTo(not, _, _, pattern, escape) => {
            let escape_char = if let Some(PatternEscape { escape: _, value }) = escape {
                let mut chars = value.value.chars();
                match (chars.next(), chars.next()) {
                    (Some(x), None) => Some(x),
                    _ => {
                        return Err(SqlError::invalid(
                            "invalid escape character in SIMILAR TO expression",
                        ))
                    }
                }
            } else {
                None
            };
            Ok(spec::Expr::SimilarTo {
                expr: Box::new(expr),
                pattern: Box::new(from_ast_expression(*pattern)?),
                negated: not.is_some(),
                escape_char,
                case_insensitive: false,
            })
        }
    }
}
