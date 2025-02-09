use std::iter::once;

use sail_common::spec;
use sail_sql_parser::ast::expression::{
    AtomExpr, BinaryOperator, CaseElse, CaseWhen, DuplicateTreatment, Expr, ExprList, ExprModifier,
    ExprPredicate, FunctionArgument, FunctionExpr, LambdaFunctionParameters, OrderBy, OrderByExpr,
    OrderDirection, OrderNulls, OverClause, PartitionBy, PatternEscape, TrimExpr, UnaryOperator,
    WindowFrame, WindowFrameBound, WindowSpec,
};
use sail_sql_parser::ast::identifier::ObjectName;
use sail_sql_parser::ast::query::IdentList;

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::literal::{parse_date_string, parse_timestamp_string, LiteralValue, Signed};
use crate::query::{from_ast_named_expression, from_ast_query};
use crate::value::{from_ast_boolean_literal, from_ast_number_literal, from_ast_string_literal};

fn negated(expr: spec::Expr) -> spec::Expr {
    spec::Expr::UnresolvedFunction {
        function_name: "not".to_string(),
        arguments: vec![expr],
        is_distinct: false,
        is_user_defined_function: false,
    }
}

pub(crate) fn from_ast_function_argument(arg: FunctionArgument) -> SqlResult<spec::Expr> {
    match arg {
        FunctionArgument::Unnamed(arg) => from_ast_expression(arg),
        FunctionArgument::Named(_, _, _) => Err(SqlError::todo("named function arguments")),
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

fn from_ast_expression_list(expr: ExprList) -> SqlResult<Vec<spec::Expr>> {
    let ExprList {
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
        Expr::Atom(atom) => from_ast_atom_expression(atom),
        Expr::Modifier(expr, modifier) => from_ast_expression_modifier(*expr, modifier),
        Expr::UnaryOperator(op, expr) => Ok(spec::Expr::UnresolvedFunction {
            function_name: from_ast_unary_operator(op)?,
            arguments: vec![from_ast_expression(*expr)?],
            is_distinct: false,
            is_user_defined_function: false,
        }),
        Expr::BinaryOperator(left, op, right) => {
            let op = from_ast_binary_operator(op)?;
            Ok(spec::Expr::UnresolvedFunction {
                function_name: op,
                arguments: vec![from_ast_expression(*left)?, from_ast_expression(*right)?],
                is_distinct: false,
                is_user_defined_function: false,
            })
        }
        Expr::Predicate(expr, predicate) => from_ast_expression_predicate(*expr, predicate),
    }
}

pub(crate) fn from_ast_atom_expression(atom: AtomExpr) -> SqlResult<spec::Expr> {
    match atom {
        AtomExpr::SubqueryExpr(_, query, _) => Ok(spec::Expr::ScalarSubquery {
            subquery: Box::new(from_ast_query(query)?),
        }),
        AtomExpr::ExistsExpr(_, _, query, _) => Ok(spec::Expr::Exists {
            subquery: Box::new(from_ast_query(query)?),
            negated: false,
        }),
        AtomExpr::LambdaFunction {
            params,
            arrow: _,
            body,
        } => {
            let arguments = match params {
                LambdaFunctionParameters::Single(ident) => vec![ident],
                LambdaFunctionParameters::Multiple(_, idents, _) => idents.into_items().collect(),
            };
            let arguments = arguments
                .into_iter()
                .map(|arg| spec::UnresolvedNamedLambdaVariable {
                    name: spec::ObjectName::new_unqualified(arg.value.into()),
                })
                .collect();
            let function = from_ast_expression(*body)?;
            Ok(spec::Expr::LambdaFunction {
                arguments,
                function: Box::new(function),
            })
        }
        AtomExpr::Nested(_, e, _) => from_ast_expression(*e),
        AtomExpr::Tuple(_, expressions, _) => {
            let arguments = expressions
                .into_items()
                .map(from_ast_named_expression)
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::UnresolvedFunction {
                function_name: "named_struct".to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            })
        }
        AtomExpr::Struct(_, _, expressions, _) => {
            let arguments = expressions
                .into_items()
                .map(from_ast_named_expression)
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::UnresolvedFunction {
                function_name: "struct".to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            })
        }
        AtomExpr::Case {
            case: _,
            operand,
            conditions: (head, tail),
            r#else,
            end: _,
        } => {
            let operand = operand.map(|x| from_ast_expression(*x)).transpose()?;
            let mut arguments = vec![];
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
                        spec::Expr::UnresolvedFunction {
                            function_name: "==".to_string(),
                            arguments: vec![operand.clone(), condition],
                            is_distinct: false,
                            is_user_defined_function: false,
                        }
                    } else {
                        condition
                    };
                    arguments.push(condition);
                    arguments.push(from_ast_expression(*result)?);
                    Ok(())
                })?;
            if let Some(r#else) = r#else {
                let CaseElse { r#else: _, result } = r#else;
                arguments.push(from_ast_expression(*result)?);
            }
            Ok(spec::Expr::UnresolvedFunction {
                function_name: "when".to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            })
        }
        AtomExpr::GroupingSets(_, _, _, expr, _) => {
            let expr = expr
                .into_items()
                .map(from_ast_expression_list)
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::GroupingSets(expr))
        }
        AtomExpr::Cube(_, expr) => {
            let expr = from_ast_expression_list(expr)?;
            Ok(spec::Expr::Cube(expr))
        }
        AtomExpr::Rollup(_, expr) => {
            let expr = from_ast_expression_list(expr)?;
            Ok(spec::Expr::Rollup(expr))
        }
        AtomExpr::Cast(_, _, expr, _, data_type, _) => Ok(spec::Expr::Cast {
            expr: Box::new(from_ast_expression(*expr)?),
            cast_to_type: from_ast_data_type(data_type)?,
        }),
        AtomExpr::Extract(_, _, ident, _, expr, _) => Ok(spec::Expr::UnresolvedFunction {
            function_name: "extract".to_string(),
            arguments: vec![
                LiteralValue(ident.value).try_into()?,
                from_ast_expression(*expr)?,
            ],
            is_distinct: false,
            is_user_defined_function: false,
        }),
        AtomExpr::Substring(_, _, expr, r#from, r#for, _) => {
            let mut arguments = vec![from_ast_expression(*expr)?];
            if let Some((_, pos)) = r#from {
                arguments.push(from_ast_expression(*pos)?);
            }
            if let Some((_, len)) = r#for {
                arguments.push(from_ast_expression(*len)?);
            }
            Ok(spec::Expr::UnresolvedFunction {
                function_name: "substring".to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            })
        }
        AtomExpr::Trim(_, _, trim, _) => {
            let (name, arguments) = match trim {
                TrimExpr::LeadingSpace(_, _, e) => ("ltrim", vec![from_ast_expression(*e)?]),
                TrimExpr::Leading(_, what, _, e) => (
                    "ltrim",
                    vec![from_ast_expression(*e)?, from_ast_expression(*what)?],
                ),
                TrimExpr::TrailingSpace(_, _, e) => ("rtrim", vec![from_ast_expression(*e)?]),
                TrimExpr::Trailing(_, what, _, e) => (
                    "rtrim",
                    vec![from_ast_expression(*e)?, from_ast_expression(*what)?],
                ),
                TrimExpr::BothSpace(_, _, e) => ("trim", vec![from_ast_expression(*e)?]),
                TrimExpr::Both(_, what, _, e) => (
                    "trim",
                    vec![from_ast_expression(*e)?, from_ast_expression(*what)?],
                ),
            };
            Ok(spec::Expr::UnresolvedFunction {
                function_name: name.to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            })
        }
        AtomExpr::Overlay(_, _, e, _, what, _, pos, r#for, _) => {
            let mut arguments = vec![
                from_ast_expression(*e)?,
                from_ast_expression(*what)?,
                from_ast_expression(*pos)?,
            ];
            if let Some((_, len)) = r#for {
                arguments.push(from_ast_expression(*len)?);
            }
            Ok(spec::Expr::UnresolvedFunction {
                function_name: "overlay".to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            })
        }
        AtomExpr::Position(_, _, what, _, e, _) => Ok(spec::Expr::UnresolvedFunction {
            function_name: "strpos".to_string(),
            arguments: vec![from_ast_expression(*e)?, from_ast_expression(*what)?],
            is_distinct: false,
            is_user_defined_function: false,
        }),
        AtomExpr::CurrentTimestamp(_) => Ok(spec::Expr::UnresolvedFunction {
            function_name: "current_timestamp".to_string(),
            arguments: vec![],
            is_distinct: false,
            is_user_defined_function: false,
        }),
        AtomExpr::CurrentDate(_) => Ok(spec::Expr::UnresolvedFunction {
            function_name: "current_date".to_string(),
            arguments: vec![],
            is_distinct: false,
            is_user_defined_function: false,
        }),
        AtomExpr::Timestamp(_, _, value, _) | AtomExpr::TimestampLiteral(_, value) => {
            Ok(spec::Expr::Literal(parse_timestamp_string(&value.value)?))
        }
        AtomExpr::Date(_, _, value, _) | AtomExpr::DateLiteral(_, value) => {
            Ok(spec::Expr::Literal(parse_date_string(&value.value)?))
        }
        AtomExpr::Function(function) => {
            let FunctionExpr {
                name: ObjectName(name),
                left: _,
                duplicate_treatment,
                arguments,
                right: _,
                over_clause,
            } = function;
            if !name.tail.is_empty() {
                return Err(SqlError::unsupported("qualified function name"));
            }
            let function_name = name.head.value;
            let is_distinct = match duplicate_treatment {
                Some(DuplicateTreatment::All(_)) | None => false,
                Some(DuplicateTreatment::Distinct(_)) => true,
            };
            let arguments = arguments
                .map(|x| {
                    x.into_items()
                        .map(from_ast_function_argument)
                        .collect::<SqlResult<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default();
            if let Some(over_clause) = over_clause {
                let OverClause { over: _, window } = over_clause;
                match window {
                    WindowSpec::Named(_) => Err(SqlError::unsupported("named window function")),
                    WindowSpec::Detailed {
                        left: _,
                        partition_by,
                        order_by,
                        window_frame,
                        right: _,
                    } => {
                        let partition_spec = partition_by
                            .map(|x| {
                                let PartitionBy {
                                    partition: _,
                                    by: _,
                                    columns: partition_by,
                                } = x;
                                partition_by
                                    .into_items()
                                    .map(from_ast_expression)
                                    .collect::<SqlResult<Vec<_>>>()
                            })
                            .transpose()?
                            .unwrap_or_default();
                        let order_spec = order_by
                            .map(|x| {
                                let OrderBy {
                                    order: _,
                                    by: _,
                                    expressions: order_by,
                                } = x;
                                order_by
                                    .into_items()
                                    .map(from_ast_order_by)
                                    .collect::<SqlResult<Vec<_>>>()
                            })
                            .transpose()?
                            .unwrap_or_default();
                        let frame_spec = window_frame.map(from_ast_window_frame).transpose()?;
                        let function = spec::Expr::UnresolvedFunction {
                            function_name,
                            arguments,
                            is_distinct,
                            is_user_defined_function: false,
                        };
                        Ok(spec::Expr::Window {
                            window_function: Box::new(function),
                            partition_spec,
                            order_spec,
                            frame_spec,
                        })
                    }
                }
            } else {
                Ok(spec::Expr::UnresolvedFunction {
                    function_name,
                    arguments,
                    is_distinct,
                    is_user_defined_function: false,
                })
            }
        }
        AtomExpr::Wildcard(_) => Ok(spec::Expr::UnresolvedStar {
            target: None,
            wildcard_options: Default::default(),
        }),
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

pub(crate) fn from_ast_expression_modifier(
    expr: Expr,
    modifier: ExprModifier,
) -> SqlResult<spec::Expr> {
    let expr = from_ast_expression(expr)?;
    match modifier {
        ExprModifier::Wildcard(_, _) => match expr {
            spec::Expr::UnresolvedAttribute {
                name,
                plan_id: None,
            } => Ok(spec::Expr::UnresolvedStar {
                target: Some(name),
                wildcard_options: Default::default(),
            }),
            _ => Err(SqlError::invalid("wildcard qualifier")),
        },
        ExprModifier::FieldAccess(_, field) => match expr {
            spec::Expr::UnresolvedAttribute { name, plan_id } => {
                Ok(spec::Expr::UnresolvedAttribute {
                    name: name.child(field.value.into()),
                    plan_id,
                })
            }
            _ => Ok(spec::Expr::UnresolvedExtractValue {
                child: Box::new(expr),
                extraction: Box::new(spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::new_unqualified(field.value.into()),
                    plan_id: None,
                }),
            }),
        },
        ExprModifier::SubscriptAccess(_, subscript, _) => Ok(spec::Expr::UnresolvedExtractValue {
            child: Box::new(expr),
            extraction: Box::new(from_ast_expression(*subscript)?),
        }),
        ExprModifier::Cast(_, data_type) => Ok(spec::Expr::Cast {
            expr: Box::new(expr),
            cast_to_type: from_ast_data_type(data_type)?,
        }),
    }
}

pub(crate) fn from_ast_expression_predicate(
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
            let mut arguments = vec![expr, from_ast_expression(*pattern)?];
            if let Some(PatternEscape { escape: _, value }) = escape {
                arguments.push(LiteralValue(value.value).try_into()?);
            };
            let expr = spec::Expr::UnresolvedFunction {
                function_name: "like".to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            };
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
            let mut arguments = vec![expr, from_ast_expression(*pattern)?];
            if let Some(PatternEscape { escape: _, value }) = escape {
                arguments.push(LiteralValue(value.value).try_into()?);
            };
            let expr = spec::Expr::UnresolvedFunction {
                function_name: "ilike".to_string(),
                arguments,
                is_distinct: false,
                is_user_defined_function: false,
            };
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        ExprPredicate::RLike(not, _, pattern) => {
            let expr = spec::Expr::UnresolvedFunction {
                function_name: "rlike".to_string(),
                arguments: vec![expr, from_ast_expression(*pattern)?],
                is_distinct: false,
                is_user_defined_function: false,
            };
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

pub(crate) fn from_ast_identifier_list(identifiers: IdentList) -> SqlResult<Vec<spec::Identifier>> {
    let IdentList {
        left: _,
        columns,
        right: _,
    } = identifiers;
    Ok(columns.into_items().map(|x| x.value.into()).collect())
}
