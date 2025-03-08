use std::iter::once;

use sail_common::spec;
use sail_sql_parser::ast::expression::{
    AtomExpr, BinaryOperator, CaseElse, CaseWhen, DuplicateTreatment, Expr, FilterClause,
    FunctionArgument, FunctionArgumentList, FunctionExpr, GroupingExpr, GroupingSet,
    LambdaFunctionParameters, NullTreatment, OrderByExpr, OrderDirection, OrderNulls, OverClause,
    PatternEscape, PatternQuantifier, TableExpr, TrimExpr, UnaryOperator, WindowFrame,
    WindowFrameBound, WindowModifier, WindowSpec, WithinGroupClause,
};
use sail_sql_parser::ast::identifier::{ObjectName, QualifiedWildcard};
use sail_sql_parser::ast::query::{
    ClusterByClause, DistributeByClause, IdentList, NamedExpr, OrderByClause, PartitionByClause,
    SortByClause,
};

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::literal::{parse_date_string, parse_timestamp_string, LiteralValue, Signed};
use crate::query::{from_ast_named_expression, from_ast_query};
use crate::value::{
    from_ast_boolean_literal, from_ast_number_literal, from_ast_string, from_ast_string_literal,
};

#[derive(Default)]
struct WindowModifiers {
    cluster_by: Option<Vec<Expr>>,
    partition_by: Option<Vec<Expr>>,
    order_by: Option<Vec<OrderByExpr>>,
}

impl TryFrom<Vec<WindowModifier>> for WindowModifiers {
    type Error = SqlError;

    fn try_from(value: Vec<WindowModifier>) -> SqlResult<Self> {
        let mut output = Self::default();
        for modifier in value {
            match modifier {
                WindowModifier::ClusterBy(ClusterByClause {
                    cluster_by: _,
                    items,
                }) => {
                    if output
                        .cluster_by
                        .replace(items.into_items().collect())
                        .is_some()
                    {
                        return Err(SqlError::invalid("duplicated CLUSTER BY clause"));
                    }
                }
                WindowModifier::PartitionBy(PartitionByClause {
                    partition_by: _,
                    items,
                })
                | WindowModifier::DistributeBy(DistributeByClause {
                    distribute_by: _,
                    items,
                }) => {
                    if output
                        .partition_by
                        .replace(items.into_items().collect())
                        .is_some()
                    {
                        return Err(SqlError::invalid(
                            "duplicated PARTITION BY or DISTRIBUTED BY clause",
                        ));
                    }
                }
                WindowModifier::OrderBy(OrderByClause { order_by: _, items })
                | WindowModifier::SortBy(SortByClause { sort_by: _, items }) => {
                    if output
                        .order_by
                        .replace(items.into_items().collect())
                        .is_some()
                    {
                        return Err(SqlError::invalid("duplicated ORDER BY or SORT BY clause"));
                    }
                }
            }
        }
        Ok(output)
    }
}
fn negated(expr: spec::Expr) -> spec::Expr {
    spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
        function_name: spec::ObjectName::bare("not"),
        arguments: vec![expr],
        named_arguments: vec![],
        is_distinct: false,
        is_user_defined_function: false,
        ignore_nulls: None,
        filter: None,
        order_by: None,
    })
}

#[allow(clippy::type_complexity)]
pub(crate) fn from_ast_function_arguments(
    args: impl IntoIterator<Item = FunctionArgument>,
) -> SqlResult<(Vec<spec::Expr>, Vec<(spec::Identifier, spec::Expr)>)> {
    let mut arguments = vec![];
    let mut named_arguments = vec![];
    for arg in args {
        match arg {
            FunctionArgument::Named(name, _, expr) => {
                let expr = from_ast_expression(expr)?;
                named_arguments.push((name.value.into(), expr));
            }
            FunctionArgument::Unnamed(expr) => {
                if !named_arguments.is_empty() {
                    return Err(SqlError::invalid(
                        "positional argument after named argument",
                    ));
                }
                let expr = from_ast_expression(expr)?;
                arguments.push(expr);
            }
        }
    }
    Ok((arguments, named_arguments))
}

pub fn from_ast_object_name(name: ObjectName) -> SqlResult<spec::ObjectName> {
    let ObjectName(parts) = name;
    Ok(parts
        .into_items()
        .map(|i| i.value)
        .collect::<Vec<_>>()
        .into())
}

pub fn from_ast_qualified_wildcard(wildcard: QualifiedWildcard) -> SqlResult<spec::ObjectName> {
    let QualifiedWildcard(qualifier, _, _) = wildcard;
    Ok(qualifier
        .into_items()
        .map(|x| x.value)
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
        BinaryOperator::NotGt(_) => Ok("<=".to_string()),
        BinaryOperator::NotLt(_) => Ok(">=".to_string()),
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
        WindowFrameBound::UnboundedPreceding(_, _) => {
            Ok(spec::WindowFrameBoundary::UnboundedPreceding)
        }
        WindowFrameBound::UnboundedFollowing(_, _) => {
            Ok(spec::WindowFrameBoundary::UnboundedFollowing)
        }
        WindowFrameBound::Preceding(e, _) => Ok(spec::WindowFrameBoundary::Preceding(Box::new(
            from_ast_expression(e)?,
        ))),
        WindowFrameBound::Following(e, _) => Ok(spec::WindowFrameBoundary::Following(Box::new(
            from_ast_expression(e)?,
        ))),
    }
}

pub fn from_ast_expression(expr: Expr) -> SqlResult<spec::Expr> {
    match expr {
        Expr::Atom(atom) => from_ast_atom_expression(atom),
        Expr::UnaryOperator(op, expr) => {
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(from_ast_unary_operator(op)?),
                arguments: vec![from_ast_expression(*expr)?],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        Expr::BinaryOperator(left, op, right) => {
            let op = from_ast_binary_operator(op)?;
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(op),
                arguments: vec![from_ast_expression(*left)?, from_ast_expression(*right)?],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        Expr::Wildcard(expr, _, _) => {
            let expr = from_ast_expression(*expr)?;
            match expr {
                spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id: None,
                } => Ok(spec::Expr::UnresolvedStar {
                    target: Some(name),
                    wildcard_options: Default::default(),
                }),
                _ => Err(SqlError::invalid("wildcard qualifier")),
            }
        }
        Expr::Field(expr, _, field) => {
            let expr = from_ast_expression(*expr)?;
            match expr {
                spec::Expr::UnresolvedAttribute { name, plan_id } => {
                    Ok(spec::Expr::UnresolvedAttribute {
                        name: name.child(field.value),
                        plan_id,
                    })
                }
                _ => Ok(spec::Expr::UnresolvedExtractValue {
                    child: Box::new(expr),
                    extraction: Box::new(spec::Expr::UnresolvedAttribute {
                        name: spec::ObjectName::bare(field.value),
                        plan_id: None,
                    }),
                }),
            }
        }
        Expr::Subscript(expr, _, subscript, _) => {
            let expr = from_ast_expression(*expr)?;
            Ok(spec::Expr::UnresolvedExtractValue {
                child: Box::new(expr),
                extraction: Box::new(from_ast_expression(*subscript)?),
            })
        }
        Expr::Cast(expr, _, data_type) => {
            let expr = from_ast_expression(*expr)?;
            Ok(spec::Expr::Cast {
                expr: Box::new(expr),
                cast_to_type: from_ast_data_type(data_type)?,
                rename: false,
            })
        }
        Expr::IsFalse(expr, _, not, _) => {
            let expr = from_ast_expression(*expr)?;
            if not.is_some() {
                Ok(spec::Expr::IsNotFalse(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsFalse(Box::new(expr)))
            }
        }
        Expr::IsTrue(expr, _, not, _) => {
            let expr = from_ast_expression(*expr)?;
            if not.is_some() {
                Ok(spec::Expr::IsNotTrue(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsTrue(Box::new(expr)))
            }
        }
        Expr::IsUnknown(expr, _, not, _) => {
            let expr = from_ast_expression(*expr)?;
            if not.is_some() {
                Ok(spec::Expr::IsNotUnknown(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsUnknown(Box::new(expr)))
            }
        }
        Expr::IsNull(expr, _, not, _) => {
            let expr = from_ast_expression(*expr)?;
            if not.is_some() {
                Ok(spec::Expr::IsNotNull(Box::new(expr)))
            } else {
                Ok(spec::Expr::IsNull(Box::new(expr)))
            }
        }
        Expr::IsDistinctFrom(expr, _, not, _, _, other) => {
            let expr = from_ast_expression(*expr)?;
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
        Expr::InList(expr, not, _, _, list, _) => {
            let expr = from_ast_expression(*expr)?;
            Ok(spec::Expr::InList {
                expr: Box::new(expr),
                list: list
                    .into_items()
                    .map(from_ast_expression)
                    .collect::<SqlResult<Vec<_>>>()?,
                negated: not.is_some(),
            })
        }
        Expr::InSubquery(expr, not, _, _, query, _) => {
            let expr = from_ast_expression(*expr)?;
            Ok(spec::Expr::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(from_ast_query(query)?),
                negated: not.is_some(),
            })
        }
        Expr::Between(expr, not, _, low, _, high) => {
            let expr = from_ast_expression(*expr)?;
            Ok(spec::Expr::Between {
                expr: Box::new(expr),
                negated: not.is_some(),
                low: Box::new(from_ast_expression(*low)?),
                high: Box::new(from_ast_expression(*high)?),
            })
        }
        Expr::Like(expr, not, _, quantifier, pattern, escape) => {
            let expr = from_ast_expression(*expr)?;
            let pattern = from_ast_quantified_pattern(quantifier, *pattern)?;
            let mut arguments = vec![expr, pattern];
            if let Some(escape) = from_ast_pattern_escape_string(escape)? {
                arguments.push(LiteralValue(escape.to_string()).try_into()?);
            };
            let expr = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("like"),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            });
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        Expr::ILike(expr, not, _, quantifier, pattern, escape) => {
            let expr = from_ast_expression(*expr)?;
            let pattern = from_ast_quantified_pattern(quantifier, *pattern)?;
            let mut arguments = vec![expr, pattern];
            if let Some(escape) = from_ast_pattern_escape_string(escape)? {
                arguments.push(LiteralValue(escape.to_string()).try_into()?);
            };
            let expr = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("ilike"),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            });
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        Expr::RLike(expr, not, _, pattern) => {
            let expr = from_ast_expression(*expr)?;
            let pattern = from_ast_expression(*pattern)?;
            let expr = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("rlike"),
                arguments: vec![expr, pattern],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            });
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        Expr::RegExp(expr, not, _, pattern) => {
            let expr = from_ast_expression(*expr)?;
            let pattern = from_ast_expression(*pattern)?;
            let expr = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("regexp"),
                arguments: vec![expr, pattern],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            });
            if not.is_some() {
                Ok(negated(expr))
            } else {
                Ok(expr)
            }
        }
        Expr::SimilarTo(expr, not, _, _, pattern, escape) => {
            let expr = from_ast_expression(*expr)?;
            let escape_char = from_ast_pattern_escape_string(escape)?;
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

fn from_ast_atom_expression(atom: AtomExpr) -> SqlResult<spec::Expr> {
    match atom {
        AtomExpr::Subquery(_, query, _) => Ok(spec::Expr::ScalarSubquery {
            subquery: Box::new(from_ast_query(query)?),
        }),
        AtomExpr::Exists(_, _, query, _) => Ok(spec::Expr::Exists {
            subquery: Box::new(from_ast_query(query)?),
            negated: false,
        }),
        AtomExpr::Table(_, expr) => {
            let expr = match expr {
                TableExpr::Name(x) | TableExpr::NestedName(_, x, _) => {
                    spec::Expr::UnresolvedAttribute {
                        name: from_ast_object_name(x)?,
                        plan_id: None,
                    }
                }
                TableExpr::Query(_, query, _) => spec::Expr::ScalarSubquery {
                    subquery: Box::new(from_ast_query(query)?),
                },
            };
            Ok(spec::Expr::Table {
                expr: Box::new(expr),
            })
        }
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
                    name: spec::ObjectName::bare(arg.value),
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
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("struct"),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::Struct(_, _, expressions, _) => {
            let arguments = expressions
                .into_items()
                .map(from_ast_named_expression)
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("struct"),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::Case {
            case: _,
            operand,
            first_condition,
            other_conditions,
            r#else,
            end: _,
        } => {
            let operand = operand.map(|x| from_ast_expression(*x)).transpose()?;
            let mut arguments = vec![];
            once(*first_condition)
                .chain(other_conditions.into_iter())
                .try_for_each::<_, SqlResult<_>>(|x| {
                    let CaseWhen {
                        when: _,
                        condition,
                        then: _,
                        result,
                    } = x;
                    let condition = from_ast_expression(condition)?;
                    let condition = if let Some(ref operand) = operand {
                        spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                            function_name: spec::ObjectName::bare("=="),
                            arguments: vec![operand.clone(), condition],
                            named_arguments: vec![],
                            is_distinct: false,
                            is_user_defined_function: false,
                            ignore_nulls: None,
                            filter: None,
                            order_by: None,
                        })
                    } else {
                        condition
                    };
                    arguments.push(condition);
                    arguments.push(from_ast_expression(result)?);
                    Ok(())
                })?;
            if let Some(r#else) = r#else {
                let CaseElse { r#else: _, result } = *r#else;
                arguments.push(from_ast_expression(result)?);
            }
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("when"),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::Cast(_, _, expr, _, data_type, _) => Ok(spec::Expr::Cast {
            expr: Box::new(from_ast_expression(*expr)?),
            cast_to_type: from_ast_data_type(data_type)?,
            rename: true,
        }),
        AtomExpr::Extract(_, _, ident, _, expr, _) => {
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("extract"),
                arguments: vec![
                    LiteralValue(ident.value).try_into()?,
                    from_ast_expression(*expr)?,
                ],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::Substring(_, _, expr, r#from, r#for, _) => {
            let mut arguments = vec![from_ast_expression(*expr)?];
            if let Some((_, pos)) = r#from {
                arguments.push(from_ast_expression(*pos)?);
            }
            if let Some((_, len)) = r#for {
                arguments.push(from_ast_expression(*len)?);
            }
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("substring"),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::Trim(_, _, trim, _) => {
            let (name, arguments) = match *trim {
                TrimExpr::LeadingSpace(_, _, e) => ("ltrim", vec![from_ast_expression(e)?]),
                TrimExpr::Leading(_, what, _, e) => (
                    "ltrim",
                    vec![from_ast_expression(e)?, from_ast_expression(what)?],
                ),
                TrimExpr::TrailingSpace(_, _, e) => ("rtrim", vec![from_ast_expression(e)?]),
                TrimExpr::Trailing(_, what, _, e) => (
                    "rtrim",
                    vec![from_ast_expression(e)?, from_ast_expression(what)?],
                ),
                TrimExpr::BothSpace(_, _, e) => ("trim", vec![from_ast_expression(e)?]),
                TrimExpr::Both(_, what, _, e) => (
                    "trim",
                    vec![from_ast_expression(e)?, from_ast_expression(what)?],
                ),
            };
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(name),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
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
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("overlay"),
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::Position(_, _, what, _, e, _) => {
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("strpos"),
                arguments: vec![from_ast_expression(*e)?, from_ast_expression(*what)?],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::CurrentUser(_, _) => {
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("current_user"),
                arguments: vec![],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::CurrentTimestamp(_, _) => {
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("current_timestamp"),
                arguments: vec![],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::CurrentDate(_, _) => {
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare("current_date"),
                arguments: vec![],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        AtomExpr::Timestamp(_, _, value, _)
        | AtomExpr::TimestampLiteral(_, value)
        | AtomExpr::TimestampLtzLiteral(_, value)
        | AtomExpr::TimestampNtzLiteral(_, value) => {
            // FIXME: timezone information is lost
            Ok(spec::Expr::Literal(parse_timestamp_string(
                &from_ast_string(value)?,
            )?))
        }
        AtomExpr::Date(_, _, value, _) | AtomExpr::DateLiteral(_, value) => Ok(
            spec::Expr::Literal(parse_date_string(&from_ast_string(value)?)?),
        ),
        AtomExpr::Function(function) => {
            let FunctionExpr {
                name,
                arguments,
                null_treatment,
                within_group,
                filter,
                over_clause,
            } = *function;
            let FunctionArgumentList {
                left: _,
                duplicate_treatment,
                arguments,
                null_treatment: inner_null_treatment,
                right: _,
            } = arguments;
            let function_name = from_ast_object_name(name)?;
            let (arguments, named_arguments) = arguments
                .map(|x| from_ast_function_arguments(x.into_items()))
                .transpose()?
                .unwrap_or_default();
            let is_distinct = match duplicate_treatment {
                Some(DuplicateTreatment::All(_)) | None => false,
                Some(DuplicateTreatment::Distinct(_)) => true,
            };
            let null_treatment = match (inner_null_treatment, null_treatment) {
                (Some(x), None) => Some(x),
                (None, Some(x)) => Some(x),
                (Some(_), Some(_)) => return Err(SqlError::invalid(
                    "conflicting null treatment clause inside and outside function argument list",
                )),
                (None, None) => None,
            };
            let ignore_nulls = match null_treatment {
                Some(NullTreatment::IgnoreNulls(_, _)) => Some(true),
                Some(NullTreatment::RespectNulls(_, _)) => Some(false),
                None => None,
            };
            let filter = filter
                .map(|x| {
                    let FilterClause {
                        filter: _,
                        left: _,
                        r#where: _,
                        condition,
                        right: _,
                    } = x;
                    from_ast_expression(condition).map(Box::new)
                })
                .transpose()?;
            let order_by = within_group
                .map(|x| {
                    let WithinGroupClause {
                        within_group: _,
                        left: _,
                        order_by: _,
                        expressions,
                        right: _,
                    } = x;
                    expressions
                        .into_items()
                        .map(from_ast_order_by)
                        .collect::<SqlResult<Vec<_>>>()
                })
                .transpose()?;
            let function = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name,
                arguments,
                named_arguments,
                is_distinct,
                is_user_defined_function: false,
                ignore_nulls,
                filter,
                order_by,
            });
            if let Some(over_clause) = over_clause {
                let OverClause { over: _, window } = over_clause;
                let window = match window {
                    WindowSpec::Named(x) => spec::Window::Named(x.value.into()),
                    WindowSpec::Unnamed {
                        left: _,
                        modifiers,
                        window_frame,
                        right: _,
                    } => {
                        let WindowModifiers {
                            cluster_by,
                            partition_by,
                            order_by,
                        } = modifiers.try_into()?;
                        let cluster_by = cluster_by
                            .unwrap_or_default()
                            .into_iter()
                            .map(from_ast_expression)
                            .collect::<SqlResult<Vec<_>>>()?;
                        let partition_by = partition_by
                            .unwrap_or_default()
                            .into_iter()
                            .map(from_ast_expression)
                            .collect::<SqlResult<Vec<_>>>()?;
                        let order_by = order_by
                            .unwrap_or_default()
                            .into_iter()
                            .map(from_ast_order_by)
                            .collect::<SqlResult<Vec<_>>>()?;
                        let frame = window_frame.map(from_ast_window_frame).transpose()?;
                        spec::Window::Unnamed {
                            cluster_by,
                            partition_by,
                            order_by,
                            frame,
                        }
                    }
                };
                Ok(spec::Expr::Window {
                    window_function: Box::new(function),
                    window,
                })
            } else {
                Ok(function)
            }
        }
        AtomExpr::Wildcard(_) => Ok(spec::Expr::UnresolvedStar {
            target: None,
            wildcard_options: Default::default(),
        }),
        AtomExpr::StringLiteral(head, tail) => from_ast_string_literal(head, tail),
        AtomExpr::NumberLiteral(value) => from_ast_number_literal(value),
        AtomExpr::BooleanLiteral(value) => from_ast_boolean_literal(value),
        AtomExpr::Null(_) => Ok(spec::Expr::Literal(spec::Literal::Null)),
        AtomExpr::Interval(_, value) => Ok(spec::Expr::Literal(spec::Literal::try_from(
            LiteralValue(Signed::Positive(*value)),
        )?)),
        AtomExpr::Placeholder(variable) => Ok(spec::Expr::Placeholder(variable.value)),
        AtomExpr::Identifier(x) => Ok(spec::Expr::UnresolvedAttribute {
            name: spec::ObjectName::bare(x.value),
            plan_id: None,
        }),
    }
}

pub(crate) fn from_ast_grouping_expression(expr: GroupingExpr) -> SqlResult<spec::Expr> {
    match expr {
        GroupingExpr::GroupingSets(_, _, _, grouping, _) => {
            let expr = grouping
                .into_items()
                .map(from_ast_grouping_set)
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::GroupingSets(expr))
        }
        GroupingExpr::Cube(_, grouping) => {
            let expr = from_ast_grouping_set(grouping)?;
            Ok(spec::Expr::Cube(expr))
        }
        GroupingExpr::Rollup(_, grouping) => {
            let expr = from_ast_grouping_set(grouping)?;
            Ok(spec::Expr::Rollup(expr))
        }
        GroupingExpr::Default(expr) => from_ast_expression(expr),
    }
}

fn from_ast_grouping_set(grouping: GroupingSet) -> SqlResult<Vec<spec::Expr>> {
    let GroupingSet {
        left: _,
        expressions,
        right: _,
    } = grouping;
    Ok(expressions
        .map(|x| {
            x.into_items()
                .map(from_ast_expression)
                .collect::<SqlResult<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default())
}

pub(crate) fn from_ast_identifier_list(identifiers: IdentList) -> SqlResult<Vec<spec::Identifier>> {
    let IdentList {
        left: _,
        names,
        right: _,
    } = identifiers;
    Ok(names.into_items().map(|x| x.value.into()).collect())
}

fn from_ast_quantified_pattern(
    quantifier: Option<PatternQuantifier>,
    pattern: Expr,
) -> SqlResult<spec::Expr> {
    let Some(quantifier) = quantifier else {
        return from_ast_expression(pattern);
    };
    let quantifier = match quantifier {
        PatternQuantifier::All(_) => "all",
        PatternQuantifier::Any(_) => "any",
        PatternQuantifier::Some(_) => "some",
    };
    let Expr::Atom(AtomExpr::Tuple(_, arguments, _)) = pattern else {
        return Err(SqlError::invalid("quantified pattern expression"));
    };
    let arguments = arguments
        .into_items()
        .map(|x| {
            let NamedExpr { expr, alias: None } = x else {
                return Err(SqlError::invalid("pattern expression with alias"));
            };
            from_ast_expression(expr)
        })
        .collect::<SqlResult<Vec<_>>>()?;
    Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
        function_name: spec::ObjectName::bare(quantifier),
        arguments,
        named_arguments: vec![],
        is_distinct: false,
        is_user_defined_function: false,
        ignore_nulls: None,
        filter: None,
        order_by: None,
    }))
}

fn from_ast_pattern_escape_string(escape: Option<PatternEscape>) -> SqlResult<Option<char>> {
    let Some(escape) = escape else {
        return Ok(None);
    };
    let PatternEscape { escape: _, value } = escape;
    let value = from_ast_string(value)?;
    let mut chars = value.chars();
    match (chars.next(), chars.next()) {
        (Some(x), None) => Ok(Some(x)),
        _ => Err(SqlError::invalid(format!(
            "invalid escape character: {value}"
        ))),
    }
}
