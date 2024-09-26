use sail_common::spec;
use sqlparser::ast;
use sqlparser::keywords::RESERVED_FOR_COLUMN_ALIAS;
use sqlparser::parser::Parser;

use crate::data_type::from_ast_data_type;
use crate::error::{SqlError, SqlResult};
use crate::expression::value::from_ast_value;
use crate::literal::{parse_date_string, parse_timestamp_string, LiteralValue, Signed};
use crate::parser::{fail_on_extra_token, SparkDialect};
use crate::query::from_ast_query;
use crate::utils::normalize_ident;

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

fn negate_expression(expr: spec::Expr, negated: bool) -> spec::Expr {
    if negated {
        spec::Expr::from(Function {
            name: "not".to_string(),
            args: vec![expr],
        })
    } else {
        expr
    }
}

pub(crate) fn from_ast_object_name(name: ast::ObjectName) -> SqlResult<spec::ObjectName> {
    Ok(name
        .0
        .into_iter()
        .map(|i| i.value)
        .collect::<Vec<_>>()
        .into())
}

pub(crate) fn from_ast_object_name_normalized(
    name: &ast::ObjectName,
) -> SqlResult<spec::ObjectName> {
    Ok(name
        .0
        .iter()
        .map(normalize_ident)
        .collect::<Vec<_>>()
        .into())
}

#[allow(unused_doc_comments)]
pub(crate) fn from_ast_ident(ident: &ast::Ident, normalize: bool) -> SqlResult<spec::Identifier> {
    Ok(if normalize {
        normalize_ident(ident).into()
    } else {
        /// Uses logic in [`ast::Ident::fmt`] to convert `Ident` to `String`.
        ident.to_string().into()
    })
}

fn from_ast_unary_operator(op: ast::UnaryOperator) -> SqlResult<String> {
    use ast::UnaryOperator;

    match op {
        UnaryOperator::Plus => Ok("+".to_string()),
        UnaryOperator::Minus => Ok("-".to_string()),
        UnaryOperator::Not => Ok("not".to_string()),
        UnaryOperator::PGBitwiseNot
        | UnaryOperator::PGSquareRoot
        | UnaryOperator::PGCubeRoot
        | UnaryOperator::PGPostfixFactorial
        | UnaryOperator::PGPrefixFactorial
        | UnaryOperator::PGAbs => Err(SqlError::unsupported(format!("unary operator: {:?}", op))),
    }
}

fn from_ast_binary_operator(op: ast::BinaryOperator) -> SqlResult<String> {
    use ast::BinaryOperator;

    match op {
        BinaryOperator::Plus => Ok("+".to_string()),
        BinaryOperator::Minus => Ok("-".to_string()),
        BinaryOperator::Multiply => Ok("*".to_string()),
        BinaryOperator::Divide => Ok("/".to_string()),
        BinaryOperator::Modulo => Ok("%".to_string()),
        BinaryOperator::StringConcat => Ok("concat".to_string()),
        BinaryOperator::Gt => Ok(">".to_string()),
        BinaryOperator::Lt => Ok("<".to_string()),
        BinaryOperator::GtEq => Ok(">=".to_string()),
        BinaryOperator::LtEq => Ok("<=".to_string()),
        BinaryOperator::Spaceship => Ok("<=>".to_string()),
        BinaryOperator::Eq => Ok("==".to_string()),
        BinaryOperator::NotEq => Ok("!=".to_string()),
        BinaryOperator::And => Ok("and".to_string()),
        BinaryOperator::Or => Ok("or".to_string()),
        BinaryOperator::BitwiseOr => Ok("|".to_string()),
        BinaryOperator::BitwiseAnd => Ok("&".to_string()),
        BinaryOperator::BitwiseXor => Ok("^".to_string()),
        BinaryOperator::MyIntegerDivide => Ok("div".to_string()),
        BinaryOperator::Custom(_)
        | BinaryOperator::Xor
        | BinaryOperator::DuckIntegerDivide
        | BinaryOperator::Arrow
        | BinaryOperator::LongArrow
        | BinaryOperator::HashArrow
        | BinaryOperator::HashLongArrow
        | BinaryOperator::AtAt
        | BinaryOperator::AtArrow
        | BinaryOperator::ArrowAt
        | BinaryOperator::HashMinus
        | BinaryOperator::AtQuestion
        | BinaryOperator::Question
        | BinaryOperator::QuestionAnd
        | BinaryOperator::QuestionPipe
        | BinaryOperator::PGBitwiseXor
        | BinaryOperator::PGBitwiseShiftLeft
        | BinaryOperator::PGBitwiseShiftRight
        | BinaryOperator::PGExp
        | BinaryOperator::PGOverlap
        | BinaryOperator::PGRegexMatch
        | BinaryOperator::PGRegexIMatch
        | BinaryOperator::PGRegexNotMatch
        | BinaryOperator::PGRegexNotIMatch
        | BinaryOperator::PGLikeMatch
        | BinaryOperator::PGILikeMatch
        | BinaryOperator::PGNotLikeMatch
        | BinaryOperator::PGNotILikeMatch
        | BinaryOperator::PGStartsWith
        | BinaryOperator::PGCustomBinaryOperator(_) => {
            Err(SqlError::unsupported(format!("binary operator: {:?}", op)))
        }
    }
}

fn from_ast_date_time_field(field: ast::DateTimeField) -> SqlResult<String> {
    Ok(field.to_string())
}

fn from_ast_interval(interval: ast::Interval) -> SqlResult<spec::Expr> {
    Ok(spec::Expr::Literal(
        LiteralValue(Signed(interval, false)).try_into()?,
    ))
}

fn from_ast_function_arg(arg: ast::FunctionArg) -> SqlResult<spec::Expr> {
    use ast::{FunctionArg, FunctionArgExpr};

    match arg {
        // TODO: Support named argument names.
        FunctionArg::Unnamed(arg) | FunctionArg::Named { arg, .. } => {
            let arg = match arg {
                FunctionArgExpr::Expr(e) => from_ast_expression(e)?,
                FunctionArgExpr::QualifiedWildcard(name) => spec::Expr::UnresolvedStar {
                    target: Some(from_ast_object_name(name)?),
                    wildcard_options: Default::default(),
                },
                FunctionArgExpr::Wildcard => spec::Expr::UnresolvedStar {
                    target: None,
                    wildcard_options: Default::default(),
                },
            };
            Ok(arg)
        }
    }
}

pub(crate) fn from_ast_order_by(order_by: ast::OrderByExpr) -> SqlResult<spec::SortOrder> {
    let ast::OrderByExpr {
        expr,
        asc,
        nulls_first,
        with_fill,
    } = order_by;
    if with_fill.is_some() {
        return Err(SqlError::unsupported("order by with fill"));
    }
    let direction = match asc {
        None => spec::SortDirection::Unspecified,
        Some(true) => spec::SortDirection::Ascending,
        Some(false) => spec::SortDirection::Descending,
    };
    let null_ordering = match nulls_first {
        None => spec::NullOrdering::Unspecified,
        Some(true) => spec::NullOrdering::NullsFirst,
        Some(false) => spec::NullOrdering::NullsLast,
    };
    Ok(spec::SortOrder {
        child: Box::new(from_ast_expression(expr)?),
        direction,
        null_ordering,
    })
}

fn from_ast_window_frame(frame: ast::WindowFrame) -> SqlResult<spec::WindowFrame> {
    use ast::WindowFrameUnits;

    let ast::WindowFrame {
        units,
        start_bound,
        end_bound,
    } = frame;
    let frame_type = match units {
        WindowFrameUnits::Rows => spec::WindowFrameType::Row,
        WindowFrameUnits::Range => spec::WindowFrameType::Range,
        WindowFrameUnits::Groups => return Err(SqlError::unsupported("window frame groups")),
    };
    let end_bound = end_bound.unwrap_or(ast::WindowFrameBound::CurrentRow);
    let lower = from_ast_window_frame_bound(start_bound)?;
    let upper = from_ast_window_frame_bound(end_bound)?;
    Ok(spec::WindowFrame {
        frame_type,
        lower,
        upper,
    })
}

fn from_ast_window_frame_bound(
    bound: ast::WindowFrameBound,
) -> SqlResult<spec::WindowFrameBoundary> {
    use ast::WindowFrameBound;

    match bound {
        WindowFrameBound::CurrentRow => Ok(spec::WindowFrameBoundary::CurrentRow),
        WindowFrameBound::Preceding(None) | WindowFrameBound::Following(None) => {
            Ok(spec::WindowFrameBoundary::Unbounded)
        }
        WindowFrameBound::Preceding(Some(e)) | WindowFrameBound::Following(Some(e)) => Ok(
            spec::WindowFrameBoundary::Value(Box::new(from_ast_expression(*e)?)),
        ),
    }
}

pub(crate) fn from_ast_expression(expr: ast::Expr) -> SqlResult<spec::Expr> {
    use ast::Expr;

    match expr {
        Expr::Identifier(ast::Ident {
            value,
            quote_style: _,
        }) => Ok(spec::Expr::UnresolvedAttribute {
            name: spec::ObjectName::new_unqualified(value.into()),
            plan_id: None,
        }),
        Expr::CompoundIdentifier(x) => Ok(spec::Expr::UnresolvedAttribute {
            name: from_ast_object_name(ast::ObjectName(x))?,
            plan_id: None,
        }),
        Expr::IsFalse(expr) => Ok(spec::Expr::IsFalse(Box::new(from_ast_expression(*expr)?))),
        Expr::IsNotFalse(expr) => Ok(spec::Expr::IsNotFalse(Box::new(from_ast_expression(
            *expr,
        )?))),
        Expr::IsTrue(expr) => Ok(spec::Expr::IsTrue(Box::new(from_ast_expression(*expr)?))),
        Expr::IsNotTrue(expr) => Ok(spec::Expr::IsNotTrue(Box::new(from_ast_expression(*expr)?))),
        Expr::IsNull(expr) => Ok(spec::Expr::IsNull(Box::new(from_ast_expression(*expr)?))),
        Expr::IsNotNull(expr) => Ok(spec::Expr::IsNotNull(Box::new(from_ast_expression(*expr)?))),
        Expr::IsUnknown(expr) => Ok(spec::Expr::IsUnknown(Box::new(from_ast_expression(*expr)?))),
        Expr::IsNotUnknown(expr) => Ok(spec::Expr::IsNotUnknown(Box::new(from_ast_expression(
            *expr,
        )?))),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(spec::Expr::InList {
            expr: Box::new(from_ast_expression(*expr)?),
            list: list
                .into_iter()
                .map(from_ast_expression)
                .collect::<SqlResult<Vec<_>>>()?,
            negated,
        }),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(spec::Expr::Between {
            expr: Box::new(from_ast_expression(*expr)?),
            negated,
            low: Box::new(from_ast_expression(*low)?),
            high: Box::new(from_ast_expression(*high)?),
        }),
        Expr::BinaryOp { left, op, right } => {
            let op = from_ast_binary_operator(op)?;
            Ok(spec::Expr::from(Function {
                name: op,
                args: vec![from_ast_expression(*left)?, from_ast_expression(*right)?],
            }))
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let mut args = vec![from_ast_expression(*expr)?, from_ast_expression(*pattern)?];
            if let Some(escape_char) = escape_char {
                args.push(LiteralValue(escape_char).try_into()?);
            };
            let result = spec::Expr::from(Function {
                name: "like".to_string(),
                args,
            });
            Ok(negate_expression(result, negated))
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let mut args = vec![from_ast_expression(*expr)?, from_ast_expression(*pattern)?];
            if let Some(escape_char) = escape_char {
                args.push(LiteralValue(escape_char).try_into()?);
            };
            let result = spec::Expr::from(Function {
                name: "ilike".to_string(),
                args,
            });
            Ok(negate_expression(result, negated))
        }
        Expr::RLike {
            negated,
            expr,
            pattern,
            regexp: _,
        } => {
            let result = spec::Expr::from(Function {
                name: "rlike".to_string(),
                args: vec![from_ast_expression(*expr)?, from_ast_expression(*pattern)?],
            });
            Ok(negate_expression(result, negated))
        }
        Expr::UnaryOp { op, expr } => Ok(spec::Expr::from(Function {
            name: from_ast_unary_operator(op)?,
            args: vec![from_ast_expression(*expr)?],
        })),
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if kind != ast::CastKind::Cast {
                return Err(SqlError::unsupported(format!("cast kind: {:?}", kind)));
            }
            if let Some(f) = format {
                return Err(SqlError::unsupported(format!("cast format: {:?}", f)));
            }
            Ok(spec::Expr::Cast {
                expr: Box::new(from_ast_expression(*expr)?),
                cast_to_type: from_ast_data_type(&data_type)?,
            })
        }
        Expr::Extract {
            field,
            syntax: _,
            expr,
        } => Ok(spec::Expr::from(Function {
            name: "extract".to_string(),
            args: vec![
                LiteralValue(from_ast_date_time_field(field)?).try_into()?,
                from_ast_expression(*expr)?,
            ],
        })),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            special: _,
        } => {
            let mut args = vec![from_ast_expression(*expr)?];
            if let Some(substring_from) = substring_from {
                args.push(from_ast_expression(*substring_from)?);
            }
            if let Some(substring_for) = substring_for {
                args.push(from_ast_expression(*substring_for)?);
            }
            Ok(spec::Expr::from(Function {
                name: "substring".to_string(),
                args,
            }))
        }
        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            use ast::TrimWhereField;

            if trim_characters.is_some() {
                return Err(SqlError::unsupported("trim characters"));
            }
            let name = match trim_where {
                Some(TrimWhereField::Both) | None => "trim",
                Some(TrimWhereField::Leading) => "ltrim",
                Some(TrimWhereField::Trailing) => "rtrim",
            };
            let mut args = vec![from_ast_expression(*expr)?];
            if let Some(trim_what) = trim_what {
                args.push(from_ast_expression(*trim_what)?);
            }
            Ok(spec::Expr::from(Function {
                name: name.to_string(),
                args,
            }))
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            let mut args = vec![
                from_ast_expression(*expr)?,
                from_ast_expression(*overlay_what)?,
                from_ast_expression(*overlay_from)?,
            ];
            if let Some(overlay_for) = overlay_for {
                args.push(from_ast_expression(*overlay_for)?);
            }
            Ok(spec::Expr::from(Function {
                name: "overlay".to_string(),
                args,
            }))
        }
        Expr::Nested(e) => from_ast_expression(*e),
        Expr::Value(v) => from_ast_value(v),
        Expr::TypedString {
            ref data_type,
            ref value,
        } => {
            let literal = match data_type {
                ast::DataType::Date => parse_date_string(value.as_str()),
                ast::DataType::Timestamp(_, _) => parse_timestamp_string(value.as_str()),
                _ => Err(SqlError::unsupported(format!(
                    "typed string expression: {expr:?}"
                ))),
            }?;
            Ok(spec::Expr::Literal(literal))
        }
        Expr::Function(ast::Function {
            name,
            parameters,
            args,
            filter,
            null_treatment,
            over,
            within_group,
        }) => {
            use ast::FunctionArguments;

            if !matches!(parameters, FunctionArguments::None) {
                return Err(SqlError::unsupported("function parameters"));
            }
            if filter.is_some() {
                return Err(SqlError::unsupported("function filter"));
            }
            if null_treatment.is_some() {
                return Err(SqlError::unsupported("function null treatment"));
            }
            if !within_group.is_empty() {
                return Err(SqlError::unsupported("function within group"));
            }
            let (args, distinct) = match args {
                FunctionArguments::None => (vec![], false),
                FunctionArguments::Subquery(_) => {
                    return Err(SqlError::unsupported("subquery function arguments"))
                }
                FunctionArguments::List(ast::FunctionArgumentList {
                    duplicate_treatment,
                    args,
                    clauses,
                }) => {
                    if !clauses.is_empty() {
                        return Err(SqlError::unsupported("function argument clauses"));
                    }
                    let distinct = match duplicate_treatment {
                        Some(ast::DuplicateTreatment::All) | None => false,
                        Some(ast::DuplicateTreatment::Distinct) => true,
                    };
                    let args = args
                        .into_iter()
                        .map(from_ast_function_arg)
                        .collect::<SqlResult<Vec<_>>>()?;
                    (args, distinct)
                }
            };
            let function = spec::Expr::UnresolvedFunction {
                function_name: name.to_string(),
                arguments: args,
                is_distinct: distinct,
                is_user_defined_function: false,
            };
            if let Some(over) = over {
                use ast::WindowType;

                match over {
                    WindowType::WindowSpec(ast::WindowSpec {
                        window_name: _,
                        partition_by,
                        order_by,
                        window_frame,
                    }) => {
                        let partition_spec = partition_by
                            .into_iter()
                            .map(from_ast_expression)
                            .collect::<SqlResult<Vec<_>>>()?;
                        let order_spec = order_by
                            .into_iter()
                            .map(from_ast_order_by)
                            .collect::<SqlResult<Vec<_>>>()?;
                        let frame_spec = window_frame
                            .map(|f| -> SqlResult<_> { from_ast_window_frame(f) })
                            .transpose()?;
                        Ok(spec::Expr::Window {
                            window_function: Box::new(function),
                            partition_spec,
                            order_spec,
                            frame_spec,
                        })
                    }
                    WindowType::NamedWindow(_) => {
                        Err(SqlError::unsupported("named window function"))
                    }
                }
            } else {
                Ok(function)
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut args = vec![];
            conditions
                .into_iter()
                .zip(results.into_iter())
                .try_for_each::<_, SqlResult<_>>(|(condition, result)| {
                    if let Some(ref operand) = operand {
                        let condition = spec::Expr::from(Function {
                            name: "==".to_string(),
                            args: vec![
                                from_ast_expression(*operand.clone())?,
                                from_ast_expression(condition)?,
                            ],
                        });
                        args.push(condition);
                    } else {
                        args.push(from_ast_expression(condition)?);
                    }
                    args.push(from_ast_expression(result)?);
                    Ok(())
                })?;
            if let Some(else_result) = else_result {
                args.push(from_ast_expression(*else_result)?);
            }
            Ok(spec::Expr::from(Function {
                name: "when".to_string(),
                args,
            }))
        }
        Expr::Interval(interval) => from_ast_interval(interval),
        Expr::Wildcard => Ok(spec::Expr::UnresolvedStar {
            target: None,
            wildcard_options: Default::default(),
        }),
        Expr::QualifiedWildcard(name) => Ok(spec::Expr::UnresolvedStar {
            target: Some(from_ast_object_name(name)?),
            wildcard_options: Default::default(),
        }),
        Expr::Lambda(ast::LambdaFunction { params, body }) => {
            use ast::OneOrManyWithParens;

            let function = from_ast_expression(*body)?;
            let args = match params {
                OneOrManyWithParens::One(x) => vec![x],
                OneOrManyWithParens::Many(x) => x,
            };
            let args = args
                .into_iter()
                .map(|arg| spec::UnresolvedNamedLambdaVariable {
                    name: spec::ObjectName::new_unqualified(arg.value.into()),
                })
                .collect();
            Ok(spec::Expr::LambdaFunction {
                arguments: args,
                function: Box::new(function),
            })
        }
        Expr::MapAccess { column, keys } => {
            let mut column = from_ast_expression(*column)?;
            for key in keys {
                column = spec::Expr::UnresolvedExtractValue {
                    child: Box::new(column),
                    extraction: Box::new(from_ast_expression(key.key)?),
                };
            }
            Ok(column)
        }
        Expr::CompositeAccess {
            expr,
            key: ast::Ident { value, .. },
        } => Ok(spec::Expr::UnresolvedExtractValue {
            child: Box::new(from_ast_expression(*expr)?),
            extraction: Box::new(spec::Expr::UnresolvedAttribute {
                name: spec::ObjectName::new_unqualified(value.into()),
                plan_id: None,
            }),
        }),
        Expr::Map(_) => Err(SqlError::unsupported("map expression")),
        Expr::Subscript { expr, subscript } => {
            let mut expr = from_ast_expression(*expr)?;
            expr = match *subscript {
                ast::Subscript::Index { index } => spec::Expr::UnresolvedExtractValue {
                    child: Box::new(expr),
                    extraction: Box::new(from_ast_expression(index)?),
                },
                ast::Subscript::Slice {
                    lower_bound: _,
                    upper_bound: _,
                    stride: _,
                } => {
                    return Err(SqlError::unsupported("Expr::Subscript::Slice"));
                }
            };
            Ok(expr)
        }
        Expr::IsDistinctFrom(a, b) => Ok(spec::Expr::IsDistinctFrom {
            left: Box::new(from_ast_expression(*a)?),
            right: Box::new(from_ast_expression(*b)?),
        }),
        Expr::IsNotDistinctFrom(a, b) => Ok(spec::Expr::IsNotDistinctFrom {
            left: Box::new(from_ast_expression(*a)?),
            right: Box::new(from_ast_expression(*b)?),
        }),
        Expr::Named { expr, name } => Ok(spec::Expr::Alias {
            expr: Box::new(from_ast_expression(*expr)?),
            name: vec![name.value.into()],
            metadata: None,
        }),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(spec::Expr::InSubquery {
            expr: Box::new(from_ast_expression(*expr)?),
            subquery: Box::new(from_ast_query(*subquery)?),
            negated,
        }),
        Expr::Subquery(subquery) => Ok(spec::Expr::ScalarSubquery {
            subquery: Box::new(from_ast_query(*subquery)?),
        }),
        Expr::Exists { subquery, negated } => Ok(spec::Expr::Exists {
            subquery: Box::new(from_ast_query(*subquery)?),
            negated,
        }),
        Expr::SimilarTo {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let escape_char = if let Some(char) = escape_char {
                if char.len() != 1 {
                    return Err(SqlError::invalid(
                        "Invalid escape character in SIMILAR TO expression",
                    ));
                }
                Some(char.chars().next().unwrap())
            } else {
                None
            };
            Ok(spec::Expr::SimilarTo {
                expr: Box::new(from_ast_expression(*expr)?),
                pattern: Box::new(from_ast_expression(*pattern)?),
                negated,
                escape_char,
                case_insensitive: false,
            })
        }
        Expr::Cube(cube) => {
            let cube = cube
                .into_iter()
                .map(|mut expr| {
                    let e = expr
                        .pop()
                        .ok_or_else(|| SqlError::invalid("missing CUBE expression"))?;
                    if !expr.is_empty() {
                        return Err(SqlError::invalid(
                            "tuple expression is not supported for CUBE",
                        ));
                    }
                    from_ast_expression(e)
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::Cube(cube))
        }
        Expr::Rollup(rollup) => {
            let rollup = rollup
                .into_iter()
                .map(|mut expr| {
                    let e = expr
                        .pop()
                        .ok_or_else(|| SqlError::invalid("missing ROLLUP expression"))?;
                    if !expr.is_empty() {
                        return Err(SqlError::unsupported(
                            "tuple expression is not supported for ROLLUP",
                        ));
                    }
                    from_ast_expression(e)
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::Rollup(rollup))
        }
        Expr::GroupingSets(sets) => {
            let sets = sets
                .into_iter()
                .map(|set| {
                    set.into_iter()
                        .map(from_ast_expression)
                        .collect::<SqlResult<Vec<_>>>()
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::Expr::GroupingSets(sets))
        }
        Expr::Struct { values, fields } => from_ast_struct(values, fields),
        Expr::Tuple(values) => match values.first() {
            Some(Expr::Identifier(_)) | Some(Expr::Value(_)) => from_ast_struct(values, vec![]),
            other => Err(SqlError::unsupported(format!(
                "Only tuple of identifiers or values are supported, found: {other:?}"
            ))),
        },
        Expr::Ceil {
            expr,
            field: _field,
        } => {
            // TODO: When Sail's patched sqlparser is updated to the latest version, field will be
            //  `CeilFloorKind` instead of `DateTimeField` which we can use.
            Ok(spec::Expr::from(Function {
                name: "ceil".to_string(),
                args: vec![from_ast_expression(*expr)?],
            }))
        }
        Expr::Floor {
            expr,
            field: _field,
        } => {
            // TODO: When Sail's patched sqlparser is updated to the latest version, field will be
            //  `CeilFloorKind` instead of `DateTimeField` which we can use.
            Ok(spec::Expr::from(Function {
                name: "floor".to_string(),
                args: vec![from_ast_expression(*expr)?],
            }))
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
        } => {
            match compare_op {
                ast::BinaryOperator::Eq => {
                    // left = ANY(right)
                    Ok(spec::Expr::from(Function {
                        name: "array_contains".to_string(),
                        args: vec![from_ast_expression(*right)?, from_ast_expression(*left)?],
                    }))
                }
                other => Err(SqlError::unsupported(format!(
                    "ANY operator with compare operator: {other:?}"
                ))),
            }
        }
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => {
            match compare_op {
                ast::BinaryOperator::Eq => {
                    // left = ALL(right)
                    Ok(spec::Expr::from(Function {
                        name: "array_contains_all".to_string(),
                        args: vec![from_ast_expression(*right)?, from_ast_expression(*left)?],
                    }))
                }
                other => Err(SqlError::unsupported(format!(
                    "ALL operator with compare operator: {other:?}"
                ))),
            }
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            let expr = Box::new(from_ast_expression(*timestamp)?);
            let cast_to_type = match *time_zone {
                Expr::Value(ast::Value::SingleQuotedString(time_zone))
                | Expr::Value(ast::Value::DoubleQuotedString(time_zone)) => {
                    spec::DataType::Timestamp(
                        Some(spec::TimeUnit::Microsecond),
                        Some(time_zone.into()),
                    )
                }
                _ => {
                    return Err(SqlError::invalid(
                        "AT TIME ZONE expression must be a single or double quoted string",
                    ))
                }
            };
            Ok(spec::Expr::Cast { expr, cast_to_type })
        }
        Expr::Position { expr, r#in } => {
            let string = from_ast_expression(*r#in)?;
            let substring = from_ast_expression(*expr)?;
            Ok(spec::Expr::from(Function {
                name: "strpos".to_string(),
                args: vec![string, substring],
            }))
        }
        Expr::JsonAccess { .. }
        | Expr::InUnnest { .. }
        | Expr::Convert { .. }
        | Expr::Collate { .. }
        | Expr::IntroducedString { .. }
        | Expr::Array(_)
        | Expr::MatchAgainst { .. }
        | Expr::Dictionary(_)
        | Expr::OuterJoin(_)
        | Expr::Prior(_) => Err(SqlError::unsupported(format!("expression: {expr:?}"))),
    }
}

pub fn from_ast_struct(
    values: Vec<ast::Expr>,
    fields: Vec<ast::StructField>,
) -> SqlResult<spec::Expr> {
    if !fields.is_empty() {
        return Err(SqlError::unsupported("struct fields"));
    }
    let is_named_struct = values
        .iter()
        .any(|value| matches!(value, ast::Expr::Named { .. }));
    let args = values
        .into_iter()
        .map(from_ast_expression)
        .collect::<SqlResult<Vec<_>>>()?;
    Ok(spec::Expr::from(Function {
        name: if is_named_struct {
            "named_struct".to_string()
        } else {
            "struct".to_string()
        },
        args,
    }))
}

pub fn parse_object_name(s: &str) -> SqlResult<spec::ObjectName> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(s)?;
    let names: Vec<String> = parser
        .parse_multipart_identifier()?
        .into_iter()
        .map(|x| x.value)
        .collect();
    fail_on_extra_token(&mut parser, "object name")?;
    Ok(names.into())
}

pub fn parse_expression(sql: &str) -> SqlResult<spec::Expr> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let expr = parser.parse_expr()?;
    fail_on_extra_token(&mut parser, "expression")?;
    from_ast_expression(expr)
}

pub fn parse_wildcard_expression(sql: &str) -> SqlResult<spec::Expr> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let expr = parser.parse_wildcard_expr()?;
    let expr = match expr {
        x @ ast::Expr::Wildcard | x @ ast::Expr::QualifiedWildcard(_) => from_ast_expression(x)?,
        x => match parser.parse_optional_alias(RESERVED_FOR_COLUMN_ALIAS)? {
            Some(ast::Ident { value, .. }) => spec::Expr::Alias {
                expr: Box::new(from_ast_expression(x)?),
                name: vec![value.into()],
                metadata: None,
            },
            None => from_ast_expression(x)?,
        },
    };
    fail_on_extra_token(&mut parser, "wildcard expression")?;
    Ok(expr)
}

pub fn parse_qualified_wildcard(sql: &str) -> SqlResult<spec::ObjectName> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let expr = parser.parse_wildcard_expr()?;
    let name: Vec<String> = match expr {
        ast::Expr::QualifiedWildcard(name) => name.0.into_iter().map(|x| x.value).collect(),
        _ => {
            return Err(SqlError::invalid(format!(
                "invalid qualified wildcard: {sql}",
            )))
        }
    };
    fail_on_extra_token(&mut parser, "qualified wildcard")?;
    Ok(name.into())
}
