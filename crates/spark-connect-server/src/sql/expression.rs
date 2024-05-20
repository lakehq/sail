use crate::error::{SparkError, SparkResult};
use crate::spark::connect as sc;
use crate::spark::connect::expression::literal::{Decimal, LiteralType};
use crate::spark::connect::expression::ExprType;
use crate::sql::data_type::from_ast_data_type;
use crate::sql::literal::{parse_date_string, parse_timestamp_string, LiteralValue};
use crate::sql::parser::SparkDialect;
use sqlparser::ast;
use sqlparser::ast::DataType;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

struct Identifier(String);

impl From<Identifier> for sc::Expression {
    fn from(identifier: Identifier) -> sc::Expression {
        sc::Expression {
            expr_type: Some(ExprType::UnresolvedAttribute(
                sc::expression::UnresolvedAttribute {
                    unparsed_identifier: identifier.0,
                    plan_id: None,
                },
            )),
        }
    }
}

struct Function {
    name: String,
    args: Vec<sc::Expression>,
}

impl From<Function> for sc::Expression {
    fn from(function: Function) -> sc::Expression {
        sc::Expression {
            expr_type: Some(ExprType::UnresolvedFunction(
                sc::expression::UnresolvedFunction {
                    function_name: function.name,
                    arguments: function.args,
                    is_distinct: false,
                    is_user_defined_function: false,
                },
            )),
        }
    }
}

fn negate_expression(expr: sc::Expression, negated: bool) -> sc::Expression {
    if negated {
        sc::Expression::from(Function {
            name: "not".to_string(),
            args: vec![expr],
        })
    } else {
        expr
    }
}

fn from_ast_unary_operator(op: ast::UnaryOperator) -> SparkResult<String> {
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
        | UnaryOperator::PGAbs => Err(SparkError::unsupported(format!("unary operator: {:?}", op))),
    }
}

fn from_ast_binary_operator(op: ast::BinaryOperator) -> SparkResult<String> {
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
        | BinaryOperator::PGCustomBinaryOperator(_) => Err(SparkError::unsupported(format!(
            "binary operator: {:?}",
            op
        ))),
    }
}

fn from_ast_date_time_field(field: ast::DateTimeField) -> SparkResult<String> {
    Ok(field.to_string())
}

fn from_ast_value(value: ast::Value) -> SparkResult<sc::Expression> {
    use ast::Value;

    match value {
        Value::Number(value, postfix) => match postfix.as_deref() {
            Some("Y") | Some("y") => {
                let value = LiteralValue::<i8>::try_from(value.clone())?;
                sc::Expression::try_from(value)
            }
            Some("S") | Some("s") => {
                let value = LiteralValue::<i16>::try_from(value.clone())?;
                sc::Expression::try_from(value)
            }
            Some("L") | Some("l") => {
                let value = LiteralValue::<i64>::try_from(value.clone())?;
                sc::Expression::try_from(value)
            }
            Some("F") | Some("f") => {
                let value = LiteralValue::<f32>::try_from(value.clone())?;
                sc::Expression::try_from(value)
            }
            Some("D") | Some("d") => {
                let value = LiteralValue::<f64>::try_from(value.clone())?;
                sc::Expression::try_from(value)
            }
            Some(x) if x.to_uppercase() == "BD" => {
                let value = LiteralValue::<Decimal>::try_from(value.clone())?;
                sc::Expression::try_from(value)
            }
            None | Some("") => {
                if let Ok(value) = LiteralValue::<i32>::try_from(value.clone()) {
                    sc::Expression::try_from(value)
                } else if let Ok(value) = LiteralValue::<i64>::try_from(value.clone()) {
                    sc::Expression::try_from(value)
                } else {
                    let value = LiteralValue::<Decimal>::try_from(value.clone())?;
                    sc::Expression::try_from(value)
                }
            }
            Some(&_) => Err(SparkError::invalid(format!(
                "number postfix: {:?}{:?}",
                value, postfix
            ))),
        },
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::DollarQuotedString(ast::DollarQuotedString { value, .. })
        | Value::TripleSingleQuotedString(value)
        | Value::TripleDoubleQuotedString(value) => sc::Expression::try_from(LiteralValue(value)),
        Value::HexStringLiteral(value) => {
            let value: LiteralValue<Vec<u8>> = value.try_into()?;
            sc::Expression::try_from(value)
        }
        Value::Boolean(value) => sc::Expression::try_from(LiteralValue(value)),
        Value::Null => Ok(sc::Expression {
            expr_type: Some(ExprType::Literal(sc::expression::Literal {
                // We cannot infer the data type without a schema.
                // So we just use an "unparsed" data type here.
                literal_type: Some(LiteralType::Null(sc::DataType {
                    kind: Some(sc::data_type::Kind::Unparsed(sc::data_type::Unparsed {
                        data_type_string: "".to_string(),
                    })),
                })),
            })),
        }),
        Value::Placeholder(_) => return Err(SparkError::todo("placeholder value")),
        Value::EscapedStringLiteral(_)
        | Value::SingleQuotedByteStringLiteral(_)
        | Value::DoubleQuotedByteStringLiteral(_)
        | Value::TripleSingleQuotedByteStringLiteral(_)
        | Value::TripleDoubleQuotedByteStringLiteral(_)
        | Value::SingleQuotedRawStringLiteral(_)
        | Value::DoubleQuotedRawStringLiteral(_)
        | Value::TripleSingleQuotedRawStringLiteral(_)
        | Value::TripleDoubleQuotedRawStringLiteral(_)
        | Value::NationalStringLiteral(_) => {
            return Err(SparkError::unsupported(format!("value: {:?}", value)))
        }
    }
}

fn from_ast_interval(interval: ast::Interval) -> SparkResult<sc::Expression> {
    Ok(sc::Expression {
        expr_type: Some(ExprType::Literal(sc::expression::Literal {
            literal_type: Some(LiteralValue(interval).try_into()?),
        })),
    })
}

fn from_ast_function_arg(arg: ast::FunctionArg) -> SparkResult<sc::Expression> {
    use ast::{FunctionArg, FunctionArgExpr};

    match arg {
        FunctionArg::Named { .. } => Err(SparkError::unsupported("named function argument")),
        FunctionArg::Unnamed(arg) => {
            let arg = match arg {
                FunctionArgExpr::Expr(e) => from_ast_expression(e)?,
                FunctionArgExpr::QualifiedWildcard(name) => sc::Expression {
                    expr_type: Some(ExprType::UnresolvedStar(sc::expression::UnresolvedStar {
                        unparsed_target: Some(name.to_string()),
                    })),
                },
                FunctionArgExpr::Wildcard => sc::Expression {
                    expr_type: Some(ExprType::UnresolvedStar(sc::expression::UnresolvedStar {
                        unparsed_target: None,
                    })),
                },
            };
            Ok(arg)
        }
    }
}

fn from_ast_order_by(order_by: ast::OrderByExpr) -> SparkResult<sc::expression::SortOrder> {
    use sc::expression::sort_order::{NullOrdering, SortDirection};

    let ast::OrderByExpr {
        expr,
        asc,
        nulls_first,
    } = order_by;
    let direction = match asc {
        None => SortDirection::Unspecified,
        Some(true) => SortDirection::Ascending,
        Some(false) => SortDirection::Descending,
    };
    let null_ordering = match nulls_first {
        None => NullOrdering::SortNullsUnspecified,
        Some(true) => NullOrdering::SortNullsFirst,
        Some(false) => NullOrdering::SortNullsLast,
    };
    Ok(sc::expression::SortOrder {
        child: Some(Box::new(from_ast_expression(expr)?)),
        direction: direction as i32,
        null_ordering: null_ordering as i32,
    })
}

fn from_ast_window_frame(
    frame: ast::WindowFrame,
) -> SparkResult<sc::expression::window::WindowFrame> {
    use ast::WindowFrameUnits;
    use sc::expression::window::window_frame::FrameType;

    let ast::WindowFrame {
        units,
        start_bound,
        end_bound,
    } = frame;
    let units = match units {
        WindowFrameUnits::Rows => FrameType::Row,
        WindowFrameUnits::Range => FrameType::Range,
        WindowFrameUnits::Groups => return Err(SparkError::unsupported("window frame groups")),
    };
    let lower = Some(Box::new(from_ast_window_frame_bound(start_bound)?));
    let upper = end_bound
        .map(|b| -> SparkResult<_> { Ok(Box::new(from_ast_window_frame_bound(b)?)) })
        .transpose()?;
    Ok(sc::expression::window::WindowFrame {
        frame_type: units as i32,
        lower,
        upper,
    })
}

fn from_ast_window_frame_bound(
    bound: ast::WindowFrameBound,
) -> SparkResult<sc::expression::window::window_frame::FrameBoundary> {
    use ast::WindowFrameBound;
    use sc::expression::window::window_frame::frame_boundary::Boundary;

    let boundary = match bound {
        WindowFrameBound::CurrentRow => Boundary::CurrentRow(true),
        WindowFrameBound::Preceding(None) | WindowFrameBound::Following(None) => {
            Boundary::Unbounded(true)
        }
        WindowFrameBound::Preceding(Some(e)) | WindowFrameBound::Following(Some(e)) => {
            Boundary::Value(Box::new(from_ast_expression(*e)?))
        }
    };
    Ok(sc::expression::window::window_frame::FrameBoundary {
        boundary: Some(boundary),
    })
}

fn from_ast_expression(expr: ast::Expr) -> SparkResult<sc::Expression> {
    use ast::Expr;

    match expr {
        Expr::Identifier(ast::Ident {
            value,
            quote_style: _,
        }) => Ok(sc::Expression::from(Identifier(value))),
        Expr::CompoundIdentifier(x) => Ok(sc::Expression::from(Identifier(
            ast::ObjectName(x).to_string(),
        ))),
        Expr::IsFalse(e) => Ok(sc::Expression::from(Function {
            name: "<=>".to_string(),
            args: vec![from_ast_expression(*e)?, LiteralValue(false).try_into()?],
        })),
        Expr::IsNotFalse(e) => Ok(negate_expression(
            from_ast_expression(Expr::IsFalse(e))?,
            true,
        )),
        Expr::IsTrue(e) => Ok(sc::Expression::from(Function {
            name: "<=>".to_string(),
            args: vec![from_ast_expression(*e)?, LiteralValue(true).try_into()?],
        })),
        Expr::IsNotTrue(e) => Ok(negate_expression(
            from_ast_expression(Expr::IsTrue(e))?,
            true,
        )),
        Expr::IsNull(e) => Ok(sc::Expression::from(Function {
            name: "isnull".to_string(),
            args: vec![from_ast_expression(*e)?],
        })),
        Expr::IsNotNull(e) => Ok(sc::Expression::from(Function {
            name: "isnotnull".to_string(),
            args: vec![from_ast_expression(*e)?],
        })),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let result = sc::Expression::from(Function {
                name: "array_contains".to_string(),
                args: vec![
                    sc::Expression::from(Function {
                        name: "array".to_string(),
                        args: list
                            .into_iter()
                            .map(from_ast_expression)
                            .collect::<SparkResult<Vec<_>>>()?,
                    }),
                    from_ast_expression(*expr)?,
                ],
            });
            Ok(negate_expression(result, negated))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let result = sc::Expression::from(Function {
                name: "and".to_string(),
                args: vec![
                    sc::Expression::from(Function {
                        name: ">=".to_string(),
                        args: vec![
                            from_ast_expression(*expr.clone())?,
                            from_ast_expression(*low)?,
                        ],
                    }),
                    sc::Expression::from(Function {
                        name: "<=".to_string(),
                        args: vec![from_ast_expression(*expr)?, from_ast_expression(*high)?],
                    }),
                ],
            });
            Ok(negate_expression(result, negated))
        }
        Expr::BinaryOp { left, op, right } => {
            let op = from_ast_binary_operator(op)?;
            Ok(sc::Expression::from(Function {
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
            let result = sc::Expression::from(Function {
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
            let result = sc::Expression::from(Function {
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
            let result = sc::Expression::from(Function {
                name: "rlike".to_string(),
                args: vec![from_ast_expression(*expr)?, from_ast_expression(*pattern)?],
            });
            Ok(negate_expression(result, negated))
        }
        Expr::UnaryOp { op, expr } => Ok(sc::Expression::from(Function {
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
                return Err(SparkError::unsupported(format!("cast kind: {:?}", kind)));
            }
            if let Some(f) = format {
                return Err(SparkError::unsupported(format!("cast format: {:?}", f)));
            }
            Ok(sc::Expression {
                expr_type: Some(ExprType::Cast(Box::new(sc::expression::Cast {
                    expr: Some(Box::new(from_ast_expression(*expr)?)),
                    cast_to_type: Some(sc::expression::cast::CastToType::Type(from_ast_data_type(
                        &data_type,
                    )?)),
                }))),
            })
        }
        Expr::Extract { field, expr } => Ok(sc::Expression::from(Function {
            name: "extract".to_string(),
            args: vec![
                from_ast_expression(*expr)?,
                LiteralValue(from_ast_date_time_field(field)?).try_into()?,
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
            Ok(sc::Expression::from(Function {
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
                return Err(SparkError::unsupported("trim characters"));
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
            Ok(sc::Expression::from(Function {
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
            Ok(sc::Expression::from(Function {
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
                DataType::Date => parse_date_string(value.as_str()),
                DataType::Timestamp(_, _) => parse_timestamp_string(value.as_str()),
                _ => Err(SparkError::unsupported(format!(
                    "typed string expression: {:?}",
                    expr
                ))),
            }?;
            Ok(sc::Expression {
                expr_type: Some(ExprType::Literal(sc::expression::Literal {
                    literal_type: Some(literal),
                })),
            })
        }
        Expr::Function(ast::Function {
            name,
            args,
            filter,
            null_treatment,
            over,
            within_group,
        }) => {
            use ast::FunctionArguments;

            if filter.is_some() {
                return Err(SparkError::unsupported("function filter"));
            }
            if null_treatment.is_some() {
                return Err(SparkError::unsupported("function null treatment"));
            }
            if !within_group.is_empty() {
                return Err(SparkError::unsupported("function within group"));
            }
            let (args, distinct) = match args {
                FunctionArguments::None => (vec![], false),
                FunctionArguments::Subquery(_) => {
                    return Err(SparkError::unsupported("subquery function arguments"))
                }
                FunctionArguments::List(ast::FunctionArgumentList {
                    duplicate_treatment,
                    args,
                    clauses,
                }) => {
                    if !clauses.is_empty() {
                        return Err(SparkError::unsupported("function argument clauses"));
                    }
                    let distinct = match duplicate_treatment {
                        Some(ast::DuplicateTreatment::All) | None => false,
                        Some(ast::DuplicateTreatment::Distinct) => true,
                    };
                    let args = args
                        .into_iter()
                        .map(from_ast_function_arg)
                        .collect::<SparkResult<Vec<_>>>()?;
                    (args, distinct)
                }
            };
            let function = sc::Expression {
                expr_type: Some(ExprType::UnresolvedFunction(
                    sc::expression::UnresolvedFunction {
                        function_name: name.to_string(),
                        arguments: args,
                        is_distinct: distinct,
                        is_user_defined_function: false,
                    },
                )),
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
                            .collect::<SparkResult<Vec<_>>>()?;
                        let order_spec = order_by
                            .into_iter()
                            .map(from_ast_order_by)
                            .collect::<SparkResult<Vec<_>>>()?;
                        let frame_spec = window_frame
                            .map(|f| -> SparkResult<_> { Ok(Box::new(from_ast_window_frame(f)?)) })
                            .transpose()?;
                        Ok(sc::Expression {
                            expr_type: Some(ExprType::Window(Box::new(sc::expression::Window {
                                window_function: Some(Box::new(function)),
                                partition_spec,
                                order_spec,
                                frame_spec,
                            }))),
                        })
                    }
                    WindowType::NamedWindow(_) => {
                        return Err(SparkError::unsupported("named window function"))
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
                .try_for_each::<_, SparkResult<_>>(|(condition, result)| {
                    if let Some(ref operand) = operand {
                        let condition = sc::Expression::from(Function {
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
            Ok(sc::Expression::from(Function {
                name: "when".to_string(),
                args,
            }))
        }
        Expr::Interval(interval) => from_ast_interval(interval),
        Expr::Wildcard => Ok(sc::Expression {
            expr_type: Some(ExprType::UnresolvedStar(sc::expression::UnresolvedStar {
                unparsed_target: None,
            })),
        }),
        Expr::QualifiedWildcard(name) => Ok(sc::Expression {
            expr_type: Some(ExprType::UnresolvedStar(sc::expression::UnresolvedStar {
                unparsed_target: Some(format!("{name}.*")),
            })),
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
                .map(|arg| sc::expression::UnresolvedNamedLambdaVariable {
                    name_parts: vec![arg.value],
                })
                .collect();
            Ok(sc::Expression {
                expr_type: Some(ExprType::LambdaFunction(Box::new(
                    sc::expression::LambdaFunction {
                        arguments: args,
                        function: Some(Box::new(function)),
                    },
                ))),
            })
        }
        Expr::MapAccess { column, keys } => {
            let mut column = from_ast_expression(*column)?;
            for key in keys {
                column = sc::Expression {
                    expr_type: Some(ExprType::UnresolvedExtractValue(Box::new(
                        sc::expression::UnresolvedExtractValue {
                            child: Some(Box::new(column)),
                            extraction: Some(Box::new(from_ast_expression(key.key)?)),
                        },
                    ))),
                };
            }
            Ok(column)
        }
        Expr::CompositeAccess {
            expr,
            key: ast::Ident { value, .. },
        } => Ok(sc::Expression {
            expr_type: Some(ExprType::UnresolvedExtractValue(Box::new(
                sc::expression::UnresolvedExtractValue {
                    child: Some(Box::new(from_ast_expression(*expr)?)),
                    extraction: Some(Box::new(sc::Expression::from(Identifier(value)))),
                },
            ))),
        }),
        Expr::ArrayIndex { obj, indexes } => {
            let mut obj = from_ast_expression(*obj)?;
            for index in indexes {
                obj = sc::Expression {
                    expr_type: Some(ExprType::UnresolvedExtractValue(Box::new(
                        sc::expression::UnresolvedExtractValue {
                            child: Some(Box::new(obj)),
                            extraction: Some(Box::new(from_ast_expression(index)?)),
                        },
                    ))),
                };
            }
            Ok(obj)
        }
        Expr::IsDistinctFrom(a, b) => {
            let result = sc::Expression::from(Function {
                name: "<=>".to_string(),
                args: vec![from_ast_expression(*a)?, from_ast_expression(*b)?],
            });
            Ok(negate_expression(result, true))
        }
        Expr::IsNotDistinctFrom(a, b) => Ok(sc::Expression::from(Function {
            name: "<=>".to_string(),
            args: vec![from_ast_expression(*a)?, from_ast_expression(*b)?],
        })),
        Expr::JsonAccess { .. }
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_)
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::SimilarTo { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. }
        | Expr::Convert { .. }
        | Expr::AtTimeZone { .. }
        | Expr::Ceil { .. }
        | Expr::Floor { .. }
        | Expr::Position { .. }
        | Expr::Collate { .. }
        | Expr::IntroducedString { .. }
        | Expr::Exists { .. }
        | Expr::Subquery(_)
        | Expr::GroupingSets(_)
        | Expr::Cube(_)
        | Expr::Rollup(_)
        | Expr::Tuple(_)
        | Expr::Array(_)
        | Expr::MatchAgainst { .. }
        | Expr::Struct { .. }
        | Expr::Named { .. }
        | Expr::Dictionary(_)
        | Expr::OuterJoin(_)
        | Expr::Prior(_) => Err(SparkError::unsupported(format!("expression: {:?}", expr))),
    }
}

pub(crate) fn parse_spark_expression(sql: &str) -> SparkResult<sc::Expression> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let expr = parser.parse_expr()?;
    if parser.peek_token() != Token::EOF {
        let token = parser.next_token();
        return Err(SparkError::invalid(format!(
            "extra tokens after expression: {token}"
        )));
    }
    from_ast_expression(expr)
}

pub(crate) fn parse_spark_qualified_wildcard(sql: &str) -> SparkResult<String> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    let expr = parser.parse_wildcard_expr()?;
    match expr {
        ast::Expr::QualifiedWildcard(name) => Ok(name.to_string()),
        _ => Err(SparkError::invalid(format!(
            "invalid qualified wildcard target: {:?}",
            expr
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_spark_expression;
    use crate::error::SparkError;
    use crate::tests::test_gold_set;
    use std::thread;

    #[test]
    fn test_sql_to_expression() -> Result<(), Box<dyn std::error::Error>> {
        // Run the test in a separate thread with a large stack size
        // so that it can handle deeply nested expressions.
        let builder = thread::Builder::new().stack_size(96 * 1024 * 1024);
        let handler = builder.spawn(|| {
            test_gold_set("tests/gold_data/expression/*.json", |sql: String| {
                let expr = parse_spark_expression(&sql)?;
                if sql.len() > 128 {
                    Err(SparkError::internal("result omitted for long expression"))
                } else {
                    Ok(expr)
                }
            })
        })?;
        Ok(handler
            .join()
            .or_else(|_| Err(SparkError::internal("failed to join thread")))??)
    }
}
