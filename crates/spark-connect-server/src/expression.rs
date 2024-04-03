use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNanoType};
use datafusion::catalog::TableReference;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    expr, AggregateFunction, AggregateUDF, BuiltinScalarFunction, GetFieldAccess, GetIndexedField,
    Operator, ScalarFunctionDefinition, ScalarUDF, TableSource, WindowUDF,
};
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::parser::WildcardExpr;
use datafusion_expr::expr_fn::create_udf;

use pyo3::types::{PyBytes, PyModule};
use pyo3::{PyResult, PyObject, Python};
use pyo3::prelude::PyAnyMethods;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::schema::{from_spark_data_type, parse_spark_data_type_string};
use crate::spark::connect as sc;
use crate::sql::new_sql_parser;

#[derive(Default)]
pub(crate) struct EmptyContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for EmptyContextProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<Arc<dyn TableSource>> {
        Err(datafusion::error::DataFusionError::NotImplemented(format!(
            "get table source: {:?}",
            name
        )))
    }

    fn get_function_meta(&self, _: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

pub(crate) fn from_spark_expression(
    expr: &sc::Expression,
    schema: &DFSchema,
) -> SparkResult<expr::Expr> {
    use sc::expression::ExprType;

    let sc::Expression { expr_type } = expr;
    let expr_type = expr_type.as_ref().required("expression type")?;
    match expr_type {
        ExprType::Literal(literal) => {
            Ok(expr::Expr::Literal(from_spark_literal_to_scalar(literal)?))
        }
        ExprType::UnresolvedAttribute(attr) => {
            let col = Column::new_unqualified(&attr.unparsed_identifier);
            Ok(expr::Expr::Column(col))
        }
        ExprType::UnresolvedFunction(func) => {
            if func.is_distinct {
                return Err(SparkError::unsupported("distinct function"));
            }
            // if func.is_user_defined_function {
            //     return Err(SparkError::unsupported("user defined function"));
            // }
            let args = func
                .arguments
                .iter()
                .map(|x| from_spark_expression(x, schema))
                .collect::<SparkResult<Vec<_>>>()?;
            Ok(get_scalar_function(func.function_name.as_str(), args)?)
        }
        ExprType::ExpressionString(expr) => {
            let mut parser = new_sql_parser(&expr.expression)?;
            match parser.parse_wildcard_expr()? {
                WildcardExpr::Expr(expr) => {
                    let provider = EmptyContextProvider::default();
                    let planner = SqlToRel::new(&provider);
                    let expr = planner.sql_to_expr(expr, schema, &mut PlannerContext::default())?;
                    Ok(expr)
                }
                WildcardExpr::QualifiedWildcard(ObjectName(name)) => {
                    let qualifier = name
                        .iter()
                        .map(|x| x.value.as_str())
                        .collect::<Vec<_>>()
                        .join(".");
                    Ok(expr::Expr::Wildcard {
                        qualifier: Some(qualifier),
                    })
                }
                WildcardExpr::Wildcard => Ok(expr::Expr::Wildcard { qualifier: None }),
            }
        }
        ExprType::UnresolvedStar(star) => {
            // FIXME: column reference is parsed as qualifier
            if let Some(target) = &star.unparsed_target {
                let mut parser = new_sql_parser(target)?;
                let expr = parser.parse_wildcard_expr()?;
                match expr {
                    WildcardExpr::Expr(_) => Err(SparkError::todo("expression as wildcard target")),
                    WildcardExpr::QualifiedWildcard(ast::ObjectName(names)) => {
                        let qualifier = names
                            .iter()
                            .map(|x| match x {
                                ast::Ident {
                                    value,
                                    quote_style: None,
                                } => Ok(value.as_str()),
                                _ => Err(SparkError::todo("quoted identifier")),
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .join(".");
                        Ok(expr::Expr::Wildcard {
                            qualifier: Some(qualifier),
                        })
                    }
                    WildcardExpr::Wildcard => Ok(expr::Expr::Wildcard { qualifier: None }),
                }
            } else {
                Ok(expr::Expr::Wildcard { qualifier: None })
            }
        }
        ExprType::Alias(alias) => {
            let expr = alias.expr.as_ref().required("expression for alias")?;
            if let [name] = alias.name.as_slice() {
                Ok(expr::Expr::Alias(expr::Alias {
                    expr: Box::new(from_spark_expression(expr, schema)?),
                    relation: None,
                    name: name.to_string(),
                }))
            } else {
                Err(SparkError::invalid("one alias name must be specified"))
            }
        }
        ExprType::Cast(cast) => {
            use sc::expression::cast::CastToType;

            let expr = cast.expr.as_ref().required("expression for cast")?;
            let data_type = cast.cast_to_type.as_ref().required("data type for cast")?;
            let data_type = match data_type {
                CastToType::Type(t) => from_spark_data_type(&t)?,
                CastToType::TypeStr(s) => parse_spark_data_type_string(s)?,
            };
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(from_spark_expression(expr, schema)?),
                data_type,
            }))
        }
        ExprType::UnresolvedRegex(_) => Err(SparkError::todo("unresolved regex")),
        ExprType::SortOrder(sort) => {
            use sc::expression::sort_order::{NullOrdering, SortDirection};

            let expr = sort.child.as_ref().required("expression for sort order")?;
            let asc = match SortDirection::try_from(sort.direction) {
                Ok(SortDirection::Ascending) => true,
                Ok(SortDirection::Descending) => false,
                Ok(SortDirection::Unspecified) => true,
                Err(_) => {
                    return Err(SparkError::invalid("invalid sort direction"));
                }
            };
            let nulls_first = match NullOrdering::try_from(sort.null_ordering) {
                Ok(NullOrdering::SortNullsFirst) => true,
                Ok(NullOrdering::SortNullsLast) => false,
                Ok(NullOrdering::SortNullsUnspecified) => asc,
                Err(_) => {
                    return Err(SparkError::invalid("invalid null ordering"));
                }
            };
            Ok(expr::Expr::Sort(expr::Sort {
                expr: Box::new(from_spark_expression(expr, schema)?),
                asc,
                nulls_first,
            }))
        }
        ExprType::LambdaFunction(_) => Err(SparkError::todo("lambda function")),
        ExprType::Window(_) => Err(SparkError::todo("window")),
        ExprType::UnresolvedExtractValue(extract) => {
            use sc::expression::literal::LiteralType;

            let child = extract
                .child
                .as_ref()
                .required("child for extract value")?
                .as_ref();
            let extraction = extract
                .extraction
                .as_ref()
                .required("extraction for extract value")?
                .as_ref();
            let literal = match extraction.expr_type.as_ref().required("extraction type")? {
                ExprType::Literal(literal) => literal.literal_type.as_ref().required("literal")?,
                _ => {
                    return Err(SparkError::invalid("extraction must be a literal"));
                }
            };
            let field = match literal {
                LiteralType::Byte(x) | LiteralType::Short(x) | LiteralType::Integer(x) => {
                    GetFieldAccess::ListIndex {
                        key: Box::new(expr::Expr::Literal(ScalarValue::Int64(Some(*x as i64)))),
                    }
                }
                LiteralType::Long(x) => GetFieldAccess::ListIndex {
                    key: Box::new(expr::Expr::Literal(ScalarValue::Int64(Some(*x)))),
                },
                LiteralType::String(s) => GetFieldAccess::NamedStructField {
                    name: ScalarValue::Utf8(Some(s.clone())),
                },
                _ => {
                    return Err(SparkError::invalid("invalid extraction value"));
                }
            };
            Ok(expr::Expr::GetIndexedField(GetIndexedField {
                expr: Box::new(from_spark_expression(child, schema)?),
                field,
            }))
        }
        ExprType::UpdateFields(_) => Err(SparkError::todo("update fields")),
        ExprType::UnresolvedNamedLambdaVariable(_) => {
            Err(SparkError::todo("unresolved named lambda variable"))
        }
        ExprType::CommonInlineUserDefinedFunction(udf) => {
            let function_name = udf
                .function_name
                .as_str()
                .required("function name for user defined function");

            let deterministic = udf
                .deterministic
                .as_bool()?;

            let arguments = udf
                .arguments
                .iter()
                .map(|x| from_spark_expression(x, schema))
                .collect::<SparkResult<Vec<_>>>()?;

            let python_udf = match &udf.function {
                Some(sc::common_inline_user_defined_function::Function::PythonUdf(python_udf)) => python_udf,
                _ => return Err(SparkError::invalid("Expected a Python UDF")),
            };

            let python_function = Python::with_gil(|py| {
                let cloudpickle = PyModule::import_bound(py, "pyspark.cloudpickle")
                    .expect("Unable to import 'pyspark.cloudpickle'")
                    .getattr("loads")
                    .unwrap();

                let python_function: PyObject = cloudpickle
                    .call1(PyBytes::new_bound(py, python_udf.as_bytes()))
                    .map_err(|s| SparkError::custom(format!("cannot pickle {s}")))
                    .extract(py)?;

                // Wrap the deserialized Python UDF in a Rust function
                let rust_function = move |args: Vec<PyObject>| -> PyResult<PyObject> {
                    python_function.call_bound(py, args, None)
                };

                Ok(rust_function)
            });

            let input_types = udf
                .arguments
                .iter()
                .map(|arg| arg.get_type(schema))
                .collect::<Result<Vec<_>, _>>()?;

            // TODO: create_udf is less performant. Look into ScalarUDFImpl:
            //       https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
            let udf = create_udf(
                function_name,
                arguments.iter().map(|arg| arg.data_type(schema)).collect::<Result<Vec<_>, _>>()?,
            );
            // ...
        }
        ExprType::CallFunction(_) => Err(SparkError::todo("call function")),
        ExprType::Extension(_) => Err(SparkError::unsupported("expression extension")),
    }
}

pub(crate) fn from_spark_literal_to_scalar(
    literal: &sc::expression::Literal,
) -> SparkResult<ScalarValue> {
    use sc::expression::literal::LiteralType;

    let sc::expression::Literal { literal_type } = literal;
    let literal_type = literal_type.as_ref().required("literal type")?;
    match literal_type {
        LiteralType::Null(_) => Ok(ScalarValue::Null),
        LiteralType::Binary(x) => Ok(ScalarValue::Binary(Some(x.clone()))),
        LiteralType::Boolean(x) => Ok(ScalarValue::Boolean(Some(*x))),
        LiteralType::Byte(x) => Ok(ScalarValue::Int8(Some(*x as i8))),
        LiteralType::Short(x) => Ok(ScalarValue::Int16(Some(*x as i16))),
        LiteralType::Integer(x) => Ok(ScalarValue::Int32(Some(*x))),
        LiteralType::Long(x) => Ok(ScalarValue::Int64(Some(*x))),
        LiteralType::Float(x) => Ok(ScalarValue::Float32(Some(*x))),
        LiteralType::Double(x) => Ok(ScalarValue::Float64(Some(*x))),
        LiteralType::Decimal(_) => Err(SparkError::todo("literal decimal")),
        LiteralType::String(x) => Ok(ScalarValue::Utf8(Some(x.clone()))),
        LiteralType::Date(x) => Ok(ScalarValue::Date32(Some(*x))),
        // TODO: timezone
        LiteralType::Timestamp(x) => Ok(ScalarValue::TimestampMicrosecond(Some(*x), None)),
        LiteralType::TimestampNtz(x) => Ok(ScalarValue::TimestampMicrosecond(Some(*x), None)),
        LiteralType::CalendarInterval(x) => Ok(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNanoType::make_value(x.months, x.days, x.microseconds * 1000),
        ))),
        LiteralType::YearMonthInterval(x) => Ok(ScalarValue::IntervalYearMonth(Some(*x))),
        LiteralType::DayTimeInterval(x) => Ok(ScalarValue::IntervalDayTime(Some(*x))),
        LiteralType::Array(_) => Err(SparkError::todo("literal array")),
        LiteralType::Map(_) => Err(SparkError::todo("literal map")),
        LiteralType::Struct(_) => Err(SparkError::todo("literal struct")),
    }
}

fn get_one_argument(mut args: Vec<expr::Expr>) -> SparkResult<Box<expr::Expr>> {
    if args.len() != 1 {
        return Err(SparkError::invalid("unary operator requires 1 argument"));
    }
    Ok(Box::new(args.pop().unwrap()))
}

fn get_two_arguments(mut args: Vec<expr::Expr>) -> SparkResult<(Box<expr::Expr>, Box<expr::Expr>)> {
    if args.len() != 2 {
        return Err(SparkError::invalid("binary operator requires 2 arguments"));
    }
    let right = Box::new(args.pop().unwrap());
    let left = Box::new(args.pop().unwrap());
    Ok((left, right))
}

pub(crate) fn get_scalar_function(
    name: &str,
    mut args: Vec<expr::Expr>,
) -> SparkResult<expr::Expr> {
    let op = match name {
        ">" => Some(Operator::Gt),
        ">=" => Some(Operator::GtEq),
        "<" => Some(Operator::Lt),
        "<=" => Some(Operator::LtEq),
        "+" => Some(Operator::Plus),
        "-" => Some(Operator::Minus),
        "*" => Some(Operator::Multiply),
        "/" => Some(Operator::Divide),
        "%" => Some(Operator::Modulo),
        "<=>" => Some(Operator::Eq), // TODO: null safe equality
        "and" => Some(Operator::And),
        "or" => Some(Operator::Or),
        "|" => Some(Operator::BitwiseOr),
        "&" => Some(Operator::BitwiseAnd),
        "^" => Some(Operator::BitwiseXor),
        "==" => Some(Operator::Eq),
        "!=" => Some(Operator::NotEq),
        "shiftleft" => Some(Operator::BitwiseShiftLeft),
        "shiftright" => Some(Operator::BitwiseShiftRight),
        _ => None,
    };
    if let Some(op) = op {
        let (left, right) = get_two_arguments(args)?;
        return Ok(expr::Expr::BinaryExpr(expr::BinaryExpr { left, op, right }));
    }

    match name {
        "isnull" => {
            let expr = get_one_argument(args)?;
            return Ok(expr::Expr::IsNull(expr));
        }
        "isnotnull" => {
            let expr = get_one_argument(args)?;
            return Ok(expr::Expr::IsNotNull(expr));
        }
        "negative" => {
            let expr = get_one_argument(args)?;
            return Ok(expr::Expr::Negative(expr));
        }
        "not" => {
            let expr = get_one_argument(args)?;
            return Ok(expr::Expr::Not(expr));
        }
        "like" => {
            let (expr, pattern) = get_two_arguments(args)?;
            return Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr,
                pattern,
                case_insensitive: false,
                escape_char: None,
            }));
        }
        "ilike" => {
            let (expr, pattern) = get_two_arguments(args)?;
            return Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr,
                pattern,
                case_insensitive: true,
                escape_char: None,
            }));
        }
        "rlike" => {
            let (expr, pattern) = get_two_arguments(args)?;
            return Ok(expr::Expr::SimilarTo(expr::Like {
                negated: false,
                expr,
                pattern,
                case_insensitive: false,
                escape_char: None,
            }));
        }
        // TODO: contains
        "startswith" => {
            return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: expr::ScalarFunctionDefinition::BuiltIn(
                    BuiltinScalarFunction::StartsWith,
                ),
                args,
            }));
        }
        // TODO: endswith
        "array" => {
            return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: expr::ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args,
            }));
        }
        "avg" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Avg),
                args,
                distinct: false,
                filter: None,
                order_by: None,
            }));
        }
        "sum" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Sum),
                args,
                distinct: false,
                filter: None,
                order_by: None,
            }));
        }
        "count" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Count),
                args,
                distinct: false,
                filter: None,
                order_by: None,
            }));
        }
        "max" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Max),
                args,
                distinct: false,
                filter: None,
                order_by: None,
            }));
        }
        "min" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Min),
                args,
                distinct: false,
                filter: None,
                order_by: None,
            }));
        }
        "in" => {
            if args.is_empty() {
                return Err(SparkError::invalid("in requires at least 1 argument"));
            }
            let expr = args.remove(0);
            return Ok(expr::Expr::InList(expr::InList {
                expr: Box::new(expr),
                list: args,
                negated: false,
            }));
        }
        _ => {}
    }

    match name {
        "array_repeat" => {
            if args.len() != 2 {
                return Err(SparkError::invalid("array_repeat requires 2 arguments"));
            }
            // DataFusion requires the repeat count to be int64.
            let count = args.pop().unwrap();
            let count = expr::Expr::Cast(expr::Cast {
                expr: Box::new(count),
                data_type: DataType::Int64,
            });
            args.push(count);
        }
        "regexp_replace" => {
            if args.len() != 3 {
                return Err(SparkError::invalid("regexp_replace requires 3 arguments"));
            }
            // Spark replaces all occurrences of the pattern.
            args.push(expr::Expr::Literal(ScalarValue::Utf8(Some(
                "g".to_string(),
            ))));
        }
        _ => {}
    }
    Ok(expr::Expr::ScalarFunction(ScalarFunction {
        func_def: ScalarFunctionDefinition::BuiltIn(name.parse()?),
        args,
    }))
}
