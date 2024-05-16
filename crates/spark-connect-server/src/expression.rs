use std::sync::Arc;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::extension::function::alias::MultiAlias;
use crate::extension::function::contains::Contains;
use crate::extension::function::struct_function::StructFunction;
use crate::schema::{from_spark_built_in_data_type, parse_spark_data_type_string};
use crate::spark::connect as sc;
use crate::sql::parser::new_sql_parser;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNanoType};
use datafusion::catalog::TableReference;
use datafusion::common::{Column, DFSchema, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion::{functions, functions_array};
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::DataFusionError;
use datafusion_expr::{
    expr, window_frame, AggregateFunction, AggregateUDF, BuiltInWindowFunction,
    BuiltinScalarFunction, ExprSchemable, GetFieldAccess, GetIndexedField, Operator,
    ScalarFunctionDefinition, ScalarUDF, TableSource, WindowUDF,
};
use framework_python::partial_python_udf::PartialPythonUDF;
use sqlparser::ast;

#[derive(Default)]
pub(crate) struct EmptyContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for EmptyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        Err(DataFusionError::NotImplemented(format!(
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

    fn udfs_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udafs_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwfs_names(&self) -> Vec<String> {
        Vec::new()
    }
}

pub(crate) fn from_spark_sort_order(
    sort: &sc::expression::SortOrder,
    schema: &DFSchema,
) -> SparkResult<expr::Expr> {
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

pub(crate) fn from_spark_window_frame(
    frame: &sc::expression::window::WindowFrame,
) -> SparkResult<window_frame::WindowFrame> {
    use sc::expression::window::window_frame::frame_boundary::Boundary;
    use sc::expression::window::window_frame::FrameType;

    let frame_type = FrameType::try_from(frame.frame_type)?;
    let lower = frame
        .lower
        .as_ref()
        .required("lower boundary")?
        .boundary
        .as_ref()
        .required("lower boundary")?;
    let upper = frame
        .upper
        .as_ref()
        .required("upper boundary")?
        .boundary
        .as_ref()
        .required("upper boundary")?;

    let units = match frame_type {
        FrameType::Undefined => return Err(SparkError::invalid("undefined frame type")),
        FrameType::Row => window_frame::WindowFrameUnits::Rows,
        FrameType::Range => window_frame::WindowFrameUnits::Range,
    };

    fn from_boundary_value(value: &sc::Expression) -> SparkResult<ScalarValue> {
        let value = from_spark_expression(value, &DFSchema::empty())?;
        match value {
            expr::Expr::Literal(
                v @ (ScalarValue::UInt32(_)
                | ScalarValue::Int32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int64(_)),
            ) => Ok(v),
            _ => Err(SparkError::invalid(format!(
                "invalid boundary value: {:?}",
                value
            ))),
        }
    }

    let start = match lower {
        Boundary::CurrentRow(true) => window_frame::WindowFrameBound::CurrentRow,
        Boundary::Unbounded(true) => {
            window_frame::WindowFrameBound::Preceding(ScalarValue::UInt64(None))
        }
        Boundary::Value(value) => {
            window_frame::WindowFrameBound::Preceding(from_boundary_value(value)?)
        }
        _ => {
            return Err(SparkError::invalid(format!(
                "invalid lower boundary: {:?}",
                lower
            )));
        }
    };
    let end = match upper {
        Boundary::CurrentRow(true) => window_frame::WindowFrameBound::CurrentRow,
        Boundary::Unbounded(true) => {
            window_frame::WindowFrameBound::Following(ScalarValue::UInt64(None))
        }
        Boundary::Value(value) => {
            window_frame::WindowFrameBound::Following(from_boundary_value(value)?)
        }
        _ => {
            return Err(SparkError::invalid(format!(
                "invalid upper boundary: {:?}",
                upper
            )));
        }
    };
    Ok(window_frame::WindowFrame::new_bounds(units, start, end))
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
            let _plan_id = attr.plan_id;
            Ok(expr::Expr::Column(col))
        }
        ExprType::UnresolvedFunction(func) => {
            if func.is_user_defined_function {
                return Err(SparkError::unsupported("user defined function"));
            }
            if func.is_distinct {
                return Err(SparkError::unsupported("distinct function"));
            }

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
                ast::Expr::Wildcard => Ok(expr::Expr::Wildcard { qualifier: None }),
                ast::Expr::QualifiedWildcard(ast::ObjectName(name)) => {
                    let qualifier = name
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(".");
                    Ok(expr::Expr::Wildcard {
                        qualifier: Some(qualifier),
                    })
                }
                expr => {
                    let provider = EmptyContextProvider::default();
                    let planner = SqlToRel::new(&provider);
                    let expr = planner.sql_to_expr(expr, schema, &mut PlannerContext::default())?;
                    Ok(expr)
                }
            }
        }
        ExprType::UnresolvedStar(star) => {
            // FIXME: column reference is parsed as qualifier
            if let Some(target) = &star.unparsed_target {
                let mut parser = new_sql_parser(target)?;
                let expr = parser.parse_wildcard_expr()?;
                match expr {
                    ast::Expr::Wildcard => Ok(expr::Expr::Wildcard { qualifier: None }),
                    ast::Expr::QualifiedWildcard(ast::ObjectName(names)) => {
                        let qualifier = names
                            .iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(".");
                        Ok(expr::Expr::Wildcard {
                            qualifier: Some(qualifier),
                        })
                    }
                    _ => Err(SparkError::todo("expression as wildcard target")),
                }
            } else {
                Ok(expr::Expr::Wildcard { qualifier: None })
            }
        }
        ExprType::Alias(alias) => {
            let expr = alias.expr.as_ref().required("expression for alias")?;
            let expr = from_spark_expression(expr, schema)?;
            if let [name] = alias.name.as_slice() {
                Ok(expr::Expr::Alias(expr::Alias {
                    expr: Box::new(expr),
                    relation: None,
                    name: name.to_string(),
                }))
            } else {
                Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                    func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                        MultiAlias::new(alias.name.clone()),
                    ))),
                    args: vec![expr],
                }))
            }
        }
        ExprType::Cast(cast) => {
            use sc::expression::cast::CastToType;

            let expr = cast.expr.as_ref().required("expression for cast")?;
            let data_type = cast.cast_to_type.as_ref().required("data type for cast")?;
            let data_type = match data_type {
                CastToType::Type(t) => from_spark_built_in_data_type(&t)?,
                CastToType::TypeStr(s) => parse_spark_data_type_string(s)?,
            };
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(from_spark_expression(expr, schema)?),
                data_type,
            }))
        }
        ExprType::UnresolvedRegex(_) => Err(SparkError::todo("unresolved regex")),
        ExprType::SortOrder(sort) => from_spark_sort_order(sort.as_ref(), schema),
        ExprType::LambdaFunction(_) => Err(SparkError::todo("lambda function")),
        ExprType::Window(window) => {
            let func = window
                .window_function
                .as_ref()
                .required("window function")?
                .expr_type
                .as_ref()
                .required("window function expression")?;
            let (func_name, args) = match func {
                ExprType::UnresolvedFunction(sc::expression::UnresolvedFunction {
                    function_name,
                    arguments,
                    is_user_defined_function,
                    is_distinct,
                }) => {
                    if *is_user_defined_function {
                        return Err(SparkError::unsupported("user defined window function"));
                    }
                    if *is_distinct {
                        return Err(SparkError::unsupported("distinct window function"));
                    }
                    let args = arguments
                        .iter()
                        .map(|x| from_spark_expression(x, schema))
                        .collect::<SparkResult<Vec<_>>>()?;
                    (function_name, args)
                }
                ExprType::CommonInlineUserDefinedFunction(_) => {
                    return Err(SparkError::unsupported(
                        "inline user defined window function",
                    ));
                }
                _ => {
                    return Err(SparkError::invalid(format!(
                        "invalid window function expression: {:?}",
                        func
                    )));
                }
            };
            let partition_by = window
                .partition_spec
                .iter()
                .map(|x| from_spark_expression(x, schema))
                .collect::<SparkResult<Vec<_>>>()?;
            let order_by = window
                .order_spec
                .iter()
                .map(|x| from_spark_sort_order(x, schema))
                .collect::<SparkResult<Vec<_>>>()?;
            let window_frame = if let Some(frame) = window.frame_spec.as_ref() {
                from_spark_window_frame(frame)?
            } else {
                window_frame::WindowFrame::new(None)
            };
            Ok(expr::Expr::WindowFunction(expr::WindowFunction {
                fun: get_window_function(func_name.as_str())?,
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment: None,
            }))
        }
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
            use framework_python::partial_python_udf::deserialize_partial_python_udf;
            use framework_python::udf::PythonUDF;
            use pyo3::prelude::*;
            use sc::common_inline_user_defined_function::Function::PythonUdf;
            use sc::PythonUdf as PythonUDFStruct;

            let function_name: &str = &udf.function_name;

            let deterministic: bool = udf.deterministic;

            let arguments: Vec<expr::Expr> = udf
                .arguments
                .iter()
                .map(|x| from_spark_expression(x, schema))
                .collect::<SparkResult<Vec<expr::Expr>>>()?;

            let input_types: Vec<DataType> = arguments
                .iter()
                .map(|arg| arg.get_type(schema))
                .collect::<Result<Vec<DataType>, DataFusionError>>()?;

            let function: &PythonUDFStruct = match &udf.function {
                Some(PythonUdf(function)) => function,
                _ => {
                    return Err(SparkError::invalid("UDF function type must be Python UDF"));
                }
            };

            let output_type: DataType = from_spark_built_in_data_type(
                function
                    .output_type
                    .as_ref()
                    .required("UDF Function output type")?,
            )?;

            let eval_type: i32 = function.eval_type;

            let command: &[u8] = &function.command;

            let python_ver: &str = &function.python_ver;

            let pyo3_python_version: String = Python::with_gil(|py| py.version().to_string());

            if !pyo3_python_version.starts_with(python_ver) {
                return Err(SparkError::invalid(format!(
                    "Python version mismatch. Version used to compile the UDF must match the version used to run the UDF. Version used to compile the UDF: {:?}. Version used to run the UDF: {:?}",
                    python_ver,
                    pyo3_python_version,
                )));
            }

            let python_function: PartialPythonUDF = deserialize_partial_python_udf(command)
                .map_err(|e| {
                    SparkError::invalid(format!("Python UDF deserialization error: {:?}", e))
                })?;

            let python_udf: PythonUDF = PythonUDF::new(
                function_name.to_owned(),
                deterministic,
                input_types,
                python_function,
                output_type,
                eval_type,
            );

            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(python_udf))),
                args: arguments,
            }))
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
        LiteralType::Decimal(x) => {
            let (value, precision, scale) = match x.value.parse::<i128>() {
                Ok(v) => (v, x.precision() as u8, x.scale() as i8),
                Err(_) => {
                    return Err(SparkError::invalid(format!(
                        "Failed to parse decimal value {:?}",
                        Some(x.value.clone())
                    )));
                }
            };
            Ok(ScalarValue::Decimal128(Some(value), precision, scale))
        }
        LiteralType::String(x) => Ok(ScalarValue::Utf8(Some(x.clone()))),
        LiteralType::Date(x) => Ok(ScalarValue::Date32(Some(*x))),
        LiteralType::Timestamp(x) => {
            // TODO: should we use "spark.sql.session.timeZone"?
            let timezone: Arc<str> = Arc::from("UTC");
            Ok(ScalarValue::TimestampMicrosecond(Some(*x), Some(timezone)))
        }
        LiteralType::TimestampNtz(x) => Ok(ScalarValue::TimestampMicrosecond(Some(*x), None)),
        LiteralType::CalendarInterval(x) => Ok(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNanoType::make_value(x.months, x.days, x.microseconds * 1000),
        ))),
        LiteralType::YearMonthInterval(x) => Ok(ScalarValue::IntervalYearMonth(Some(*x))),
        LiteralType::DayTimeInterval(x) => Ok(ScalarValue::IntervalDayTime(Some(*x))),
        LiteralType::Array(array) => {
            // TODO: Validate that this works
            let element_type: &sc::DataType =
                array.element_type.as_ref().required("array element type")?;
            let element_type: DataType = from_spark_built_in_data_type(element_type)?;
            let scalars: Vec<ScalarValue> = array
                .elements
                .iter()
                .map(|literal| from_spark_literal_to_scalar(literal))
                .collect::<SparkResult<Vec<_>>>()?;
            Ok(ScalarValue::List(ScalarValue::new_list(
                &scalars,
                &element_type,
            )))
        }
        LiteralType::Map(_map) => Err(SparkError::todo("LiteralType::Map")),
        LiteralType::Struct(r#struct) => {
            // TODO: Validate that this works
            let struct_type: &sc::DataType =
                r#struct.struct_type.as_ref().required("struct type")?;
            let struct_type: DataType = from_spark_built_in_data_type(struct_type)?;
            let fields = match &struct_type {
                DataType::Struct(fields) => fields,
                _ => return Err(SparkError::invalid("expected struct type")),
            };

            let mut builder: ScalarStructBuilder = ScalarStructBuilder::new();
            for (literal, field) in r#struct.elements.iter().zip(fields.iter()) {
                let scalar = from_spark_literal_to_scalar(literal)?;
                builder = builder.with_scalar(field, scalar);
            }
            Ok(builder.build()?)
        }
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
    use crate::extension::function::explode::Explode;

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

    // TODO: Add all functions::expr_fn and all functions_array::expr_fn
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
        "contains" => {
            // TODO: Validate that this works
            return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(Contains::new()))),
                args: args,
            }));
        }
        "startswith" => {
            let (left, right) = get_two_arguments(args)?;
            return Ok(functions::expr_fn::starts_with(*left, *right));
        }
        "endswith" => {
            return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::EndsWith),
                args,
            }));
        }
        "array" => {
            return Ok(functions_array::expr_fn::make_array(args));
        }
        "array_has" | "array_contains" => {
            let (left, right) = get_two_arguments(args)?;
            return Ok(functions_array::expr_fn::array_has(*left, *right));
        }
        "array_has_all" | "array_contains_all" => {
            let (left, right) = get_two_arguments(args)?;
            return Ok(functions_array::expr_fn::array_has_all(*left, *right));
        }
        "array_has_any" | "array_contains_any" => {
            let (left, right) = get_two_arguments(args)?;
            return Ok(functions_array::expr_fn::array_has_any(*left, *right));
        }
        "array_repeat" => {
            let (element, count) = get_two_arguments(args)?;
            let count = expr::Expr::Cast(expr::Cast {
                expr: count,
                data_type: DataType::Int64,
            });
            return Ok(functions_array::expr_fn::array_repeat(*element, count));
        }
        "avg" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Avg),
                args,
                distinct: false,
                filter: None,
                order_by: None,
                null_treatment: None,
            }));
        }
        "sum" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Sum),
                args,
                distinct: false,
                filter: None,
                order_by: None,
                null_treatment: None,
            }));
        }
        "count" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Count),
                args,
                distinct: false,
                filter: None,
                order_by: None,
                null_treatment: None,
            }));
        }
        "max" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Max),
                args,
                distinct: false,
                filter: None,
                order_by: None,
                null_treatment: None,
            }));
        }
        "min" => {
            return Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
                func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Min),
                args,
                distinct: false,
                filter: None,
                order_by: None,
                null_treatment: None,
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
        "abs" => {
            let expr = get_one_argument(args)?;
            return Ok(functions::expr_fn::abs(*expr));
        }
        name @ ("explode" | "explode_outer" | "posexplode" | "posexplode_outer") => {
            let udf = ScalarUDF::from(Explode::new(name));
            return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(udf)),
                args,
            }));
        }
        "regexp_replace" => {
            if args.len() != 3 {
                return Err(SparkError::invalid("regexp_replace requires 3 arguments"));
            }
            // Spark replaces all occurrences of the pattern.
            args.push(expr::Expr::Literal(ScalarValue::Utf8(Some(
                "g".to_string(),
            ))));
            return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                    functions::regex::regexpreplace::RegexpReplaceFunc::new(),
                ))),
                args: args,
            }));
        }
        "timestamp" | "to_timestamp" => {
            return Ok(functions::expr_fn::to_timestamp_micros(args));
        }
        "unix_timestamp" | "to_unixtime" => {
            return Ok(functions::expr_fn::to_unixtime(args));
        }
        "struct" => {
            let field_names: Vec<String> = args.iter().map(|x| {
                match x {
                    expr::Expr::Column(column) => Ok(column.name.to_string()),
                    _ => {
                        Err(SparkError::invalid("get_scalar_function: struct function should have expr::Expr::Column as arguments"))
                    }
                }
            }).collect::<SparkResult<Vec<String>>>()?;
            return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                    StructFunction::new(field_names),
                ))),
                args: args,
            }));
        }
        _ => {}
    }

    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func_def: ScalarFunctionDefinition::BuiltIn(name.parse()?),
        args,
    }))
}

pub(crate) fn get_window_function(name: &str) -> SparkResult<expr::WindowFunctionDefinition> {
    match name {
        "avg" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Avg,
        )),
        "min" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Min,
        )),
        "max" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Max,
        )),
        "sum" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Sum,
        )),
        "count" => Ok(expr::WindowFunctionDefinition::AggregateFunction(
            AggregateFunction::Count,
        )),
        "row_number" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::RowNumber,
        )),
        "rank" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Rank,
        )),
        "dense_rank" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::DenseRank,
        )),
        "percent_rank" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::PercentRank,
        )),
        "ntile" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Ntile,
        )),
        "first_value" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::FirstValue,
        )),
        "last_value" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::LastValue,
        )),
        "nth_value" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::NthValue,
        )),
        "lead" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Lead,
        )),
        "lag" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::Lag,
        )),
        "cume_dist" => Ok(expr::WindowFunctionDefinition::BuiltInWindowFunction(
            BuiltInWindowFunction::CumeDist,
        )),
        s => Err(SparkError::invalid(format!(
            "unknown window function: {}",
            s
        ))),
    }
}
