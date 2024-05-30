use std::sync::Arc;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::alias::MultiAlias;
use crate::extension::function::contains::Contains;
use crate::extension::function::map_function::MapFunction;
use crate::extension::function::struct_function::StructFunction;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableReference;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::sql::planner::ContextProvider;
use datafusion::{functions, functions_array};
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::{
    expr, window_frame, AggregateFunction, AggregateUDF, BuiltInWindowFunction, ExprSchemable,
    GetFieldAccess, GetIndexedField, Operator, ScalarFunctionDefinition, ScalarUDF, TableSource,
    WindowUDF,
};
use framework_common::spec;
use framework_python::partial_python_udf::PartialPythonUDF;

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
    sort: spec::SortOrder,
    schema: &DFSchema,
) -> PlanResult<expr::Expr> {
    use spec::{NullOrdering, SortDirection};

    let spec::SortOrder {
        child,
        direction,
        null_ordering,
    } = sort;
    let asc = match direction {
        SortDirection::Ascending => true,
        SortDirection::Descending => false,
        SortDirection::Unspecified => true,
    };
    let nulls_first = match null_ordering {
        NullOrdering::NullsFirst => true,
        NullOrdering::NullsLast => false,
        NullOrdering::Unspecified => asc,
    };
    Ok(expr::Expr::Sort(expr::Sort {
        expr: Box::new(from_spark_expression(*child, schema)?),
        asc,
        nulls_first,
    }))
}

pub(crate) fn from_spark_window_frame(
    frame: spec::WindowFrame,
) -> PlanResult<window_frame::WindowFrame> {
    use spec::{WindowFrameBoundary, WindowFrameType};

    let spec::WindowFrame {
        frame_type,
        lower,
        upper,
    } = frame;

    let units = match frame_type {
        WindowFrameType::Undefined => return Err(PlanError::invalid("undefined frame type")),
        WindowFrameType::Row => window_frame::WindowFrameUnits::Rows,
        WindowFrameType::Range => window_frame::WindowFrameUnits::Range,
    };
    let start = match lower {
        WindowFrameBoundary::CurrentRow => window_frame::WindowFrameBound::CurrentRow,
        WindowFrameBoundary::Unbounded => {
            window_frame::WindowFrameBound::Preceding(ScalarValue::UInt64(None))
        }
        WindowFrameBoundary::Value(value) => {
            window_frame::WindowFrameBound::Preceding(from_spark_window_boundary_value(*value)?)
        }
    };
    let end = match upper {
        WindowFrameBoundary::CurrentRow => window_frame::WindowFrameBound::CurrentRow,
        WindowFrameBoundary::Unbounded => {
            window_frame::WindowFrameBound::Following(ScalarValue::UInt64(None))
        }
        WindowFrameBoundary::Value(value) => {
            window_frame::WindowFrameBound::Following(from_spark_window_boundary_value(*value)?)
        }
    };
    Ok(window_frame::WindowFrame::new_bounds(units, start, end))
}

fn from_spark_window_boundary_value(value: spec::Expr) -> PlanResult<ScalarValue> {
    let value = from_spark_expression(value, &DFSchema::empty())?;
    match value {
        expr::Expr::Literal(
            v @ (ScalarValue::UInt32(_)
            | ScalarValue::Int32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Int64(_)),
        ) => Ok(v),
        _ => Err(PlanError::invalid(format!(
            "invalid boundary value: {:?}",
            value
        ))),
    }
}

pub(crate) fn from_spark_expression(expr: spec::Expr, schema: &DFSchema) -> PlanResult<expr::Expr> {
    use spec::Expr;

    match expr {
        Expr::Literal(literal) => Ok(expr::Expr::Literal(literal.try_into()?)),
        Expr::UnresolvedAttribute {
            identifier,
            plan_id: _,
        } => {
            // FIXME: resolve identifier using schema
            let column: Vec<String> = identifier.into();
            Ok(expr::Expr::Column(Column::new_unqualified(
                column.join("."),
            )))
        }
        Expr::UnresolvedFunction {
            function_name,
            arguments,
            is_distinct,
            is_user_defined_function,
        } => {
            if is_user_defined_function {
                return Err(PlanError::unsupported("user defined function"));
            }
            if is_distinct {
                return Err(PlanError::unsupported("distinct function"));
            }

            let args = arguments
                .into_iter()
                .map(|x| from_spark_expression(x, schema))
                .collect::<PlanResult<Vec<_>>>()?;
            Ok(get_scalar_function(function_name.as_str(), args)?)
        }
        Expr::UnresolvedStar { target } => {
            // FIXME: column reference is parsed as qualifier
            if let Some(target) = target {
                let target: Vec<String> = target.into();
                Ok(expr::Expr::Wildcard {
                    qualifier: Some(target.join(".")),
                })
            } else {
                Ok(expr::Expr::Wildcard { qualifier: None })
            }
        }
        Expr::Alias {
            expr,
            name,
            metadata,
        } => {
            if metadata.is_some() {
                return Err(PlanError::unsupported("alias metadata"));
            }
            let expr = from_spark_expression(*expr, schema)?;
            if let [name] = name.as_slice() {
                Ok(expr::Expr::Alias(expr::Alias {
                    expr: Box::new(expr),
                    relation: None,
                    name: name.clone().into(),
                }))
            } else {
                let name: Vec<String> = name.into_iter().map(|x| x.into()).collect();
                Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                    func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                        MultiAlias::new(name),
                    ))),
                    args: vec![expr],
                }))
            }
        }
        Expr::Cast { expr, cast_to_type } => {
            let data_type = cast_to_type.try_into()?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(from_spark_expression(*expr, schema)?),
                data_type,
            }))
        }
        Expr::UnresolvedRegex { .. } => Err(PlanError::todo("unresolved regex")),
        Expr::SortOrder(sort) => from_spark_sort_order(sort, schema),
        Expr::LambdaFunction { .. } => Err(PlanError::todo("lambda function")),
        Expr::Window {
            window_function,
            partition_spec,
            order_spec,
            frame_spec,
        } => {
            let (func_name, args) = match *window_function {
                Expr::UnresolvedFunction {
                    function_name,
                    arguments,
                    is_user_defined_function,
                    is_distinct,
                } => {
                    if is_user_defined_function {
                        return Err(PlanError::unsupported("user defined window function"));
                    }
                    if is_distinct {
                        return Err(PlanError::unsupported("distinct window function"));
                    }
                    let args = arguments
                        .into_iter()
                        .map(|x| from_spark_expression(x, schema))
                        .collect::<PlanResult<Vec<_>>>()?;
                    (function_name, args)
                }
                Expr::CommonInlineUserDefinedFunction(_) => {
                    return Err(PlanError::unsupported(
                        "inline user defined window function",
                    ));
                }
                _ => {
                    return Err(PlanError::invalid(format!(
                        "invalid window function expression: {:?}",
                        window_function
                    )));
                }
            };
            let partition_by = partition_spec
                .into_iter()
                .map(|x| from_spark_expression(x, schema))
                .collect::<PlanResult<Vec<_>>>()?;
            let order_by = order_spec
                .into_iter()
                .map(|x| from_spark_sort_order(x, schema))
                .collect::<PlanResult<Vec<_>>>()?;
            let window_frame = if let Some(frame) = frame_spec {
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
        Expr::UnresolvedExtractValue { child, extraction } => {
            use spec::Literal;

            let literal = match *extraction {
                Expr::Literal(literal) => literal,
                _ => {
                    return Err(PlanError::invalid("extraction must be a literal"));
                }
            };
            let field = match literal {
                Literal::Byte(x) => GetFieldAccess::ListIndex {
                    key: Box::new(expr::Expr::Literal(ScalarValue::Int64(Some(x as i64)))),
                },
                Literal::Short(x) => GetFieldAccess::ListIndex {
                    key: Box::new(expr::Expr::Literal(ScalarValue::Int64(Some(x as i64)))),
                },
                Literal::Integer(x) => GetFieldAccess::ListIndex {
                    key: Box::new(expr::Expr::Literal(ScalarValue::Int64(Some(x as i64)))),
                },
                Literal::Long(x) => GetFieldAccess::ListIndex {
                    key: Box::new(expr::Expr::Literal(ScalarValue::Int64(Some(x)))),
                },
                Literal::String(s) => GetFieldAccess::NamedStructField {
                    name: ScalarValue::Utf8(Some(s.clone())),
                },
                _ => {
                    return Err(PlanError::invalid("invalid extraction value"));
                }
            };
            Ok(expr::Expr::GetIndexedField(GetIndexedField {
                expr: Box::new(from_spark_expression(*child, schema)?),
                field,
            }))
        }
        Expr::UpdateFields { .. } => Err(PlanError::todo("update fields")),
        Expr::UnresolvedNamedLambdaVariable(_) => {
            Err(PlanError::todo("unresolved named lambda variable"))
        }
        Expr::CommonInlineUserDefinedFunction(function) => {
            use framework_python::partial_python_udf::deserialize_partial_python_udf;
            use framework_python::udf::PythonUDF;
            use pyo3::prelude::*;

            let spec::CommonInlineUserDefinedFunction {
                function_name,
                deterministic,
                arguments,
                function,
            } = function;

            let function_name: &str = function_name.as_str();
            let arguments: Vec<expr::Expr> = arguments
                .into_iter()
                .map(|x| from_spark_expression(x, schema))
                .collect::<PlanResult<Vec<expr::Expr>>>()?;
            let input_types: Vec<DataType> = arguments
                .iter()
                .map(|arg| arg.get_type(schema))
                .collect::<Result<Vec<DataType>, DataFusionError>>()?;

            let (output_type, eval_type, command, python_version) = match function {
                spec::FunctionDefinition::PythonUdf {
                    output_type,
                    eval_type,
                    command,
                    python_version,
                } => (output_type, eval_type, command, python_version),
                _ => {
                    return Err(PlanError::invalid("UDF function type must be Python UDF"));
                }
            };
            let output_type: DataType = output_type.try_into()?;

            let pyo3_python_version: String = Python::with_gil(|py| py.version().to_string());
            if !pyo3_python_version.starts_with(python_version.as_str()) {
                return Err(PlanError::invalid(format!(
                    "Python version mismatch. Version used to compile the UDF must match the version used to run the UDF. Version used to compile the UDF: {:?}. Version used to run the UDF: {:?}",
                    python_version,
                    pyo3_python_version,
                )));
            }

            let python_function: PartialPythonUDF = deserialize_partial_python_udf(&command)
                .map_err(|e| {
                    PlanError::invalid(format!("Python UDF deserialization error: {:?}", e))
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
        Expr::CallFunction { .. } => Err(PlanError::todo("call function")),
        Expr::Placeholder(placeholder) => Ok(expr::Expr::Placeholder(expr::Placeholder::new(
            placeholder,
            None,
        ))),
    }
}

fn get_one_argument(mut args: Vec<expr::Expr>) -> PlanResult<Box<expr::Expr>> {
    if args.len() != 1 {
        return Err(PlanError::invalid("unary operator requires 1 argument"));
    }
    Ok(Box::new(args.pop().unwrap()))
}

fn get_two_arguments(mut args: Vec<expr::Expr>) -> PlanResult<(Box<expr::Expr>, Box<expr::Expr>)> {
    if args.len() != 2 {
        return Err(PlanError::invalid("binary operator requires 2 arguments"));
    }
    let right = Box::new(args.pop().unwrap());
    let left = Box::new(args.pop().unwrap());
    Ok((left, right))
}

pub(crate) fn get_scalar_function(name: &str, mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    use crate::extension::function::explode::Explode;

    let name = name.to_lowercase();

    match name.as_str() {
        "+" if args.len() <= 1 => {
            let arg = get_one_argument(args)?;
            return Ok(*arg);
        }
        "-" if args.len() <= 1 => {
            let arg = get_one_argument(args)?;
            return Ok(expr::Expr::Negative(arg));
        }
        _ => {}
    };

    let op = match name.as_str() {
        ">" => Some(Operator::Gt),
        ">=" => Some(Operator::GtEq),
        "<" => Some(Operator::Lt),
        "<=" => Some(Operator::LtEq),
        "+" if args.len() > 1 => Some(Operator::Plus),
        "-" if args.len() > 1 => Some(Operator::Minus),
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
    match name.as_str() {
        "isnull" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::IsNull(expr))
        }
        "isnotnull" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::IsNotNull(expr))
        }
        "negative" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Negative(expr))
        }
        "not" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Not(expr))
        }
        "like" => {
            let (expr, pattern) = get_two_arguments(args)?;
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr,
                pattern,
                case_insensitive: false,
                escape_char: None,
            }))
        }
        "ilike" => {
            let (expr, pattern) = get_two_arguments(args)?;
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr,
                pattern,
                case_insensitive: true,
                escape_char: None,
            }))
        }
        "rlike" => {
            let (expr, pattern) = get_two_arguments(args)?;
            Ok(expr::Expr::SimilarTo(expr::Like {
                negated: false,
                expr,
                pattern,
                case_insensitive: false,
                escape_char: None,
            }))
        }
        "contains" => {
            // TODO: Validate that this works
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(Contains::new()))),
                args: args,
            }))
        }
        "startswith" => {
            let (left, right) = get_two_arguments(args)?;
            Ok(functions::expr_fn::starts_with(*left, *right))
        }
        "endswith" => {
            let (left, right) = get_two_arguments(args)?;
            Ok(functions::expr_fn::ends_with(*left, *right))
        }
        "array" => Ok(functions_array::expr_fn::make_array(args)),
        "array_has" | "array_contains" => {
            let (left, right) = get_two_arguments(args)?;
            Ok(functions_array::expr_fn::array_has(*left, *right))
        }
        "array_has_all" | "array_contains_all" => {
            let (left, right) = get_two_arguments(args)?;
            Ok(functions_array::expr_fn::array_has_all(*left, *right))
        }
        "array_has_any" | "array_contains_any" => {
            let (left, right) = get_two_arguments(args)?;
            Ok(functions_array::expr_fn::array_has_any(*left, *right))
        }
        "array_repeat" => {
            let (element, count) = get_two_arguments(args)?;
            let count = expr::Expr::Cast(expr::Cast {
                expr: count,
                data_type: DataType::Int64,
            });
            Ok(functions_array::expr_fn::array_repeat(*element, count))
        }
        "avg" => Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
            func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Avg),
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })),
        "sum" => Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
            func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Sum),
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })),
        "count" => Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
            func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Count),
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })),
        "max" => Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
            func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Max),
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })),
        "min" => Ok(expr::Expr::AggregateFunction(expr::AggregateFunction {
            func_def: expr::AggregateFunctionDefinition::BuiltIn(AggregateFunction::Min),
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })),
        "in" => {
            if args.is_empty() {
                return Err(PlanError::invalid("in requires at least 1 argument"));
            }
            let expr = args.remove(0);
            Ok(expr::Expr::InList(expr::InList {
                expr: Box::new(expr),
                list: args,
                negated: false,
            }))
        }
        "abs" => {
            let expr = get_one_argument(args)?;
            Ok(functions::expr_fn::abs(*expr))
        }
        name @ ("explode" | "explode_outer" | "posexplode" | "posexplode_outer") => {
            let udf = ScalarUDF::from(Explode::new(name));
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(udf)),
                args,
            }))
        }
        "regexp_replace" => {
            if args.len() != 3 {
                return Err(PlanError::invalid("regexp_replace requires 3 arguments"));
            }
            // Planner replaces all occurrences of the pattern.
            args.push(expr::Expr::Literal(ScalarValue::Utf8(Some(
                "g".to_string(),
            ))));
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                    functions::regex::regexpreplace::RegexpReplaceFunc::new(),
                ))),
                args,
            }))
        }
        "date" => Ok(functions::expr_fn::to_date(args)),
        "timestamp" | "to_timestamp" => Ok(functions::expr_fn::to_timestamp_micros(args)),
        "unix_timestamp" | "to_unixtime" => Ok(functions::expr_fn::to_unixtime(args)),
        "struct" => Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                StructFunction::try_new_from_expressions(args.clone())?,
            ))),
            args,
        })),
        "map" => Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(MapFunction::new()))),
            args,
        })),
        "bigint" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Int64,
            }))
        }
        "binary" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Binary,
            }))
        }
        "boolean" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Boolean,
            }))
        }
        "decimal" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Decimal128(10, 0),
            }))
        }
        "double" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Float64,
            }))
        }
        "float" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Float32,
            }))
        }
        "int" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Int32,
            }))
        }
        "smallint" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Int16,
            }))
        }
        "string" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Utf8,
            }))
        }
        "tinyint" => {
            let expr = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr,
                data_type: DataType::Int8,
            }))
        }
        _ => Err(PlanError::invalid(format!("unknown function: {}", name))),
    }
}

pub(crate) fn get_window_function(name: &str) -> PlanResult<expr::WindowFunctionDefinition> {
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
        s => Err(PlanError::invalid(format!(
            "unknown window function: {}",
            s
        ))),
    }
}
