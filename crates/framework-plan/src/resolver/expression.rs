use std::sync::Arc;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::alias::MultiAlias;
use crate::function::{
    get_built_in_aggregate_function, get_built_in_function, get_built_in_window_function,
};
use crate::resolver::PlanResolver;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::{
    expr, window_frame, ExprSchemable, GetFieldAccess, GetIndexedField, ScalarFunctionDefinition,
    ScalarUDF,
};
use framework_common::spec;
use framework_python::cereal::partial_pyspark_udf::{
    deserialize_partial_pyspark_udf, PartialPySparkUDF,
};
use framework_python::udf::pyspark_udf::PySparkUDF;
use framework_python::udf::unresolved_pyspark_udf::UnresolvedPySparkUDF;

impl PlanResolver<'_> {
    pub(crate) fn resolve_sort_order(
        &self,
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
            expr: Box::new(self.resolve_expression(*child, schema)?),
            asc,
            nulls_first,
        }))
    }

    pub(crate) fn resolve_window_frame(
        &self,
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
            WindowFrameBoundary::Value(value) => window_frame::WindowFrameBound::Preceding(
                self.resolve_window_boundary_value(*value)?,
            ),
        };
        let end = match upper {
            WindowFrameBoundary::CurrentRow => window_frame::WindowFrameBound::CurrentRow,
            WindowFrameBoundary::Unbounded => {
                window_frame::WindowFrameBound::Following(ScalarValue::UInt64(None))
            }
            WindowFrameBoundary::Value(value) => window_frame::WindowFrameBound::Following(
                self.resolve_window_boundary_value(*value)?,
            ),
        };
        Ok(window_frame::WindowFrame::new_bounds(units, start, end))
    }

    fn resolve_window_boundary_value(&self, value: spec::Expr) -> PlanResult<ScalarValue> {
        let value = self.resolve_expression(value, &DFSchema::empty())?;
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

    pub fn resolve_expression(
        &self,
        expr: spec::Expr,
        schema: &DFSchema,
    ) -> PlanResult<expr::Expr> {
        use spec::Expr;

        match expr {
            Expr::Literal(literal) => Ok(expr::Expr::Literal(self.resolve_literal(literal)?)),
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
                is_user_defined_function: _, // FIXME: is_user_defined_function is always false.
            } => {
                let arguments = arguments
                    .into_iter()
                    .map(|x| self.resolve_expression(x, schema))
                    .collect::<PlanResult<Vec<expr::Expr>>>()?;
                let input_types: Vec<DataType> = arguments
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<DataType>, DataFusionError>>(
                )?;

                if let Ok(func) = self.ctx.udf(function_name.as_str()) {
                    // TODO: UnresolvedPythonUDF will likely need to be accounted for as well
                    //  once we integrate LakeSail Python UDF.
                    let func = match func.inner().as_any().downcast_ref::<UnresolvedPySparkUDF>() {
                        Some(f) => {
                            let deterministic = f.deterministic()?;
                            let function_definition = f.python_function_definition()?;
                            let (output_type, eval_type, command, python_version) =
                                match &function_definition {
                                    spec::FunctionDefinition::PythonUdf {
                                        output_type,
                                        eval_type,
                                        command,
                                        python_version,
                                    } => (output_type, eval_type, command, python_version),
                                    _ => {
                                        return Err(PlanError::invalid(
                                            "UDF function type must be Python UDF",
                                        ));
                                    }
                                };
                            let output_type: DataType =
                                self.resolve_data_type(output_type.clone())?;

                            let python_function: PartialPySparkUDF =
                                deserialize_partial_pyspark_udf(
                                    &python_version,
                                    &command,
                                    &eval_type,
                                    &(arguments.len() as i32),
                                    &self.config.spark_udf_config,
                                )
                                .map_err(|e| {
                                    PlanError::invalid(format!(
                                        "Python UDF deserialization error: {:?}",
                                        e
                                    ))
                                })?;

                            let python_udf: PySparkUDF = PySparkUDF::new(
                                function_name.to_owned(),
                                deterministic,
                                input_types,
                                eval_type.clone(),
                                python_function,
                                output_type,
                            );

                            Arc::new(ScalarUDF::from(python_udf))
                        }
                        None => func,
                    };

                    return Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                        func_def: ScalarFunctionDefinition::UDF(func),
                        args: arguments,
                    }));
                }

                // TODO: udaf and udwf

                // FIXME: is_user_defined_function is always false
                //  So, we need to check udf's before built-in functions.
                if let Ok(func) = get_built_in_function(function_name.as_str()) {
                    return Ok(func(arguments.clone())?);
                }
                if let Ok(func) = get_built_in_aggregate_function(
                    function_name.as_str(),
                    arguments.clone(),
                    is_distinct,
                ) {
                    return Ok(func);
                }

                return Err(PlanError::unsupported(format!(
                    "Expr::UnresolvedFunction Unknown Function: {}",
                    function_name
                )));
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
                let expr = self.resolve_expression(*expr, schema)?;
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
                let data_type = self.resolve_data_type(cast_to_type)?;
                Ok(expr::Expr::Cast(expr::Cast {
                    expr: Box::new(self.resolve_expression(*expr, schema)?),
                    data_type,
                }))
            }
            Expr::UnresolvedRegex { .. } => Err(PlanError::todo("unresolved regex")),
            Expr::SortOrder(sort) => self.resolve_sort_order(sort, schema),
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
                            .map(|x| self.resolve_expression(x, schema))
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
                    .map(|x| self.resolve_expression(x, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                let order_by = order_spec
                    .into_iter()
                    .map(|x| self.resolve_sort_order(x, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                let window_frame = if let Some(frame) = frame_spec {
                    self.resolve_window_frame(frame)?
                } else {
                    window_frame::WindowFrame::new(None)
                };
                Ok(expr::Expr::WindowFunction(expr::WindowFunction {
                    fun: get_built_in_window_function(func_name.as_str())?,
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
                    expr: Box::new(self.resolve_expression(*child, schema)?),
                    field,
                }))
            }
            Expr::UpdateFields { .. } => Err(PlanError::todo("update fields")),
            Expr::UnresolvedNamedLambdaVariable(_) => {
                Err(PlanError::todo("unresolved named lambda variable"))
            }
            Expr::CommonInlineUserDefinedFunction(function) => {
                // TODO: Function arg for if pyspark_udf or not.
                use framework_python::cereal::partial_pyspark_udf::{
                    deserialize_partial_pyspark_udf, PartialPySparkUDF,
                };
                use framework_python::udf::pyspark_udf::PySparkUDF;

                let spec::CommonInlineUserDefinedFunction {
                    function_name,
                    deterministic,
                    arguments,
                    function,
                } = function;

                let function_name: &str = function_name.as_str();
                let arguments: Vec<expr::Expr> = arguments
                    .into_iter()
                    .map(|x| self.resolve_expression(x, schema))
                    .collect::<PlanResult<Vec<expr::Expr>>>()?;
                let input_types: Vec<DataType> = arguments
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<DataType>, DataFusionError>>(
                )?;

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
                let output_type = self.resolve_data_type(output_type)?;

                let python_function: PartialPySparkUDF = deserialize_partial_pyspark_udf(
                    &python_version,
                    &command,
                    &eval_type,
                    &(arguments.len() as i32),
                    &self.config.spark_udf_config,
                )
                .map_err(|e| {
                    PlanError::invalid(format!("Python UDF deserialization error: {:?}", e))
                })?;

                let python_udf: PySparkUDF = PySparkUDF::new(
                    function_name.to_owned(),
                    deterministic,
                    input_types,
                    eval_type,
                    python_function,
                    output_type,
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
}
