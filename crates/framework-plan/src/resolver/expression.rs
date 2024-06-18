use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::{expr, window_frame, ExprSchemable, GetFieldAccess, ScalarUDF};
use framework_common::spec;
use framework_python::cereal::partial_pyspark_udf::{
    deserialize_partial_pyspark_udf, PartialPySparkUDF,
};
use framework_python::udf::pyspark_udf::PySparkUDF;
use framework_python::udf::unresolved_pyspark_udf::UnresolvedPySparkUDF;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::alias::MultiAlias;
use crate::function::{
    get_built_in_aggregate_function, get_built_in_function, get_built_in_window_function,
    is_built_in_generator_function,
};
use crate::resolver::{PlanResolver, PlanResolverState};

impl PlanResolver<'_> {
    pub(crate) fn resolve_sort_order(
        &self,
        sort: spec::SortOrder,
        schema: &DFSchema,
        state: &mut PlanResolverState,
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
            expr: Box::new(self.resolve_expression(*child, schema, state)?),
            asc,
            nulls_first,
        }))
    }

    pub(crate) fn resolve_window_frame(
        &self,
        frame: spec::WindowFrame,
        state: &mut PlanResolverState,
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
                self.resolve_window_boundary_value(*value, state)?,
            ),
        };
        let end = match upper {
            WindowFrameBoundary::CurrentRow => window_frame::WindowFrameBound::CurrentRow,
            WindowFrameBoundary::Unbounded => {
                window_frame::WindowFrameBound::Following(ScalarValue::UInt64(None))
            }
            WindowFrameBoundary::Value(value) => window_frame::WindowFrameBound::Following(
                self.resolve_window_boundary_value(*value, state)?,
            ),
        };
        Ok(window_frame::WindowFrame::new_bounds(units, start, end))
    }

    fn resolve_window_boundary_value(
        &self,
        value: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        let value = self
            .resolve_expression(value, &DFSchema::empty(), state)?
            .unalias();
        match value {
            expr::Expr::Literal(
                v @ (ScalarValue::UInt32(_)
                | ScalarValue::Int32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int64(_)),
            ) => Ok(v),
            _ => Err(PlanError::invalid(format!(
                "invalid window boundary value: {:?}",
                value
            ))),
        }
    }

    pub fn resolve_expression(
        &self,
        expr: spec::Expr,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Expr> {
        use spec::Expr;

        match expr {
            Expr::Literal(literal) => {
                let name = self.config.plan_formatter.literal_to_string(&literal)?;
                let literal = self.resolve_literal(literal)?;
                Ok(expr::Expr::Alias(expr::Alias {
                    expr: Box::new(expr::Expr::Literal(literal)),
                    relation: None,
                    name,
                }))
            }
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
                    .map(|x| self.resolve_expression(x, schema, state))
                    .collect::<PlanResult<Vec<expr::Expr>>>()?;
                let argument_names = arguments
                    .iter()
                    .map(|arg| -> PlanResult<_> { Ok(arg.display_name()?) })
                    .collect::<PlanResult<Vec<_>>>()?;
                let input_types: Vec<DataType> = arguments
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<DataType>, DataFusionError>>(
                )?;

                let func = if let Ok(udf) = self.ctx.udf(function_name.as_str()) {
                    // TODO: UnresolvedPythonUDF will likely need to be accounted for as well
                    //  once we integrate LakeSail Python UDF.
                    let udf = if let Some(f) =
                        udf.inner().as_any().downcast_ref::<UnresolvedPySparkUDF>()
                    {
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
                        let output_type: DataType = self.resolve_data_type(output_type.clone())?;

                        let python_function: PartialPySparkUDF = deserialize_partial_pyspark_udf(
                            python_version,
                            command,
                            eval_type,
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
                            *eval_type,
                            python_function,
                            output_type,
                        );

                        Arc::new(ScalarUDF::from(python_udf))
                    } else {
                        udf
                    };
                    expr::Expr::ScalarFunction(expr::ScalarFunction {
                        func: udf,
                        args: arguments,
                    })
                }
                // FIXME: is_user_defined_function is always false
                //  So, we need to check udf's before built-in functions.
                else if let Ok(func) = get_built_in_function(function_name.as_str()) {
                    func(arguments.clone())?
                } else if let Ok(func) = get_built_in_aggregate_function(
                    function_name.as_str(),
                    arguments.clone(),
                    is_distinct,
                ) {
                    func
                } else {
                    return Err(PlanError::unsupported(format!(
                        "unknown function: {function_name}",
                    )));
                };
                // TODO: udaf and udwf

                if is_built_in_generator_function(function_name.as_str()) {
                    Ok(func)
                } else {
                    let name = self.config.plan_formatter.function_to_string(
                        function_name.as_str(),
                        argument_names.iter().map(|x| x.as_str()).collect(),
                    )?;
                    Ok(expr::Expr::Alias(expr::Alias {
                        expr: Box::new(func),
                        relation: None,
                        name,
                    }))
                }
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
                let expr = self.resolve_expression(*expr, schema, state)?.unalias();
                if let [name] = name.as_slice() {
                    Ok(expr::Expr::Alias(expr::Alias {
                        expr: Box::new(expr),
                        relation: None,
                        name: name.clone().into(),
                    }))
                } else {
                    let name: Vec<String> = name.into_iter().map(|x| x.into()).collect();
                    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                        func: Arc::new(ScalarUDF::from(MultiAlias::new(name))),
                        args: vec![expr],
                    }))
                }
            }
            Expr::Cast { expr, cast_to_type } => {
                let data_type = self.resolve_data_type(cast_to_type)?;
                Ok(expr::Expr::Cast(expr::Cast {
                    expr: Box::new(self.resolve_expression(*expr, schema, state)?),
                    data_type,
                }))
            }
            Expr::UnresolvedRegex { .. } => Err(PlanError::todo("unresolved regex")),
            Expr::SortOrder(sort) => self.resolve_sort_order(sort, schema, state),
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
                            .map(|x| self.resolve_expression(x, schema, state))
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
                    .map(|x| self.resolve_expression(x, schema, state))
                    .collect::<PlanResult<Vec<_>>>()?;
                let order_by = order_spec
                    .into_iter()
                    .map(|x| self.resolve_sort_order(x, schema, state))
                    .collect::<PlanResult<Vec<_>>>()?;
                let window_frame = if let Some(frame) = frame_spec {
                    self.resolve_window_frame(frame, state)?
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
                let literal = match *extraction {
                    Expr::Literal(literal) => literal,
                    _ => {
                        return Err(PlanError::invalid("extraction must be a literal"));
                    }
                };
                let expression = self.resolve_expression(*child, schema, state)?;
                Ok(expression.field(self.resolve_literal(literal)?))
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
                    .map(|x| self.resolve_expression(x, schema, state))
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
                    func: Arc::new(ScalarUDF::from(python_udf)),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::expr::{Alias, Expr};
    use datafusion_expr::{BinaryExpr, Operator};
    use framework_common::spec;

    use crate::config::PlanConfig;
    use crate::error::PlanResult;
    use crate::resolver::{PlanResolver, PlanResolverState};

    #[test]
    fn test_resolve_expression_with_alias() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        let resolve = |expr: spec::Expr| {
            resolver.resolve_expression(expr, &DFSchema::empty(), &mut PlanResolverState::new())
        };

        assert_eq!(
            resolve(spec::Expr::UnresolvedFunction {
                function_name: "not".to_string(),
                arguments: vec![spec::Expr::Literal(spec::Literal::Boolean(true))],
                is_distinct: false,
                is_user_defined_function: false,
            })?,
            Expr::Alias(Alias {
                expr: Box::new(Expr::Not(Box::new(Expr::Alias(Alias {
                    expr: Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                    relation: None,
                    name: "true".to_string(),
                })))),
                relation: None,
                name: "(NOT true)".to_string(),
            })
        );

        // We need to make sure there is no nested alias in the resolved logical expression.
        // This is because many DataFusion functions (e.g. `Expr::unalias()`) can only work with
        // a single level of alias.
        assert_eq!(
            resolve(spec::Expr::Alias {
                // This alias "b" is overridden by the outer alias "c".
                expr: Box::new(spec::Expr::Alias {
                    // The resolver assigns an alias (a human-readable string) for the function,
                    // and is then overridden by the explicitly specified outer alias.
                    expr: Box::new(spec::Expr::UnresolvedFunction {
                        function_name: "+".to_string(),
                        arguments: vec![
                            spec::Expr::Alias {
                                // The resolver assigns an alias "1" for the literal,
                                // and is then overridden by the explicitly specified alias.
                                expr: Box::new(spec::Expr::Literal(spec::Literal::Integer(1))),
                                name: vec!["a".to_string().into()],
                                metadata: None,
                            },
                            // The resolver assigns an alias "2" for the literal.
                            spec::Expr::Literal(spec::Literal::Integer(2)),
                        ],
                        is_distinct: false,
                        is_user_defined_function: false,
                    }),
                    name: vec!["b".to_string().into()],
                    metadata: None,
                }),
                name: vec!["c".to_string().into()],
                metadata: None,
            })?,
            Expr::Alias(Alias {
                expr: Box::new(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Alias(Alias {
                        expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
                        relation: None,
                        name: "a".to_string(),
                    })),
                    op: Operator::Plus,
                    right: Box::new(Expr::Alias(Alias {
                        expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(2)))),
                        relation: None,
                        name: "2".to_string(),
                    })),
                })),
                relation: None,
                name: "c".to_string(),
            }),
        );

        Ok(())
    }
}
