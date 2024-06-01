use std::sync::Arc;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::alias::MultiAlias;
use crate::function::{
    get_built_in_aggregate_function, get_built_in_function, get_built_in_window_function,
};
use crate::resolver::PlanResolver;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::{
    expr, window_frame, ExprSchemable, GetFieldAccess, GetIndexedField, ScalarFunctionDefinition,
    ScalarUDF,
};
use framework_common::spec;
use framework_python::partial_python_udf::PartialPythonUDF;

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

    pub(crate) fn resolve_expression(
        &self,
        expr: spec::Expr,
        schema: &DFSchema,
    ) -> PlanResult<expr::Expr> {
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

                let args = arguments
                    .into_iter()
                    .map(|x| self.resolve_expression(x, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                let func = match get_built_in_function(function_name.as_str()) {
                    Ok(func) => func(args)?,
                    Err(_) => {
                        get_built_in_aggregate_function(function_name.as_str(), args, is_distinct)?
                    }
                };
                Ok(func)
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
                let data_type = cast_to_type.try_into()?;
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
}
