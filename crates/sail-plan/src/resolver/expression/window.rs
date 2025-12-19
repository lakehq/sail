use std::cmp::Ordering;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{
    expr, AggregateUDF, ExprSchemable, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{get_null_treatment, FunctionContextInput, WinFunctionInput};
use crate::function::get_built_in_window_function;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_window(
        &self,
        window_function: spec::Expr,
        window: spec::Window,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let spec::Window::Unnamed {
            cluster_by,
            partition_by,
            order_by,
            frame,
        } = window
        else {
            return Err(PlanError::todo("named window"));
        };
        if !cluster_by.is_empty() {
            return Err(PlanError::unsupported(
                "CLUSTER BY clause in window expression",
            ));
        }
        let partition_by = self
            .resolve_expressions(partition_by, schema, state)
            .await?;
        // Spark treats literals as constants in ORDER BY window definition
        let sorts = self
            .resolve_sort_orders(order_by, false, schema, state)
            .await?;
        let window_frame = if let Some(frame) = frame {
            self.resolve_window_frame(frame, &sorts, schema, state)?
        } else {
            WindowFrame::new(if sorts.is_empty() {
                None
            } else {
                // TODO: should we use strict ordering or not?
                Some(false)
            })
        };
        let (window, function_name, argument_display_names, is_distinct) = match window_function {
            spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name,
                arguments,
                named_arguments,
                is_user_defined_function: false,
                is_internal: _,
                is_distinct,
                ignore_nulls,
                filter: None,
                // TODO: `window` and `window_function` both have an `order_by` field.
                //  Check: Which one should we use? Are they the same? Is one of them empty?
                order_by: None,
            }) => {
                let Ok(function_name) = <Vec<String>>::from(function_name).one() else {
                    return Err(PlanError::unsupported("qualified window function name"));
                };
                if !named_arguments.is_empty() {
                    return Err(PlanError::todo("named window function arguments"));
                }
                let canonical_function_name = function_name.to_ascii_lowercase();
                let (argument_display_names, arguments) = self
                    .resolve_expressions_and_names(arguments, schema, state)
                    .await?;
                let function = get_built_in_window_function(&canonical_function_name)?;
                let input = WinFunctionInput {
                    arguments,
                    partition_by,
                    order_by: sorts,
                    window_frame,
                    ignore_nulls,
                    distinct: is_distinct,
                    function_context: FunctionContextInput {
                        argument_display_names: &argument_display_names,
                        plan_config: &self.config,
                        session_context: self.ctx,
                        schema,
                    },
                };
                (
                    function(input)?,
                    function_name,
                    argument_display_names,
                    is_distinct,
                )
            }
            spec::Expr::CommonInlineUserDefinedFunction(function) => {
                let mut scope = state.enter_config_scope();
                let state = scope.state();
                state.config_mut().arrow_allow_large_var_types = true;
                let spec::CommonInlineUserDefinedFunction {
                    function_name,
                    deterministic,
                    is_distinct,
                    arguments,
                    function,
                } = function;
                let function_name: String = function_name.into();
                let (argument_display_names, arguments) = self
                    .resolve_expressions_and_names(arguments, schema, state)
                    .await?;
                let input_types: Vec<DataType> = arguments
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<DataType>, DataFusionError>>(
                )?;
                let function = self.resolve_python_udf(function, state)?;
                let payload = PySparkUdfPayload::build(
                    &function.python_version,
                    &function.command,
                    function.eval_type,
                    &((0..arguments.len()).collect::<Vec<_>>()),
                    &self.config.pyspark_udf_config,
                )?;
                let function = match function.eval_type {
                    spec::PySparkUdfType::GroupedAggPandas => {
                        let udaf = PySparkGroupAggregateUDF::new(
                            get_udf_name(&function_name, &payload),
                            payload,
                            deterministic,
                            argument_display_names.clone(),
                            input_types,
                            function.output_type,
                            self.config.pyspark_udf_config.clone(),
                        );
                        let udaf = AggregateUDF::from(udaf);
                        expr::WindowFunctionDefinition::AggregateUDF(Arc::new(udaf))
                    }
                    _ => {
                        return Err(PlanError::invalid(
                            "invalid user-defined window function type",
                        ))
                    }
                };
                let window = expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                    fun: function,
                    params: WindowFunctionParams {
                        args: arguments,
                        partition_by,
                        order_by: sorts,
                        window_frame,
                        filter: None,
                        null_treatment: get_null_treatment(None),
                        distinct: is_distinct,
                    },
                }));
                (window, function_name, argument_display_names, false)
            }
            _ => {
                return Err(PlanError::invalid(format!(
                    "invalid window function expression: {window_function:?}"
                )));
            }
        };
        let service = self.ctx.extension::<PlanService>()?;
        let name = service.plan_formatter().function_to_string(
            function_name.as_str(),
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;
        Ok(NamedExpr::new(vec![name], window))
    }

    fn resolve_window_frame(
        &self,
        frame: spec::WindowFrame,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<WindowFrame> {
        use spec::WindowFrameType;

        let spec::WindowFrame {
            frame_type,
            lower,
            upper,
        } = frame;

        let units = match frame_type {
            WindowFrameType::Row => WindowFrameUnits::Rows,
            WindowFrameType::Range => WindowFrameUnits::Range,
        };
        let (start, end) = match units {
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => (
                self.resolve_window_boundary_offset(lower, state)?,
                self.resolve_window_boundary_offset(upper, state)?,
            ),
            WindowFrameUnits::Range => (
                self.resolve_window_boundary_value(lower, order_by, schema, state)?,
                self.resolve_window_boundary_value(upper, order_by, schema, state)?,
            ),
        };
        Ok(WindowFrame::new_bounds(units, start, end))
    }

    fn resolve_window_boundary(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        let spec::Expr::Literal(value) = expr else {
            return Err(PlanError::invalid("window boundary must be a literal"));
        };
        self.resolve_literal(value, state)
    }

    fn resolve_window_boundary_offset(
        &self,
        value: spec::WindowFrameBoundary,
        state: &mut PlanResolverState,
    ) -> PlanResult<WindowFrameBound> {
        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::UnboundedPreceding => {
                Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None)))
            }
            spec::WindowFrameBoundary::UnboundedFollowing => {
                Ok(WindowFrameBound::Following(ScalarValue::UInt64(None)))
            }
            spec::WindowFrameBoundary::Preceding(expr) => {
                let value = self.resolve_window_boundary(*expr, state)?;
                Ok(WindowFrameBound::Preceding(
                    value.cast_to(&DataType::UInt64)?,
                ))
            }
            spec::WindowFrameBoundary::Following(expr) => {
                let value = self.resolve_window_boundary(*expr, state)?;
                Ok(WindowFrameBound::Following(
                    value.cast_to(&DataType::UInt64)?,
                ))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let value = self.resolve_window_boundary(*expr, state)?;
                let ScalarValue::Int64(Some(value)) = value.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::invalid("invalid window boundary offset"));
                };
                match value {
                    i64::MIN => Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None))),
                    i64::MAX => Ok(WindowFrameBound::Following(ScalarValue::UInt64(None))),
                    0 => Ok(WindowFrameBound::CurrentRow),
                    x if x < 0 => Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                        -x as u64,
                    )))),
                    x => Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
                        x as u64,
                    )))),
                }
            }
        }
    }

    fn resolve_window_boundary_value(
        &self,
        value: spec::WindowFrameBoundary,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<WindowFrameBound> {
        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::UnboundedPreceding => {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            }
            spec::WindowFrameBoundary::UnboundedFollowing => {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
            spec::WindowFrameBoundary::Preceding(expr) => {
                let value = self.resolve_window_boundary(*expr, state)?;
                Ok(WindowFrameBound::Preceding(value))
            }
            spec::WindowFrameBoundary::Following(expr) => {
                let value = self.resolve_window_boundary(*expr, state)?;
                Ok(WindowFrameBound::Following(value))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let value = self.resolve_window_boundary(*expr, state)?;
                if value.is_null() {
                    Err(PlanError::invalid("window boundary value cannot be null"))
                } else {
                    let [order_by] = order_by else {
                        return Err(PlanError::invalid(
                            "range window frame requires exactly one order by expression",
                        ));
                    };
                    let (data_type, _) = order_by.expr.data_type_and_nullable(schema)?;
                    let value = value.cast_to(&data_type)?;
                    let zero = ScalarValue::new_zero(&data_type)?;
                    match value.partial_cmp(&zero) {
                        None => Err(PlanError::invalid(
                            "cannot compare window boundary value with zero",
                        )),
                        Some(Ordering::Less) => {
                            let value = value.arithmetic_negate()?;
                            Ok(WindowFrameBound::Preceding(value))
                        }
                        Some(Ordering::Greater) => Ok(WindowFrameBound::Following(value)),
                        Some(Ordering::Equal) => Ok(WindowFrameBound::CurrentRow),
                    }
                }
            }
        }
    }
}
