use std::cmp::Ordering;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::{
    AggregateUDF, ExprSchemable, WindowFrame, WindowFrameBound, WindowFrameUnits, expr,
};
use sail_catalog::manager::CatalogManager;
use sail_common::spec::{self};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::window_aggregate::WindowAggregate;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::{
    PySparkAggregateMode, PySparkGroupAggKind, PySparkGroupAggregateUDF,
};
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, WinFunctionInput, get_null_treatment};
use crate::function::get_built_in_window_function;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_window(
        &self,
        window_function: spec::Expr,
        window: spec::Window,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let window = match window {
            spec::Window::Named(name) => state
                .get_window(name.as_ref())
                .ok_or_else(|| PlanError::analysis(format!("undefined window: {}", name.as_ref())))?
                .clone(),
            w => w,
        };

        let spec::Window::Unnamed {
            cluster_by,
            partition_by,
            order_by,
            frame,
        } = window
        else {
            return Err(PlanError::analysis("named windows in window expressions"));
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
            self.resolve_window_frame(frame, &sorts, schema, state)
                .await?
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
                filter,
                order_by: within_group,
            }) => {
                let Ok(function_name) = <Vec<String>>::from(function_name).one() else {
                    return Err(PlanError::unsupported("qualified window function name"));
                };
                if !named_arguments.is_empty() {
                    return Err(PlanError::todo("named window function arguments"));
                }
                let canonical_function_name = function_name.to_ascii_lowercase();
                // `mode() WITHIN GROUP (ORDER BY ...)` takes the ordering as its arguments, as in
                // an aggregate, and supports any window.
                let (arguments, within_group) = Self::convert_mode_within_group(
                    &canonical_function_name,
                    arguments,
                    within_group,
                )?;
                // Spark supports `WITHIN GROUP` in a window for `listagg`, `percentile_cont` and
                // `percentile_disc` when the window has no ORDER BY and covers the whole partition.
                // The values of the partition are then ordered by the `WITHIN GROUP` ordering.
                // <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/WindowResolution.scala#L142-L164>
                let ordered_set_function = matches!(
                    canonical_function_name.as_str(),
                    "listagg" | "string_agg" | "percentile_cont" | "percentile_disc"
                );
                let sorts = match within_group {
                    None if matches!(
                        canonical_function_name.as_str(),
                        "percentile_cont" | "percentile_disc"
                    ) =>
                    {
                        return Err(PlanError::analysis(format!(
                            "[INVALID_WITHIN_GROUP_EXPRESSION.WITHIN_GROUP_MISSING] Invalid function `{function_name}` with WITHIN GROUP. WITHIN GROUP is required for the function."
                        )));
                    }
                    None => sorts,
                    Some(within_group) if ordered_set_function => {
                        if is_distinct {
                            return Err(PlanError::analysis(
                                "[DISTINCT_WINDOW_FUNCTION_UNSUPPORTED] Distinct window functions are not supported",
                            ));
                        }
                        let whole_partition = window_frame.units == WindowFrameUnits::Rows
                            && matches!(window_frame.start_bound, WindowFrameBound::Preceding(_))
                            && window_frame.start_bound.is_unbounded()
                            && matches!(window_frame.end_bound, WindowFrameBound::Following(_))
                            && window_frame.end_bound.is_unbounded();
                        if !sorts.is_empty() || !whole_partition {
                            return Err(PlanError::analysis(format!(
                                "[INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC] Cannot specify ORDER BY or a window frame for {function_name} with WITHIN GROUP"
                            )));
                        }
                        self.resolve_sort_orders(within_group, true, schema, state)
                            .await?
                    }
                    Some(_) => {
                        return Err(PlanError::unsupported(format!(
                            "WITHIN GROUP in window function {function_name}"
                        )));
                    }
                };
                let filter = match filter {
                    Some(filter) => {
                        let filter = self.resolve_expression(*filter, schema, state).await?;
                        validate_window_filter(&filter, is_distinct, schema)?;
                        Some(filter)
                    }
                    None => None,
                };
                // `is_user_defined_function` is always false on the wire, so a registered
                // Python UDAF arrives here as `UnresolvedFunction`. Look it up in the
                // catalog before falling back to the built-in window function registry.
                let catalog_function = self
                    .ctx
                    .extension::<CatalogManager>()?
                    .get_function(&canonical_function_name)?;
                let registered_udaf = catalog_function.as_ref().and_then(|udf| {
                    udf.inner()
                        .downcast_ref::<PySparkUnresolvedUDF>()
                        .filter(|f| {
                            matches!(
                                f.eval_type(),
                                spec::PySparkUdfType::GroupedAggPandas
                                    | spec::PySparkUdfType::GroupedAggPandasIter
                                    | spec::PySparkUdfType::GroupedAggArrow
                                    | spec::PySparkUdfType::GroupedAggArrowIter
                                    | spec::PySparkUdfType::WindowAggPandas
                                    | spec::PySparkUdfType::WindowAggArrow
                            )
                        })
                });
                if let Some(udaf) = registered_udaf {
                    if filter.is_some() {
                        return Err(PlanError::unsupported(format!(
                            "FILTER in user-defined window function {function_name}"
                        )));
                    }
                    let (udaf, arguments, argument_display_names) = self
                        .resolve_registered_pyspark_udaf(
                            udaf,
                            &function_name,
                            arguments,
                            schema,
                            state,
                        )
                        .await?;
                    let window = expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                        fun: expr::WindowFunctionDefinition::AggregateUDF(udaf),
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
                    (window, function_name, argument_display_names, is_distinct)
                } else {
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
                    let window = function(input)?;
                    let window = match filter {
                        Some(filter) => {
                            with_window_filter(window, filter, &canonical_function_name)?
                        }
                        None => window,
                    };
                    (window, function_name, argument_display_names, is_distinct)
                }
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
                let (arguments, kwargs) = Self::extract_kwargs(arguments);
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
                    &input_types,
                    &kwargs,
                    &self.config.pyspark_udf_config,
                )?;
                let (function, arguments) = match function.eval_type {
                    spec::PySparkUdfType::GroupedAggPandas
                    | spec::PySparkUdfType::GroupedAggPandasIter
                    | spec::PySparkUdfType::GroupedAggArrow
                    | spec::PySparkUdfType::GroupedAggArrowIter
                    | spec::PySparkUdfType::WindowAggPandas
                    | spec::PySparkUdfType::WindowAggArrow => {
                        let kind = match function.eval_type {
                            spec::PySparkUdfType::GroupedAggArrow
                            | spec::PySparkUdfType::GroupedAggArrowIter
                            | spec::PySparkUdfType::WindowAggArrow => PySparkGroupAggKind::Arrow,
                            _ => PySparkGroupAggKind::Pandas,
                        };
                        // DataFusion requires at least one input to an aggregate function.
                        // For 0-arg UDFs inject a dummy Int64 literal; the accumulator strips it.
                        let actual_arg_count = arguments.len();
                        let (arguments, input_types) = if arguments.is_empty() {
                            (
                                vec![datafusion_expr::lit(0i64)],
                                vec![arrow::datatypes::DataType::Int64],
                            )
                        } else {
                            (arguments, input_types)
                        };
                        let udaf = PySparkGroupAggregateUDF::new(
                            kind,
                            PySparkAggregateMode::Window,
                            get_udf_name(&function_name, &payload),
                            payload,
                            deterministic,
                            argument_display_names.clone(),
                            input_types,
                            function.output_type,
                            self.config.pyspark_udf_config.clone(),
                            actual_arg_count,
                        );
                        let udaf = AggregateUDF::from(udaf);
                        (
                            expr::WindowFunctionDefinition::AggregateUDF(Arc::new(udaf)),
                            arguments,
                        )
                    }
                    _ => {
                        return Err(PlanError::invalid(
                            "invalid user-defined window function type",
                        ));
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

    async fn resolve_registered_pyspark_udaf(
        &self,
        udaf: &PySparkUnresolvedUDF,
        function_name: &str,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Arc<AggregateUDF>, Vec<expr::Expr>, Vec<String>)> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let (argument_display_names, arguments) = self
            .resolve_expressions_and_names(arguments, schema, state)
            .await?;
        let input_types: Vec<DataType> = arguments
            .iter()
            .map(|arg| arg.get_type(schema))
            .collect::<Result<Vec<DataType>, DataFusionError>>()?;
        let output_type = udaf.output_type().cloned().ok_or_else(|| {
            PlanError::internal(format!(
                "unresolved UDAF {function_name} has no return type"
            ))
        })?;
        let payload = PySparkUdfPayload::build(
            udaf.python_version(),
            udaf.command(),
            udaf.eval_type(),
            &((0..arguments.len()).collect::<Vec<_>>()),
            &input_types,
            &[],
            &self.config.pyspark_udf_config,
        )?;
        let kind = match udaf.eval_type() {
            spec::PySparkUdfType::GroupedAggArrow
            | spec::PySparkUdfType::GroupedAggArrowIter
            | spec::PySparkUdfType::WindowAggArrow => PySparkGroupAggKind::Arrow,
            _ => PySparkGroupAggKind::Pandas,
        };
        let actual_arg_count = arguments.len();
        let (arguments, input_types) = if arguments.is_empty() {
            (vec![datafusion_expr::lit(0i64)], vec![DataType::Int64])
        } else {
            (arguments, input_types)
        };
        let new_udaf = PySparkGroupAggregateUDF::new(
            kind,
            PySparkAggregateMode::Window,
            get_udf_name(function_name, &payload),
            payload,
            udaf.deterministic(),
            argument_display_names.clone(),
            input_types,
            output_type,
            self.config.pyspark_udf_config.clone(),
            actual_arg_count,
        );
        Ok((
            Arc::new(AggregateUDF::from(new_udaf)),
            arguments,
            argument_display_names,
        ))
    }

    async fn resolve_window_frame(
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
                self.resolve_window_boundary_offset(lower, schema, state)
                    .await?,
                self.resolve_window_boundary_offset(upper, schema, state)
                    .await?,
            ),
            WindowFrameUnits::Range => (
                self.resolve_window_boundary_value(lower, order_by, schema, state)
                    .await?,
                self.resolve_window_boundary_value(upper, order_by, schema, state)
                    .await?,
            ),
        };
        Ok(WindowFrame::new_bounds(units, start, end))
    }

    async fn resolve_window_boundary(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        if let spec::Expr::Literal(value) = expr {
            return self.resolve_literal(value, state);
        }
        let resolved = self.resolve_expression(expr, schema, state).await?;
        if let datafusion_expr::Expr::Literal(scalar, _) = resolved {
            return Ok(scalar);
        }
        // Apply type coercion so that expressions like `CAST(0 AS INTERVAL SECOND)`
        // have compatible types before physical evaluation.
        let context = SimplifyContextBuilder::default()
            .with_schema(schema.clone())
            .build();
        let simplifier = ExprSimplifier::new(context);
        let coerced = simplifier.coerce(resolved, schema).map_err(|e| {
            PlanError::invalid(format!(
                "window boundary must be a constant expression: {e}"
            ))
        })?;
        let evaluator = LiteralEvaluator::new();
        evaluator.evaluate(&coerced).map_err(|e| {
            PlanError::invalid(format!(
                "window boundary must be a constant expression: {e}"
            ))
        })
    }

    async fn resolve_window_boundary_offset(
        &self,
        value: spec::WindowFrameBoundary,
        schema: &DFSchemaRef,
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
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                Ok(WindowFrameBound::Preceding(
                    value.cast_to(&DataType::UInt64)?,
                ))
            }
            spec::WindowFrameBoundary::Following(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                Ok(WindowFrameBound::Following(
                    value.cast_to(&DataType::UInt64)?,
                ))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
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

    async fn resolve_window_boundary_value(
        &self,
        value: spec::WindowFrameBoundary,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<WindowFrameBound> {
        // Helper to get the data type from the order_by expression for casting
        let get_order_by_type = || -> PlanResult<DataType> {
            let [order_by] = order_by else {
                return Err(PlanError::invalid(
                    "range window frame requires exactly one order by expression",
                ));
            };
            Ok(order_by.expr.to_field(schema)?.1.data_type().clone())
        };

        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::UnboundedPreceding => {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            }
            spec::WindowFrameBoundary::UnboundedFollowing => {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
            spec::WindowFrameBoundary::Preceding(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                // Cast numeric boundaries to match the ORDER BY type.
                // Non-numeric boundaries (e.g. INTERVAL for TIMESTAMP ORDER BY) are left as-is
                // since DataFusion handles interval arithmetic directly.
                let data_type = get_order_by_type()?;
                let value = if data_type.is_numeric() {
                    value.cast_to(&data_type)?
                } else {
                    value
                };
                Ok(WindowFrameBound::Preceding(value))
            }
            spec::WindowFrameBoundary::Following(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                let data_type = get_order_by_type()?;
                let value = if data_type.is_numeric() {
                    value.cast_to(&data_type)?
                } else {
                    value
                };
                Ok(WindowFrameBound::Following(value))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                if value.is_null() {
                    Err(PlanError::invalid("window boundary value cannot be null"))
                } else {
                    let data_type = get_order_by_type()?;
                    let value = if data_type.is_numeric() {
                        value.cast_to(&data_type)?
                    } else {
                        value
                    };
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

/// Validates the FILTER clause of an aggregate function over a window as Spark does.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionResolution.scala#L479-L560>
fn validate_window_filter(
    filter: &expr::Expr,
    is_distinct: bool,
    schema: &DFSchemaRef,
) -> PlanResult<()> {
    if is_distinct {
        return Err(PlanError::analysis(
            "[DISTINCT_WINDOW_FUNCTION_UNSUPPORTED] Distinct window functions are not supported",
        ));
    }
    if filter.is_volatile() {
        return Err(PlanError::analysis(format!(
            "[INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC] The FILTER expression {filter} in an aggregate function is invalid. Expected a FILTER expression to be deterministic."
        )));
    }
    let data_type = filter.get_type(schema)?;
    if data_type != DataType::Boolean {
        return Err(PlanError::analysis(format!(
            "[INVALID_AGGREGATE_FILTER.NOT_BOOLEAN] The FILTER expression {filter} in an aggregate function is invalid. Expected a FILTER expression to be of BOOLEAN type, got {data_type}."
        )));
    }
    Ok(())
}

/// Applies the FILTER clause of an aggregate function over a window, so that rows where the
/// filter is not true are not aggregated but still count toward the window frame, as in Spark.
/// The filter is an argument of a [`WindowAggregate`] wrapping the aggregate function, rather
/// than the filter of the window function, since DataFusion does not serialize the latter for
/// distributed execution.
fn with_window_filter(
    window: expr::Expr,
    filter: expr::Expr,
    function_name: &str,
) -> PlanResult<expr::Expr> {
    let unsupported = window.exists(|e| {
        Ok(matches!(
            e,
            expr::Expr::WindowFunction(window)
                if !matches!(window.fun, expr::WindowFunctionDefinition::AggregateUDF(_))
        ))
    })?;
    if unsupported {
        return if matches!(
            function_name,
            "row_number"
                | "rank"
                | "dense_rank"
                | "percent_rank"
                | "cume_dist"
                | "ntile"
                | "lag"
                | "lead"
                | "nth_value"
        ) {
            Err(PlanError::analysis(format!(
                "[INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX] Invalid SQL syntax: The function `{function_name}` does not support FILTER."
            )))
        } else {
            Err(PlanError::unsupported(format!(
                "FILTER in window function {function_name}"
            )))
        };
    }
    Ok(window
        .transform(|e| match e {
            expr::Expr::WindowFunction(mut window) => {
                if let expr::WindowFunctionDefinition::AggregateUDF(function) = &window.fun {
                    // A function that is wrapped already (e.g. for its WITHIN GROUP ordering)
                    // gets the filter in the same wrapper.
                    let function = match function.inner().downcast_ref::<WindowAggregate>() {
                        Some(wrapped) if !wrapped.filter() => WindowAggregate::new(
                            Arc::clone(wrapped.inner()),
                            true,
                            wrapped.ordering(),
                        ),
                        _ => WindowAggregate::new(Arc::clone(function), true, None),
                    };
                    window.fun = expr::WindowFunctionDefinition::AggregateUDF(Arc::new(
                        AggregateUDF::from(function),
                    ));
                    window.params.args.push(filter.clone());
                }
                Ok(Transformed::yes(expr::Expr::WindowFunction(window)))
            }
            e => Ok(Transformed::no(e)),
        })
        .data()?)
}
