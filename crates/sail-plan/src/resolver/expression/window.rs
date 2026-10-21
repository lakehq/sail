use std::cmp::Ordering;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
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

/// The name of the frame that Spark implies for a window function without a frame.
fn default_window_frame_name(
    function_name: &str,
    argument_display_names: &[String],
    has_order: bool,
) -> String {
    const UNBOUNDED_TO_CURRENT: &str = "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    match function_name {
        "row_number" | "rank" | "dense_rank" | "percent_rank" | "ntile" => {
            format!("ROWS {UNBOUNDED_TO_CURRENT}")
        }
        "cume_dist" => format!("RANGE {UNBOUNDED_TO_CURRENT}"),
        "lag" | "lead" => {
            let offset = argument_display_names
                .get(1)
                .map(String::as_str)
                .unwrap_or("1");
            // The frame of `lag` is the row at the negated offset.
            let offset = match (function_name, offset.strip_prefix('-')) {
                ("lag", Some(positive)) => positive.to_string(),
                ("lag", None) => format!("-{offset}"),
                _ => offset.to_string(),
            };
            format!("ROWS BETWEEN {offset} FOLLOWING AND {offset} FOLLOWING")
        }
        _ if has_order => format!("RANGE {UNBOUNDED_TO_CURRENT}"),
        _ => "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING".to_string(),
    }
}

/// Checks a window frame against the frame that Spark requires for the function.
/// `lag` and `lead` take no frame, and the ranking functions take only their own frame.
fn check_window_frame(
    function_name: &str,
    frame: &spec::WindowFrame,
    frame_name: &str,
) -> PlanResult<()> {
    let required = match function_name {
        "lag" | "lead" => {
            return Err(PlanError::analysis(format!(
                "Cannot specify window frame for {function_name} function."
            )));
        }
        "row_number" | "rank" | "dense_rank" | "percent_rank" | "ntile" => {
            spec::WindowFrameType::Row
        }
        "cume_dist" => spec::WindowFrameType::Range,
        _ => return Ok(()),
    };
    let is_required = frame.frame_type == required
        && matches!(frame.lower, spec::WindowFrameBoundary::UnboundedPreceding)
        && matches!(frame.upper, spec::WindowFrameBoundary::CurrentRow);
    if is_required {
        Ok(())
    } else {
        let units = match required {
            spec::WindowFrameType::Row => "ROWS",
            spec::WindowFrameType::Range => "RANGE",
        };
        Err(PlanError::analysis(format!(
            "Window Frame {frame_name} must match the required frame {units} BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW."
        )))
    }
}

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
        let (partition_names, partition_by) = self
            .resolve_expressions_and_names(partition_by, schema, state)
            .await?;
        // Spark treats literals as constants in ORDER BY window definition
        let mut sorts = Vec::with_capacity(order_by.len());
        let mut sort_names = Vec::with_capacity(order_by.len());
        for spec::SortOrder {
            child,
            direction,
            null_ordering,
        } in order_by
        {
            let NamedExpr { name, expr, .. } =
                self.resolve_named_expression(*child, schema, state).await?;
            let sort = Self::sort_with_options(expr, direction, null_ordering);
            sort_names.push(format!(
                "{} {} NULLS {}",
                name.one()?,
                if sort.asc { "ASC" } else { "DESC" },
                if sort.nulls_first { "FIRST" } else { "LAST" }
            ));
            sorts.push(sort);
        }
        let frame_name = match &frame {
            Some(frame) => {
                let frame_name = self.window_frame_name(frame, schema, state).await?;
                if let spec::Expr::UnresolvedFunction(function) = &window_function
                    && let Some(function_name) = <Vec<String>>::from(function.function_name.clone())
                        .into_iter()
                        .last()
                {
                    check_window_frame(&function_name.to_ascii_lowercase(), frame, &frame_name)?;
                }
                Some(frame_name)
            }
            None => None,
        };
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
                    (
                        function(input)?,
                        function_name,
                        argument_display_names,
                        is_distinct,
                    )
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
                return Err(PlanError::analysis(
                    "[UNSUPPORTED_EXPR_FOR_WINDOW] The expression is not supported within a window function.",
                ));
            }
        };
        // Spark names a window expression with its window specification, including the
        // frame that is implied by the function when none is given.
        let canonical_function_name = function_name.to_ascii_lowercase();
        let mut argument_display_names = argument_display_names;
        let mut ignore_nulls = false;
        match canonical_function_name.as_str() {
            "lag" | "lead" => {
                if argument_display_names.len() == 1 {
                    argument_display_names.push("1".to_string());
                }
                if argument_display_names.len() == 2 {
                    argument_display_names.push("NULL".to_string());
                }
            }
            "nth_value" if argument_display_names.len() == 3 => {
                ignore_nulls = argument_display_names.pop().as_deref() == Some("true");
            }
            _ => {}
        }
        let service = self.ctx.extension::<PlanService>()?;
        let mut name = service.plan_formatter().function_to_string(
            function_name.as_str(),
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;
        if matches!(
            canonical_function_name.as_str(),
            "rank" | "dense_rank" | "percent_rank"
        ) {
            name = name.to_ascii_uppercase();
        }
        if ignore_nulls {
            name.push_str(" ignore nulls");
        }
        let frame_name = frame_name.unwrap_or_else(|| {
            default_window_frame_name(
                &canonical_function_name,
                &argument_display_names,
                !sort_names.is_empty(),
            )
        });
        let mut specification = vec![];
        if !partition_names.is_empty() {
            specification.push(format!("PARTITION BY {}", partition_names.join(", ")));
        }
        if !sort_names.is_empty() {
            specification.push(format!("ORDER BY {}", sort_names.join(", ")));
        }
        specification.push(frame_name);
        let name = format!("{name} OVER ({})", specification.join(" "));
        Ok(NamedExpr::new(vec![name], window))
    }

    /// The name of a window frame as Spark shows it, where an offset that is not given as
    /// `PRECEDING` in SQL is shown as `FOLLOWING`, even when it is negative.
    async fn window_frame_name(
        &self,
        frame: &spec::WindowFrame,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let is_range = matches!(frame.frame_type, spec::WindowFrameType::Range);
        let lower = self
            .window_frame_boundary_name(&frame.lower, is_range, schema, state)
            .await?;
        let upper = self
            .window_frame_boundary_name(&frame.upper, is_range, schema, state)
            .await?;
        let units = if is_range { "RANGE" } else { "ROWS" };
        Ok(format!("{units} BETWEEN {lower} AND {upper}"))
    }

    async fn window_frame_boundary_name(
        &self,
        boundary: &spec::WindowFrameBoundary,
        is_range: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let name = match boundary {
            spec::WindowFrameBoundary::CurrentRow => "CURRENT ROW".to_string(),
            spec::WindowFrameBoundary::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
            spec::WindowFrameBoundary::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
            spec::WindowFrameBoundary::Preceding(expr) => {
                let value = self.expression_name(expr, schema, state).await?;
                if is_range {
                    format!("(- {value}) FOLLOWING")
                } else {
                    format!("{value} PRECEDING")
                }
            }
            spec::WindowFrameBoundary::Following(expr) => {
                let value = self.expression_name(expr, schema, state).await?;
                format!("{value} FOLLOWING")
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let value = self.expression_name(expr, schema, state).await?;
                if value == "0" {
                    "CURRENT ROW".to_string()
                } else {
                    format!("{value} FOLLOWING")
                }
            }
        };
        Ok(name)
    }

    async fn expression_name(
        &self,
        expr: &spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let NamedExpr { name, .. } = self
            .resolve_named_expression(expr.clone(), schema, state)
            .await?;
        Ok(name.one()?)
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
