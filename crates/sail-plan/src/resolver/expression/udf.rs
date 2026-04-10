use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::{expr, AggregateUDF, Expr, ExprSchemable, ScalarUDF};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::{PySparkGroupAggKind, PySparkGroupAggregateUDF};
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::function::PythonUdf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

/// If `expr` contains (or is) an `AggregateFunction`, return its name.
/// Used to detect illegal nesting of aggregate functions as UDAF arguments.
fn find_aggregate_in_expr(expr: &Expr) -> Option<String> {
    let mut found: Option<String> = None;
    let _ = expr.apply(|e| {
        if let Expr::AggregateFunction(agg) = e {
            found = Some(agg.func.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_common_inline_udf(
        &self,
        function: spec::CommonInlineUserDefinedFunction,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
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

        // Separate positional args from named (keyword) args before resolution
        let (positional_args, kwarg_names) = Self::extract_kwargs(arguments);

        // Validate named arguments before building the payload:
        // 1. No duplicate kwarg names → DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE
        // 2. No positional argument after a named argument → UNEXPECTED_POSITIONAL_ARGUMENT
        {
            let mut seen_kwarg_names = std::collections::HashSet::new();
            let mut seen_named = false;
            for kwarg in &kwarg_names {
                match kwarg {
                    Some(name) => {
                        if !seen_kwarg_names.insert(name.as_str()) {
                            return Err(PlanError::AnalysisError(format!(
                                "[DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE] \
                                 Duplicate named argument: '{name}' is assigned more than once."
                            )));
                        }
                        seen_named = true;
                    }
                    None => {
                        if seen_named {
                            return Err(PlanError::AnalysisError(
                                "[UNEXPECTED_POSITIONAL_ARGUMENT] \
                                 Positional argument follows a named (keyword) argument."
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }

        let (argument_display_names, arguments) = self
            .resolve_expressions_and_names(positional_args, schema, state)
            .await?;
        let function = self.resolve_python_udf(function, state)?;
        let func = self.resolve_python_udf_expr(
            function,
            &function_name,
            arguments,
            &argument_display_names,
            &kwarg_names,
            schema,
            deterministic,
            is_distinct,
            state,
        )?;
        let service = self.ctx.extension::<PlanService>()?;
        let name = service.plan_formatter().function_to_string(
            &function_name,
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;
        Ok(NamedExpr::new(vec![name], func))
    }

    #[expect(clippy::too_many_arguments)]
    pub(super) fn resolve_python_udf_expr(
        &self,
        function: PythonUdf,
        name: &str,
        arguments: Vec<Expr>,
        argument_display_names: &[String],
        // Per-argument kwarg name: None for positional, Some(key) for keyword
        kwarg_names: &[Option<String>],
        schema: &DFSchemaRef,
        deterministic: bool,
        distinct: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<Expr> {
        use spec::PySparkUdfType;

        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let input_types: Vec<DataType> =
            arguments
                .iter()
                .map(|arg| arg.get_type(schema))
                .collect::<datafusion_common::Result<Vec<DataType>, DataFusionError>>()?;
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            &((0..arguments.len()).collect::<Vec<_>>()),
            kwarg_names,
            &self.config.pyspark_udf_config,
        )?;

        match function.eval_type {
            PySparkUdfType::None
            | PySparkUdfType::GroupedMapPandas
            | PySparkUdfType::GroupedMapArrow
            | PySparkUdfType::WindowAggPandas
            | PySparkUdfType::WindowAggArrow
            | PySparkUdfType::MapPandasIter
            | PySparkUdfType::CogroupedMapPandas
            | PySparkUdfType::CogroupedMapArrow
            | PySparkUdfType::MapArrowIter
            | PySparkUdfType::GroupedMapPandasWithState
            | PySparkUdfType::TransformWithStatePandas
            | PySparkUdfType::TransformWithStatePandasInitState
            | PySparkUdfType::TransformWithStatePythonRow
            | PySparkUdfType::TransformWithStatePythonRowInitState
            | PySparkUdfType::GroupedMapArrowIter
            | PySparkUdfType::GroupedMapPandasIter
            | PySparkUdfType::Table
            | PySparkUdfType::ArrowTable
            | PySparkUdfType::ArrowUdtf => Err(PlanError::invalid(format!(
                "unsupported Python UDF type for common inline UDF: {:?}",
                function.eval_type
            ))),
            PySparkUdfType::Batched => {
                let udf = PySparkUDF::new(
                    PySparkUdfKind::Batch,
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                );
                Ok(Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(udf)),
                    args: arguments,
                }))
            }
            PySparkUdfType::ArrowBatched => {
                let udf = PySparkUDF::new(
                    PySparkUdfKind::ArrowBatch,
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                );
                Ok(Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(udf)),
                    args: arguments,
                }))
            }
            PySparkUdfType::ScalarPandas => {
                let udf = PySparkUDF::new(
                    PySparkUdfKind::ScalarPandas,
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                );
                Ok(Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(udf)),
                    args: arguments,
                }))
            }
            PySparkUdfType::ScalarPandasIter => {
                let udf = PySparkUDF::new(
                    PySparkUdfKind::ScalarPandasIter,
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                );
                Ok(Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(udf)),
                    args: arguments,
                }))
            }
            PySparkUdfType::GroupedAggPandas | PySparkUdfType::GroupedAggPandasIter => {
                // Spark CheckAnalysis: aggregate functions cannot be nested inside another
                // aggregate function's arguments.
                for arg in &arguments {
                    if let Some(inner) = find_aggregate_in_expr(arg) {
                        return Err(PlanError::AnalysisError(format!(
                            "The aggregate function '{name}' cannot take an argument containing \
                             another aggregate function: '{inner}'."
                        )));
                    }
                }
                // DataFusion requires at least one input to an aggregate function.
                // For 0-arg UDFs inject a dummy Int64 literal; the accumulator will
                // strip it before calling Python.
                let actual_arg_count = arguments.len();
                let (arguments, input_types) = if arguments.is_empty() {
                    (
                        vec![Expr::Literal(
                            datafusion_common::ScalarValue::Int64(Some(0)),
                            None,
                        )],
                        vec![arrow::datatypes::DataType::Int64],
                    )
                } else {
                    (arguments, input_types)
                };
                let udaf = PySparkGroupAggregateUDF::new(
                    PySparkGroupAggKind::Pandas, // Pandas path: Arrow → Pandas → user func → Arrow
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    argument_display_names.to_vec(),
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                    actual_arg_count,
                );
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: Arc::new(AggregateUDF::from(udaf)),
                    params: AggregateFunctionParams {
                        args: arguments,
                        distinct,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
            // Arrow-native scalar UDF (250): user func receives/returns pyarrow.Array directly
            PySparkUdfType::ScalarArrow => {
                let udf = PySparkUDF::new(
                    PySparkUdfKind::ScalarArrow,
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                );
                Ok(Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(udf)),
                    args: arguments,
                }))
            }
            // Arrow-native scalar iterator UDF (251): user func is Iterator[pa.Array] → Iterator[pa.Array]
            PySparkUdfType::ScalarArrowIter => {
                let udf = PySparkUDF::new(
                    PySparkUdfKind::ScalarArrowIter,
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                );
                Ok(Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(udf)),
                    args: arguments,
                }))
            }
            // Arrow-native grouped aggregate UDF (252) and iterator variant (254)
            PySparkUdfType::GroupedAggArrow | PySparkUdfType::GroupedAggArrowIter => {
                // Spark CheckAnalysis: aggregate functions cannot be nested inside another
                // aggregate function's arguments.
                for arg in &arguments {
                    if let Some(inner) = find_aggregate_in_expr(arg) {
                        return Err(PlanError::AnalysisError(format!(
                            "The aggregate function '{name}' cannot take an argument containing \
                             another aggregate function: '{inner}'."
                        )));
                    }
                }
                // DataFusion requires at least one input to an aggregate function.
                // For 0-arg UDFs inject a dummy Int64 literal; the accumulator will
                // strip it before calling Python.
                let actual_arg_count = arguments.len();
                let (arguments, input_types) = if arguments.is_empty() {
                    (
                        vec![Expr::Literal(
                            datafusion_common::ScalarValue::Int64(Some(0)),
                            None,
                        )],
                        vec![arrow::datatypes::DataType::Int64],
                    )
                } else {
                    (arguments, input_types)
                };
                let udaf = PySparkGroupAggregateUDF::new(
                    PySparkGroupAggKind::Arrow, // Arrow path: no Pandas conversion
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    argument_display_names.to_vec(),
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                    actual_arg_count,
                );
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: Arc::new(AggregateUDF::from(udaf)),
                    params: AggregateFunctionParams {
                        args: arguments,
                        distinct,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                }))
            }
        }
    }

    /// Splits a list of spec expressions into positional expressions and a parallel
    /// vector of optional kwarg names. `NamedArgument` variants are unwrapped; all
    /// other expressions get `None` in the kwarg slot.
    pub(crate) fn extract_kwargs(
        arguments: Vec<spec::Expr>,
    ) -> (Vec<spec::Expr>, Vec<Option<String>>) {
        let mut exprs = Vec::with_capacity(arguments.len());
        let mut kwargs = Vec::with_capacity(arguments.len());
        for arg in arguments {
            match arg {
                spec::Expr::NamedArgument { key, value } => {
                    kwargs.push(Some(key));
                    exprs.push(*value);
                }
                other => {
                    kwargs.push(None);
                    exprs.push(other);
                }
            }
        }
        (exprs, kwargs)
    }
}
