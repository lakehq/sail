use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::{expr, AggregateUDF, Expr, ExprSchemable, ScalarUDF};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::PlanService;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::function::PythonUdf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

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
        let (argument_display_names, arguments) = self
            .resolve_expressions_and_names(arguments, schema, state)
            .await?;
        let function = self.resolve_python_udf(function, state)?;
        let func = self.resolve_python_udf_expr(
            function,
            &function_name,
            arguments,
            &argument_display_names,
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn resolve_python_udf_expr(
        &self,
        function: PythonUdf,
        name: &str,
        arguments: Vec<Expr>,
        argument_display_names: &[String],
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
            &self.config.pyspark_udf_config,
        )?;

        match function.eval_type {
            PySparkUdfType::None
            | PySparkUdfType::GroupedMapPandas
            | PySparkUdfType::GroupedMapArrow
            | PySparkUdfType::WindowAggPandas
            | PySparkUdfType::MapPandasIter
            | PySparkUdfType::CogroupedMapPandas
            | PySparkUdfType::CogroupedMapArrow
            | PySparkUdfType::MapArrowIter
            | PySparkUdfType::GroupedMapPandasWithState
            | PySparkUdfType::Table
            | PySparkUdfType::ArrowTable => Err(PlanError::invalid(format!(
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
            PySparkUdfType::GroupedAggPandas => {
                let udaf = PySparkGroupAggregateUDF::new(
                    get_udf_name(name, &payload),
                    payload,
                    deterministic,
                    argument_display_names.to_vec(),
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
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
}
