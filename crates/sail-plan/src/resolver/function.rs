use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{plan_err, DFSchemaRef, DataFusionError, Result, TableReference};
use datafusion_expr::{
    expr, AggregateUDF, Expr, ExprSchemable, Extension, LogicalPlan, Projection, ScalarUDF,
};
use sail_common::spec;
use sail_common_datafusion::udf::StreamUDF;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::cereal::pyspark_udtf::PySparkUdtfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use sail_python_udf::udf::pyspark_udtf::{PySparkUDTF, PySparkUdtfKind};

use crate::error::{PlanError, PlanResult};
use crate::extension::logical::MapPartitionsNode;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::table_input::TableInputRewriter;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

pub(super) struct PythonUdf {
    pub python_version: String,
    pub eval_type: spec::PySparkUdfType,
    pub command: Vec<u8>,
    pub output_type: DataType,
}

pub(super) struct PythonUdtf {
    pub python_version: String,
    pub eval_type: spec::PySparkUdfType,
    pub command: Vec<u8>,
    pub return_type: DataType,
}

impl PlanResolver<'_> {
    pub(super) fn resolve_python_udf(
        &self,
        function: spec::FunctionDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<PythonUdf> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let (output_type, eval_type, command, python_version) = match function {
            spec::FunctionDefinition::PythonUdf {
                output_type,
                eval_type,
                command,
                python_version,
            } => (output_type, eval_type, command, python_version),
            spec::FunctionDefinition::ScalarScalaUdf { .. } => {
                return Err(PlanError::todo("Scala UDF is not supported yet"));
            }
            spec::FunctionDefinition::JavaUdf { class_name, .. } => {
                return plan_err!("Can not load class {class_name}")?;
            }
        };
        let output_type = self.resolve_data_type(&output_type, state)?;
        Ok(PythonUdf {
            python_version,
            eval_type,
            command,
            output_type,
        })
    }

    pub(super) fn resolve_python_udtf(
        &self,
        function: spec::TableFunctionDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<PythonUdtf> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let (return_type, eval_type, command, python_version) = match function {
            spec::TableFunctionDefinition::PythonUdtf {
                return_type,
                eval_type,
                command,
                python_version,
            } => (return_type, eval_type, command, python_version),
        };
        let return_type = self.resolve_data_type(&return_type, state)?;
        Ok(PythonUdtf {
            python_version,
            eval_type,
            command,
            return_type,
        })
    }

    pub(super) fn resolve_python_udf_expr(
        &self,
        function: PythonUdf,
        name: &str,
        arguments: Vec<Expr>,
        argument_names: &[String],
        schema: &DFSchemaRef,
        deterministic: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<Expr> {
        use spec::PySparkUdfType;

        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let input_types: Vec<DataType> = arguments
            .iter()
            .map(|arg| arg.get_type(schema))
            .collect::<Result<Vec<DataType>, DataFusionError>>()?;
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
            | PySparkUdfType::WindowAggPandas
            | PySparkUdfType::MapPandasIter
            | PySparkUdfType::CogroupedMapPandas
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
                    argument_names.to_vec(),
                    input_types,
                    function.output_type,
                    self.config.pyspark_udf_config.clone(),
                );
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: Arc::new(AggregateUDF::from(udaf)),
                    args: arguments,
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                }))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn resolve_python_udtf_plan(
        &self,
        function: PythonUdtf,
        name: &str,
        plan: LogicalPlan,
        arguments: Vec<NamedExpr>,
        function_output_names: Option<Vec<String>>,
        function_output_qualifier: Option<TableReference>,
        deterministic: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let payload = PySparkUdtfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            arguments.len(),
            &function.return_type,
            &self.config.pyspark_udf_config,
        )?;
        let kind = match function.eval_type {
            spec::PySparkUdfType::Table => PySparkUdtfKind::Table,
            spec::PySparkUdfType::ArrowTable => PySparkUdtfKind::ArrowTable,
            _ => {
                return Err(PlanError::invalid(format!(
                    "PySpark UDTF type: {:?}",
                    function.eval_type,
                )))
            }
        };
        // Determine the number of passthrough columns before rewriting the plan
        // for `TABLE (...)` arguments.
        let passthrough_columns = plan.schema().fields().len();
        let projections = plan
            .schema()
            .columns()
            .into_iter()
            .map(|col| {
                Ok(NamedExpr::new(
                    vec![state.get_field_info(col.name())?.name().to_string()],
                    Expr::Column(col),
                ))
            })
            .chain(arguments.into_iter().map(Ok))
            .collect::<PlanResult<Vec<_>>>()?;
        let (plan, projections) =
            self.rewrite_projection::<TableInputRewriter>(plan, projections, state)?;
        let input_names = projections
            .iter()
            .map(|e| e.name.clone().one())
            .collect::<Result<_>>()?;
        let projections = self.rewrite_named_expressions(projections, state)?;
        let input_types = projections
            .iter()
            .map(|e| e.get_type(plan.schema()))
            .collect::<Result<Vec<_>>>()?;
        let udtf = PySparkUDTF::try_new(
            kind,
            get_udf_name(name, &payload),
            payload,
            input_names,
            input_types,
            passthrough_columns,
            function.return_type,
            function_output_names,
            deterministic,
            self.config.pyspark_udf_config.clone(),
        )?;
        let output_names = state.register_fields(udtf.output_schema().fields());
        let output_qualifiers = (0..output_names.len())
            .map(|i| {
                if i < passthrough_columns {
                    let (qualifier, _) = plan.schema().qualified_field(i);
                    qualifier.cloned()
                } else {
                    function_output_qualifier.clone()
                }
            })
            .collect();
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(MapPartitionsNode::try_new(
                Arc::new(LogicalPlan::Projection(Projection::try_new(
                    projections,
                    Arc::new(plan),
                )?)),
                output_names,
                output_qualifiers,
                Arc::new(udtf),
            )?),
        }))
    }
}
