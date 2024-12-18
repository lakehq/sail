use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Fields, Schema};
use datafusion_common::{plan_err, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{
    expr, AggregateUDF, Expr, ExprSchemable, Extension, LogicalPlan, Projection, ScalarUDF,
};
use sail_common::spec;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::cereal::pyspark_udtf::PySparkUdtfPayload;
use sail_python_udf::udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use sail_python_udf::udf::pyspark_udtf::{PySparkUDTF, PySparkUdtfKind};

use crate::error::{PlanError, PlanResult};
use crate::extension::logical::MapPartitionsNode;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

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
    ) -> PlanResult<PythonUdf> {
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
        let output_type = self.resolve_data_type(&output_type)?;
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
    ) -> PlanResult<PythonUdtf> {
        let (return_type, eval_type, command, python_version) = match function {
            spec::TableFunctionDefinition::PythonUdtf {
                return_type,
                eval_type,
                command,
                python_version,
            } => (return_type, eval_type, command, python_version),
        };
        let return_type = self.resolve_data_type(&return_type)?;
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
    ) -> PlanResult<Expr> {
        use spec::PySparkUdfType;

        let input_types: Vec<DataType> = arguments
            .iter()
            .map(|arg| arg.get_type(schema))
            .collect::<Result<Vec<DataType>, DataFusionError>>()?;
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            &((0..arguments.len()).collect::<Vec<_>>()),
            &self.config.spark_udf_config,
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

    pub(super) fn resolve_python_udtf_plan(
        &self,
        function: PythonUdtf,
        name: &str,
        input: LogicalPlan,
        arguments: Vec<Expr>,
        argument_names: Vec<String>,
        deterministic: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let num_arguments = arguments.len();
        let input_schema = input.schema();
        let argument_types = arguments
            .iter()
            .map(|arg| arg.get_type(input_schema))
            .collect::<Result<Vec<_>, _>>()?;
        let projections = {
            let mut out = input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect::<Vec<_>>();
            out.extend(arguments);
            out
        };
        let input_field_names = state.get_field_names(input_schema.inner())?;
        let input_names = {
            let mut out = input_field_names.clone();
            out.extend(argument_names);
            out
        };
        let (output_schema, function_output_names) = match &function.return_type {
            DataType::Struct(fields) => {
                let mut output_fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
                output_fields.extend_from_slice(fields.iter().as_ref());
                (
                    Arc::new(Schema::new(Fields::from(output_fields))),
                    fields
                        .iter()
                        .map(|f| state.register_field(f.name()))
                        .collect::<Vec<_>>(),
                )
            }
            _ => {
                // The PySpark unit test expects the exact error message here.
                return Err(PlanError::invalid( format!(
                    "Invalid Python user-defined table function return type. Expect a struct type, but got {}.",
                    function.return_type
                )));
            }
        };
        let output_names = {
            let mut out = input_field_names;
            out.extend(function_output_names);
            out
        };
        let payload = PySparkUdtfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            num_arguments,
            &DataType::Struct(output_schema.fields.clone()),
            &self.config.spark_udf_config,
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
        let udtf = PySparkUDTF::new(
            kind,
            get_udf_name(name, &payload),
            payload,
            argument_types,
            output_schema,
            deterministic,
            self.config.spark_udf_config.timezone.clone(),
            self.config
                .spark_udf_config
                .pandas_convert_to_arrow_array_safely,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(MapPartitionsNode::try_new(
                Arc::new(LogicalPlan::Projection(Projection::try_new(
                    projections,
                    Arc::new(input),
                )?)),
                input_names,
                output_names,
                Arc::new(udtf),
            )?),
        }))
    }
}
