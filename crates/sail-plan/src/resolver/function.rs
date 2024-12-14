use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{expr, AggregateUDF, Expr, ExprSchemable, ScalarUDF};
use sail_common::spec;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};

use crate::error::{PlanError, PlanResult};
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
            _ => {
                return Err(PlanError::invalid("UDF function type must be Python UDF"));
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
}
