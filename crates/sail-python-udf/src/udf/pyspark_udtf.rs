use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes as adt;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::pyarrow::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_expr::expr::Alias;
use datafusion_expr::{Expr, TableType};
use pyo3::prelude::PyAnyMethods;
use pyo3::Python;
use sail_common::spec;
use sail_common::utils::cast_record_batch;

use crate::cereal::pyspark_udtf::PySparkUdtfPayload;
use crate::config::SparkUdfConfig;
use crate::conversion::TryToPy;
use crate::error::{PyUdfError, PyUdfResult};
use crate::utils::spark::PySpark;

#[derive(Debug, Clone)]
pub struct PySparkUserDefinedTable {
    return_schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl PySparkUserDefinedTable {
    pub fn try_new(return_schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        let batches = batches
            .into_iter()
            .map(|batch| cast_record_batch(batch, return_schema.clone()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            return_schema,
            batches,
        })
    }
}

#[async_trait]
impl TableProvider for PySparkUserDefinedTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.return_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: Implement Stream
        let exec = MemoryExec::try_new(
            &[self.batches.clone()],
            TableProvider::schema(self),
            projection.cloned(),
        )?;
        Ok(Arc::new(exec))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PySparkUDTF {
    python_version: String,
    eval_type: spec::PySparkUdfType,
    command: Vec<u8>,
    return_type: DataType,
    return_schema: SchemaRef,
    spark_udf_config: SparkUdfConfig,
    #[allow(dead_code)]
    deterministic: bool,
}

#[derive(PartialEq, PartialOrd)]
struct PySparkUDTFOrd<'a> {
    python_version: &'a str,
    eval_type: spec::PySparkUdfType,
    command: &'a [u8],
    return_type: &'a DataType,
    spark_udf_config: &'a SparkUdfConfig,
    deterministic: &'a bool,
}

impl<'a> From<&'a PySparkUDTF> for PySparkUDTFOrd<'a> {
    fn from(udtf: &'a PySparkUDTF) -> Self {
        Self {
            python_version: &udtf.python_version,
            eval_type: udtf.eval_type,
            command: &udtf.command,
            return_type: &udtf.return_type,
            spark_udf_config: &udtf.spark_udf_config,
            deterministic: &udtf.deterministic,
        }
    }
}

impl PartialOrd for PySparkUDTF {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PySparkUDTFOrd::from(self).partial_cmp(&other.into())
    }
}

impl PySparkUDTF {
    pub fn try_new(
        python_version: String,
        eval_type: spec::PySparkUdfType,
        command: Vec<u8>,
        return_type: DataType,
        spark_udf_config: SparkUdfConfig,
        deterministic: bool,
    ) -> PyUdfResult<Self> {
        let return_schema: SchemaRef = match return_type {
            DataType::Struct(ref fields) => {
                Arc::new(adt::Schema::new(fields.clone()))
            },
            _ => {
                return Err(PyUdfError::invalid(format!(
                    "Invalid Python user-defined table function return type. Expect a struct type, but got {}",
                    return_type
                )))
            }
        };
        Ok(Self {
            python_version,
            eval_type,
            command,
            return_type,
            return_schema,
            spark_udf_config,
            deterministic,
        })
    }
}

impl TableFunctionImpl for PySparkUDTF {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let inputs = exprs
            .iter()
            .map(|expr| {
                // https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_udtf.html
                // args can either be scalar exprs or table args that represent entire input tables.
                match expr {
                    Expr::Literal(scalar_value) => {
                        scalar_value.to_array().map_err(|err| {
                            DataFusionError::Execution(format!(
                                "Failed to convert scalar to array: {}",
                                err
                            ))
                        })
                    }
                    Expr::Alias(Alias { ref expr, .. }) => {
                        if let Expr::Literal(ref scalar_value) = **expr {
                            scalar_value.to_array().map_err(|err| {
                                DataFusionError::Execution(format!(
                                    "Failed to convert scalar to array: {}",
                                    err
                                ))
                            })
                        } else {
                            Err(DataFusionError::NotImplemented(format!(
                                "Only literal expr are supported in Python UDTFs for now, got expr: {}",
                                expr
                            )))
                        }
                    }
                    other => {
                        // TODO: Support table args
                        Err(DataFusionError::NotImplemented(format!(
                            "Only literal expr are supported in Python UDTFs for now, got expr: {}, other: {}",
                            expr, other
                        )))
                    }
                }
            })
            .collect::<Result<Vec<ArrayRef>>>()?;

        let payload = PySparkUdtfPayload::build(
            &self.python_version,
            &self.command,
            self.eval_type,
            exprs.len(),
            &self.return_type,
            &self.spark_udf_config,
        )?;
        let batch = Python::with_gil(|py| -> PyUdfResult<_> {
            let udtf = PySparkUdtfPayload::load(py, &payload)?;
            let udtf = match self.eval_type {
                spec::PySparkUdfType::Table => PySpark::table_udf(py, udtf, &self.return_schema)?,
                spec::PySparkUdfType::ArrowTable => PySpark::arrow_table_udf(py, udtf)?,
                _ => {
                    return Err(PyUdfError::invalid(format!(
                        "Invalid PySpark UDTF type: {:?}",
                        self.eval_type
                    )))
                }
            };
            let batch = udtf.call1((inputs.try_to_py(py)?,))?;
            Ok(RecordBatch::from_pyarrow_bound(&batch)?)
        })?;

        Ok(Arc::new(PySparkUserDefinedTable::try_new(
            self.return_schema.clone(),
            vec![batch],
        )?))
    }
}
