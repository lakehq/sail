use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef};
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
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyList, PyTuple};
use sail_common::config::SparkUdfConfig;
use sail_common::spec::TableFunctionDefinition;
use sail_common::utils::cast_record_batch;

use crate::cereal::pyspark_udtf::{deserialize_pyspark_udtf, PySparkUdtfObject};
use crate::cereal::PythonFunction;
use crate::error::PyUdfResult;
use crate::udf::{
    build_pyarrow_record_batch_kwargs, get_pyarrow_record_batch_from_pandas_function,
    get_pyarrow_record_batch_from_pylist_function, get_pyarrow_schema,
    get_python_builtins_list_function,
};

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
    return_type: DataType,
    return_schema: SchemaRef,
    table_function_definition: TableFunctionDefinition,
    spark_udf_config: SparkUdfConfig,
    #[allow(dead_code)]
    deterministic: bool,
}

impl PySparkUDTF {
    pub fn new(
        return_type: DataType,
        return_schema: SchemaRef,
        table_function_definition: TableFunctionDefinition,
        spark_udf_config: SparkUdfConfig,
        deterministic: bool,
    ) -> Self {
        Self {
            return_type,
            return_schema,
            table_function_definition,
            spark_udf_config,
            deterministic,
        }
    }

    fn apply_pyspark_arrow_function(
        &self,
        args: &[ArrayRef],
        python_function: PySparkUdtfObject,
    ) -> PyUdfResult<RecordBatch> {
        Python::with_gil(|py| {
            let python_function = python_function.function(py)?;
            let builtins_list = get_python_builtins_list_function(py)?;
            let record_batch_from_pandas = get_pyarrow_record_batch_from_pandas_function(py)?;

            let py_args = args
                .iter()
                .map(|arg| {
                    let arg = arg
                        .into_data()
                        .to_pyarrow(py)?
                        .call_method0(py, intern!(py, "to_pandas"))?
                        .clone_ref(py)
                        .into_bound(py);
                    Ok(arg)
                })
                .collect::<PyUdfResult<Vec<_>>>()?;
            let py_args = PyList::new_bound(py, &py_args);

            let results = python_function.call1((py.None(), (py_args,)))?;
            let results = builtins_list.call1((results,))?.get_item(0)?;

            let results_data = results.get_item(0)?;
            let _results_datatype = results.get_item(1)?;

            let record_batch = record_batch_from_pandas.call1((results_data,))?;
            let record_batch = RecordBatch::from_pyarrow_bound(&record_batch)?;

            Ok(record_batch)
        })
    }

    fn apply_pyspark_function(
        &self,
        args: &[ArrayRef],
        python_function: PySparkUdtfObject,
    ) -> PyUdfResult<RecordBatch> {
        Python::with_gil(|py| {
            let python_function = python_function.function(py)?;
            let builtins_list = get_python_builtins_list_function(py)?;
            let record_batch_from_pylist = get_pyarrow_record_batch_from_pylist_function(py)?;
            let pyarrow_schema = get_pyarrow_schema(&self.return_schema, py)?;
            let pyarrow_record_batch_kwargs =
                build_pyarrow_record_batch_kwargs(py, pyarrow_schema)?;

            let py_args = args
                .iter()
                .map(|arg| {
                    let arg = arg
                        .into_data()
                        .to_pyarrow(py)?
                        .call_method0(py, intern!(py, "to_pylist"))?
                        .clone_ref(py)
                        .into_bound(py);
                    Ok(arg)
                })
                .collect::<PyUdfResult<Vec<_>>>()?;
            let py_args = PyTuple::new_bound(py, &py_args);
            let py_args = py.eval_bound("zip", None, None)?.call1(&py_args)?;
            let py_args = PyIterator::from_bound_object(&py_args)?;

            let results = PyList::empty_bound(py);
            for py_arg in py_args {
                let py_arg = py_arg?;
                let result = python_function.call1((py.None(), (py_arg,)))?;
                let result = builtins_list.call1((result,))?.get_item(0)?;
                let result = builtins_list.call1((result,))?;
                let result = list_of_tuples_to_list_of_dicts(py, &result, &self.return_schema)?;
                for item in result.iter() {
                    results.append(item)?;
                }
            }
            let record_batch =
                record_batch_from_pylist.call((results,), Some(&pyarrow_record_batch_kwargs))?;
            let record_batch = RecordBatch::from_pyarrow_bound(&record_batch)?;

            Ok(record_batch)
        })
    }

    fn apply_pyspark_function_no_args(
        &self,
        python_function: PySparkUdtfObject,
    ) -> PyUdfResult<RecordBatch> {
        Python::with_gil(|py| {
            let python_function = python_function.function(py)?;
            let builtins_list = get_python_builtins_list_function(py)?;
            let record_batch_from_pylist = get_pyarrow_record_batch_from_pylist_function(py)?;
            let pyarrow_schema = get_pyarrow_schema(&self.return_schema, py)?;
            let pyarrow_record_batch_kwargs =
                build_pyarrow_record_batch_kwargs(py, pyarrow_schema)?;

            let results = python_function.call1((py.None(), (PyList::empty_bound(py),)))?;
            let results = builtins_list.call1((results,))?.get_item(0)?;
            let results = builtins_list.call1((results,))?;
            let results = list_of_tuples_to_list_of_dicts(py, &results, &self.return_schema)?;

            let record_batch =
                record_batch_from_pylist.call((results,), Some(&pyarrow_record_batch_kwargs))?;
            let record_batch = RecordBatch::from_pyarrow_bound(&record_batch)?;

            Ok(record_batch)
        })
    }
}

impl TableFunctionImpl for PySparkUDTF {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (_return_type, eval_type, command, python_version) =
            match &self.table_function_definition {
                TableFunctionDefinition::PythonUdtf {
                    return_type,
                    eval_type,
                    command,
                    python_version,
                } => (return_type, eval_type, command, python_version),
            };

        let python_function: PySparkUdtfObject = deserialize_pyspark_udtf(
            python_version,
            command,
            *eval_type,
            exprs.len(),
            &self.return_type,
            &self.spark_udf_config,
        )?;

        if exprs.is_empty() {
            let batches: RecordBatch = if eval_type.is_arrow_udf() {
                self.apply_pyspark_arrow_function(&[], python_function)?
            } else {
                self.apply_pyspark_function_no_args(python_function)?
            };
            return Ok(Arc::new(PySparkUserDefinedTable::try_new(
                self.return_schema.clone(),
                vec![batches],
            )?));
        }

        let mut input_arrays = Vec::new();
        for expr in exprs {
            // https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_udtf.html
            // args can either be scalar exprs or table args that represent entire input tables.
            match expr {
                Expr::Literal(scalar_value) => {
                    let array_ref = scalar_value.to_array().map_err(|err| {
                        DataFusionError::Execution(format!(
                            "Failed to convert scalar to array: {}",
                            err
                        ))
                    })?;
                    input_arrays.push(array_ref);
                }
                Expr::Alias(Alias { ref expr, .. }) => {
                    if let Expr::Literal(ref scalar_value) = **expr {
                        let array_ref = scalar_value.to_array().map_err(|err| {
                            DataFusionError::Execution(format!(
                                "Failed to convert scalar to array: {}",
                                err
                            ))
                        })?;
                        input_arrays.push(array_ref);
                    } else {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Only literal expr are supported in Python UDTFs for now, got expr: {}",
                            expr
                        )));
                    }
                }
                other => {
                    // TODO: Support table args
                    return Err(DataFusionError::NotImplemented(format!(
                        "Only literal expr are supported in Python UDTFs for now, got expr: {}, other: {}",
                        expr, other
                    )));
                }
            }
        }

        let batches: RecordBatch = if eval_type.is_arrow_udf() {
            self.apply_pyspark_arrow_function(&input_arrays, python_function)?
        } else {
            self.apply_pyspark_function(&input_arrays, python_function)?
        };

        Ok(Arc::new(PySparkUserDefinedTable::try_new(
            self.return_schema.clone(),
            vec![batches],
        )?))
    }
}

fn list_of_tuples_to_list_of_dicts<'py>(
    py: Python<'py>,
    results: &Bound<'py, PyAny>,
    schema: &SchemaRef,
) -> PyUdfResult<Bound<'py, PyList>> {
    let fields = schema.fields();
    let list_of_dicts = results
        .iter()?
        .map(|result| -> PyUdfResult<_> {
            let result = result?;
            let dict = PyDict::new_bound(py);
            for (i, field) in fields.iter().enumerate() {
                let field_name = field.name().as_str();
                let value = result.get_item(i)?;
                dict.set_item(field_name, value)?;
            }
            Ok(dict)
        })
        .collect::<PyUdfResult<Vec<_>>>()?;
    Ok(PyList::new_bound(py, list_of_dicts))
}
