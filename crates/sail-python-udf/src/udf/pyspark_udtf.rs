use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::pyarrow::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_expr::expr::Alias;
use datafusion_expr::{Expr, TableType};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyList, PyTuple};
use sail_common::config::SparkUdfConfig;
use sail_common::spec::TableFunctionDefinition;
use sail_common::utils::cast_record_batch;

use crate::cereal::is_pyspark_arrow_udf;
use crate::cereal::pyspark_udtf::{deserialize_pyspark_udtf, PySparkUDTF as CerealPySparkUDTF};
use crate::udf::{
    build_pyarrow_record_batch_kwargs, get_pyarrow_record_batch_from_pandas_function,
    get_pyarrow_record_batch_from_pylist_function, get_pyarrow_schema,
    get_python_builtins_list_function, PythonFunctionType,
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
        _state: &SessionState,
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

#[derive(Debug, Clone)]
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
        python_function: PythonFunctionType,
    ) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let python_function: Bound<PyAny> = python_function.get_python_function(py)?;
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let record_batch_from_pandas: Bound<PyAny> =
                get_pyarrow_record_batch_from_pandas_function(py)?;

            let py_args: Vec<Bound<PyAny>> = args
                .iter()
                .map(|arg| {
                    let arg = arg
                        .into_data()
                        .to_pyarrow(py)
                        .map_err(|err| DataFusionError::External(err.into()))?
                        .call_method0(py, pyo3::intern!(py, "to_pandas"))
                        .map_err(|err| DataFusionError::External(err.into()))?
                        .clone_ref(py)
                        .into_bound(py);
                    Ok(arg)
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            let py_args: Bound<PyList> = PyList::new_bound(py, &py_args);

            let results: Bound<PyAny> = python_function
                .call1((py.None(), (py_args,)))
                .map_err(|err| DataFusionError::External(err.into()))?;
            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| DataFusionError::External(err.into()))?
                .get_item(0)
                .map_err(|err| DataFusionError::External(err.into()))?;

            let results_data: Bound<PyAny> = results
                .get_item(0)
                .map_err(|err| DataFusionError::External(err.into()))?;
            let _results_datatype: Bound<PyAny> = results
                .get_item(1)
                .map_err(|err| DataFusionError::External(err.into()))?;

            let record_batch: Bound<PyAny> = record_batch_from_pandas
                .call1((results_data,))
                .map_err(|err| DataFusionError::External(err.into()))?;
            let record_batch: RecordBatch = RecordBatch::from_pyarrow_bound(&record_batch)
                .map_err(|err| DataFusionError::External(err.into()))?;

            Ok(record_batch)
        })
    }

    fn apply_pyspark_function(
        &self,
        args: &[ArrayRef],
        python_function: PythonFunctionType,
    ) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let python_function: Bound<PyAny> = python_function.get_python_function(py)?;
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let record_batch_from_pylist: Bound<PyAny> =
                get_pyarrow_record_batch_from_pylist_function(py)?;
            let pyarrow_schema: Bound<PyAny> = get_pyarrow_schema(&self.return_schema, py)?;
            let pyarrow_record_batch_kwargs: Bound<PyDict> =
                build_pyarrow_record_batch_kwargs(py, pyarrow_schema)?;

            let py_args: Vec<Bound<PyAny>> = args
                .iter()
                .map(|arg| {
                    let arg = arg
                        .into_data()
                        .to_pyarrow(py)
                        .map_err(|err| DataFusionError::External(err.into()))?
                        .call_method0(py, pyo3::intern!(py, "to_pylist"))
                        .map_err(|err| DataFusionError::External(err.into()))?
                        .clone_ref(py)
                        .into_bound(py);
                    Ok(arg)
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            let py_args: Bound<PyTuple> = PyTuple::new_bound(py, &py_args);
            let py_args: Bound<PyAny> = py
                .eval_bound("zip", None, None)
                .map_err(|err| DataFusionError::External(err.into()))?
                .call1(&py_args)
                .map_err(|err| DataFusionError::External(err.into()))?;
            let py_args: Bound<PyIterator> = PyIterator::from_bound_object(&py_args)
                .map_err(|err| DataFusionError::External(err.into()))?;

            let results: Bound<PyList> = PyList::empty_bound(py);
            for py_arg in py_args {
                let py_arg = py_arg.map_err(|err| DataFusionError::External(err.into()))?;
                let result: Bound<PyAny> = python_function
                    .call1((py.None(), (py_arg,)))
                    .map_err(|err| DataFusionError::External(err.into()))?;
                let result: Bound<PyAny> = builtins_list
                    .call1((result,))
                    .map_err(|err| DataFusionError::External(err.into()))?
                    .get_item(0)
                    .map_err(|err| DataFusionError::External(err.into()))?;
                let result: Bound<PyAny> = builtins_list
                    .call1((result,))
                    .map_err(|err| DataFusionError::External(err.into()))?;
                let result: Bound<PyList> =
                    list_of_tuples_to_list_of_dicts(py, &result, &self.return_schema)?;
                for item in result.iter() {
                    results
                        .append(item)
                        .map_err(|err| DataFusionError::External(err.into()))?;
                }
            }
            let record_batch: Bound<PyAny> = record_batch_from_pylist
                .call((results,), Some(&pyarrow_record_batch_kwargs))
                .map_err(|err| DataFusionError::External(err.into()))?;
            let record_batch: RecordBatch = RecordBatch::from_pyarrow_bound(&record_batch)
                .map_err(|err| DataFusionError::External(err.into()))?;

            Ok(record_batch)
        })
    }

    fn apply_pyspark_function_no_args(
        &self,
        python_function: PythonFunctionType,
    ) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let python_function: Bound<PyAny> = python_function.get_python_function(py)?;
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let record_batch_from_pylist: Bound<PyAny> =
                get_pyarrow_record_batch_from_pylist_function(py)?;
            let pyarrow_schema: Bound<PyAny> = get_pyarrow_schema(&self.return_schema, py)?;
            let pyarrow_record_batch_kwargs: Bound<PyDict> =
                build_pyarrow_record_batch_kwargs(py, pyarrow_schema)?;

            let results: Bound<PyAny> = python_function
                .call1((py.None(), (PyList::empty_bound(py),)))
                .map_err(|err| DataFusionError::External(err.into()))?;
            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| DataFusionError::External(err.into()))?
                .get_item(0)
                .map_err(|err| DataFusionError::External(err.into()))?;
            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| DataFusionError::External(err.into()))?;
            let results: Bound<PyList> =
                list_of_tuples_to_list_of_dicts(py, &results, &self.return_schema)?;

            let record_batch: Bound<PyAny> = record_batch_from_pylist
                .call((results,), Some(&pyarrow_record_batch_kwargs))
                .map_err(|err| DataFusionError::External(err.into()))?;
            let record_batch: RecordBatch = RecordBatch::from_pyarrow_bound(&record_batch)
                .map_err(|err| DataFusionError::External(err.into()))?;

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

        let python_function: CerealPySparkUDTF = deserialize_pyspark_udtf(
            python_version,
            command,
            eval_type,
            &(exprs.len() as i32),
            &self.return_type,
            &self.spark_udf_config,
        )
        .map_err(|err| DataFusionError::External(err.into()))?;
        let python_function = PythonFunctionType::PySparkUDTF(python_function);

        if exprs.is_empty() {
            let batches: RecordBatch = if is_pyspark_arrow_udf(eval_type) {
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

        let batches: RecordBatch = if is_pyspark_arrow_udf(eval_type) {
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
) -> Result<Bound<'py, PyList>, DataFusionError> {
    let fields = schema.fields();
    let list_of_dicts: Vec<Bound<PyDict>> = results
        .iter()
        .map_err(|err| DataFusionError::External(err.into()))?
        .map(|result| {
            result
                .map_err(|err| DataFusionError::External(err.into()))
                .and_then(|result| {
                    let dict: Bound<PyDict> = PyDict::new_bound(py);
                    for (i, field) in fields.iter().enumerate() {
                        let field_name = field.name().as_str();
                        let value = result
                            .get_item(i)
                            .map_err(|err| DataFusionError::External(err.into()))?;
                        dict.set_item(field_name, value)
                            .map_err(|err| DataFusionError::External(err.into()))?;
                    }
                    Ok(dict)
                })
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    Ok(PyList::new_bound(py, list_of_dicts))
}
