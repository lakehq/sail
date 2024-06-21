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
use datafusion_expr::{Expr, TableType};
use pyo3::prelude::*;
use pyo3::types::{PyIterator, PyList, PyTuple};

use crate::cereal::is_pyspark_arrow_udf;
use crate::cereal::pyspark_udtf::PySparkUDTF as CerealPySparkUDTF;
use crate::udf::{get_python_builtins_list_function, get_python_function, CommonPythonUDF};

#[derive(Debug, Clone)]
pub struct PySparkUDT {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl PySparkUDT {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

#[async_trait]
impl TableProvider for PySparkUDT {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    #[allow(dead_code)]
    function_name: String,
    #[allow(dead_code)]
    input_types: Vec<DataType>,
    schema: SchemaRef,
    #[allow(dead_code)]
    output_type: DataType,
    python_function: CerealPySparkUDTF,
    #[allow(dead_code)]
    deterministic: bool,
    #[allow(dead_code)]
    eval_type: i32,
}

impl CommonPythonUDF for PySparkUDTF {
    type PythonFunctionType = CerealPySparkUDTF;

    fn python_function(&self) -> &Self::PythonFunctionType {
        &self.python_function
    }
}

impl PySparkUDTF {
    pub fn new(
        function_name: String,
        input_types: Vec<DataType>,
        schema: SchemaRef,
        output_type: DataType,
        python_function: CerealPySparkUDTF,
        deterministic: bool,
        eval_type: i32,
    ) -> Self {
        Self {
            function_name,
            input_types,
            schema,
            output_type,
            python_function,
            deterministic,
            eval_type,
        }
    }

    fn apply_pyspark_arrow_function(&self, args: &[ArrayRef]) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let python_function: Bound<PyAny> = get_python_function(self, py)?;

            let record_batch_from_pandas: Bound<PyAny> =
                PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("pyarrow import error: {}", err))
                    })?
                    .getattr(pyo3::intern!(py, "RecordBatch"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("pyarrow RecordBatch error: {}", err))
                    })?
                    .getattr(pyo3::intern!(py, "from_pandas"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!(
                            "pyarrow RecordBatch from_pandas error: {}",
                            err
                        ))
                    })?;

            let py_args: Vec<Bound<PyAny>> = args
                .iter()
                .map(|arg| {
                    arg.into_data()
                        .to_pyarrow(py)
                        .unwrap()
                        .clone_ref(py)
                        .into_bound(py)
                })
                .collect::<Vec<_>>();
            let py_args: Bound<PyTuple> = PyTuple::new_bound(py, &py_args);

            let results: Bound<PyAny> =
                python_function
                    .call1((py.None(), (py_args,)))
                    .map_err(|e| {
                        DataFusionError::Execution(format!("PySpark Arrow UDTF Result: {e:?}"))
                    })?;
            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark Arrow UDTF Error calling list(): {}",
                        err
                    ))
                })?
                .get_item(0)
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark Arrow UDTF Result list() first get_item(0): {}",
                        err
                    ))
                })?;

            let results_data: Bound<PyAny> = results.get_item(0).map_err(|err| {
                DataFusionError::Internal(format!(
                    "PySpark Arrow UDTF Result list get_item(0): {}",
                    err
                ))
            })?;
            let _results_datatype: Bound<PyAny> = results.get_item(1).map_err(|err| {
                DataFusionError::Internal(format!(
                    "PySpark Arrow UDTF Result list get_item(0): {}",
                    err
                ))
            })?;

            let record_batch: Bound<PyAny> = record_batch_from_pandas
                .call1((results_data,))
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark Arrow UDTF record_batch_from_pandas {}",
                        err
                    ))
                })?;
            let record_batch: RecordBatch = RecordBatch::from_pyarrow_bound(&record_batch)
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark Arrow UDTF record_batch {}", err))
                })?;

            Ok(record_batch)
        })
    }

    fn apply_pyspark_function(&self, args: &[ArrayRef]) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let python_function: Bound<PyAny> = get_python_function(self, py)?;

            let record_batch_from_pandas: Bound<PyAny> =
                PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("pyarrow import error: {}", err))
                    })?
                    .getattr(pyo3::intern!(py, "RecordBatch"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("pyarrow RecordBatch error: {}", err))
                    })?
                    .getattr(pyo3::intern!(py, "from_pandas"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!(
                            "pyarrow RecordBatch from_pandas error: {}",
                            err
                        ))
                    })?;

            let py_args: Vec<Bound<PyAny>> = args
                .iter()
                .map(|arg| {
                    arg.into_data()
                        .to_pyarrow(py)
                        .unwrap()
                        .call_method0(py, pyo3::intern!(py, "to_pylist"))
                        .unwrap()
                        .clone_ref(py)
                        .into_bound(py)
                })
                .collect::<Vec<_>>();
            let py_args: Bound<PyTuple> = PyTuple::new_bound(py, &py_args);
            let py_args: Bound<PyAny> = py
                .eval_bound("zip", None, None)
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDTF py_args_zip eval_bound{}", err))
                })?
                .call1(&py_args)
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDTF py_args_zip zip{}", err))
                })?;
            let py_args: Bound<PyIterator> =
                PyIterator::from_bound_object(&py_args).map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDTF py_args_iter {}", err))
                })?;

            let results: Vec<Bound<PyAny>> = py_args
                .map(|py_arg| -> Result<Bound<PyAny>, DataFusionError> {
                    let result: Bound<PyAny> = python_function
                        .call1((py.None(), (py_arg.unwrap(),)))
                        .map_err(|e| {
                            DataFusionError::Execution(format!("PySpark UDTF Result: {e:?}"))
                        })?;
                    println!("CHECK HERE result: {}", result);
                    let result: Bound<PyAny> = builtins_list
                        .call1((result,))
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "PySpark UDTF Error calling list(): {}",
                                err
                            ))
                        })?
                        .get_item(0)
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "PySpark UDTF Result list() first get_item(0): {}",
                                err
                            ))
                        })?;
                    println!("CHECK HERE result: {}", result);
                    Ok(result)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDTF Results: {}", err))
                })?;

            let record_batch: Bound<PyAny> =
                record_batch_from_pandas.call1((results,)).map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark Arrow UDTF record_batch_from_pandas {}",
                        err
                    ))
                })?;
            let record_batch: RecordBatch = RecordBatch::from_pyarrow_bound(&record_batch)
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark Arrow UDTF record_batch {}", err))
                })?;

            Ok(record_batch)
        })
    }

    fn apply_pyspark_function_no_args(&self) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let python_function: Bound<PyAny> = get_python_function(self, py)?;

            let record_batch_from_pylist: Bound<PyAny> =
                PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("pyarrow import error: {}", err))
                    })?
                    .getattr(pyo3::intern!(py, "RecordBatch"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("pyarrow RecordBatch error: {}", err))
                    })?
                    .getattr(pyo3::intern!(py, "from_pylist"))
                    .map_err(|err| {
                        DataFusionError::Internal(format!(
                            "pyarrow RecordBatch from_pylist error: {}",
                            err
                        ))
                    })?;

            let results: Bound<PyAny> = python_function
                .call1((py.None(), (PyList::empty_bound(py),)))
                .map_err(|e| {
                    DataFusionError::Execution(format!("PySpark UDTF No Args Result: {e:?}"))
                })?;
            println!("CHECK HERE results: {:?}", results);
            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark UDTF No Args Error calling list(): {}",
                        err
                    ))
                })?
                .get_item(0)
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark UDTF No Args Result list() first get_item(0): {}",
                        err
                    ))
                })?
                .get_item(0)
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark UDTF No Args Result list() first get_item(0): {}",
                        err
                    ))
                })?;
            println!("CHECK HERE results: {:?}", results);
            let results: Bound<PyAny> = builtins_list.call1((results,)).map_err(|err| {
                DataFusionError::Internal(format!(
                    "PySpark UDTF No Args Error converting tuple to list: {}",
                    err
                ))
            })?;
            println!("CHECK HERE results: {:?}", results);

            let record_batch: Bound<PyAny> =
                record_batch_from_pylist.call1((results,)).map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark Arrow UDTF record_batch_from_pylist {}",
                        err
                    ))
                })?;
            let record_batch: RecordBatch = RecordBatch::from_pyarrow_bound(&record_batch)
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark Arrow UDTF record_batch {}", err))
                })?;

            Ok(record_batch)
        })
    }
}

impl TableFunctionImpl for PySparkUDTF {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.is_empty() {
            let batches: RecordBatch = if is_pyspark_arrow_udf(&self.eval_type) {
                self.apply_pyspark_arrow_function(&[])?
            } else {
                self.apply_pyspark_function_no_args()?
            };
            return Ok(Arc::new(PySparkUDT::new(
                self.schema.clone(),
                vec![batches],
            )));
        }

        let mut input_types = Vec::new();
        let mut input_arrays = Vec::new();

        for expr in exprs {
            // https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_udtf.html
            // args can either be scalar exprs or table args that represent entire input tables.
            match expr {
                Expr::Literal(scalar_value) => {
                    let array_ref = scalar_value.to_array().map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to convert scalar to array: {}",
                            e
                        ))
                    })?;
                    input_types.push(array_ref.data_type().clone());
                    input_arrays.push(array_ref);
                }
                _ => {
                    // TODO: Support table args
                    return Err(DataFusionError::NotImplemented(
                        "Only literal expressions are supported in Python UDTFs for now"
                            .to_string(),
                    ));
                }
            }
        }

        let batches: RecordBatch = if is_pyspark_arrow_udf(&self.eval_type) {
            self.apply_pyspark_arrow_function(&input_arrays)?
        } else {
            self.apply_pyspark_function(&input_arrays)?
        };

        Ok(Arc::new(PySparkUDT::new(
            self.schema.clone(),
            vec![batches],
        )))
    }
}
