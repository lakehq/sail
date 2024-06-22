use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::SchemaRef;
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

use crate::cereal::is_pyspark_arrow_udf;
use crate::cereal::pyspark_udtf::PySparkUDTF as CerealPySparkUDTF;
use crate::udf::{
    build_pyarrow_record_batch_kwargs, get_pyarrow_record_batch_from_pandas_function,
    get_pyarrow_record_batch_from_pylist_function, get_pyarrow_schema,
    get_python_builtins_list_function, get_python_function, CommonPythonUDF,
};

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
    schema: SchemaRef,
    python_function: CerealPySparkUDTF,
    #[allow(dead_code)]
    deterministic: bool,
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
        schema: SchemaRef,
        python_function: CerealPySparkUDTF,
        deterministic: bool,
        eval_type: i32,
    ) -> Self {
        Self {
            schema,
            python_function,
            deterministic,
            eval_type,
        }
    }

    fn apply_pyspark_arrow_function(&self, args: &[ArrayRef]) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let python_function: Bound<PyAny> = get_python_function(self, py)?;
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let record_batch_from_pandas: Bound<PyAny> =
                get_pyarrow_record_batch_from_pandas_function(py)?;

            let py_args: Vec<Bound<PyAny>> = args
                .iter()
                .map(|arg| {
                    let arg = arg
                        .into_data()
                        .to_pyarrow(py)
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "PySpark Arrow UDTF arg into_data to_pyarrow: {}",
                                err
                            ))
                        })?
                        .call_method0(py, pyo3::intern!(py, "to_pandas"))
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "PySpark Arrow UDTF arg into_data to_pyarrow to_pandas: {}",
                                err
                            ))
                        })?
                        .clone_ref(py)
                        .into_bound(py);
                    Ok(arg)
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
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
            let python_function: Bound<PyAny> = get_python_function(self, py)?;
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let record_batch_from_pylist: Bound<PyAny> =
                get_pyarrow_record_batch_from_pylist_function(py)?;
            let pyarrow_schema: Bound<PyAny> = get_pyarrow_schema(&self.schema, py)?;
            let pyarrow_record_batch_kwargs: Bound<PyDict> =
                build_pyarrow_record_batch_kwargs(py, pyarrow_schema)?;

            let py_args: Vec<Bound<PyAny>> = args
                .iter()
                .map(|arg| {
                    let arg = arg
                        .into_data()
                        .to_pyarrow(py)
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "PySpark UDTF arg into_data to_pyarrow: {}",
                                err
                            ))
                        })?
                        .call_method0(py, pyo3::intern!(py, "to_pylist"))
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "PySpark UDTF arg into_data to_pyarrow to_pylist: {}",
                                err
                            ))
                        })?
                        .clone_ref(py)
                        .into_bound(py);
                    Ok(arg)
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            let py_args: Bound<PyTuple> = PyTuple::new_bound(py, &py_args);
            let py_args: Bound<PyAny> = py
                .eval_bound("zip", None, None)
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark UDTF py_args_zip eval_bound {}",
                        err
                    ))
                })?
                .call1(&py_args)
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDTF py_args_zip zip {}", err))
                })?;
            let py_args: Bound<PyIterator> =
                PyIterator::from_bound_object(&py_args).map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDTF py_args_iter {}", err))
                })?;

            let results: Bound<PyList> = PyList::empty_bound(py);
            for py_arg in py_args {
                let py_arg = py_arg.map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDTF py_arg: {}", err))
                })?;
                let result: Bound<PyAny> =
                    python_function
                        .call1((py.None(), (py_arg,)))
                        .map_err(|err| {
                            DataFusionError::Execution(format!("PySpark UDTF Result: {}", err))
                        })?;
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
                            "PySpark UDTF result list() first get_item(0): {}",
                            err
                        ))
                    })?;
                let result: Bound<PyAny> = builtins_list.call1((result,)).map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark UDTF Error converting tuple to list: {}",
                        err
                    ))
                })?;
                let result: Bound<PyList> =
                    list_of_tuples_to_list_of_dicts(py, &result, &self.schema)?;
                for item in result.iter() {
                    results.append(item).map_err(|err| {
                        DataFusionError::Internal(format!(
                            "PySpark UDTF Error appending item to results: {}",
                            err
                        ))
                    })?;
                }
            }
            let record_batch: Bound<PyAny> = record_batch_from_pylist
                .call((results,), Some(&pyarrow_record_batch_kwargs))
                .map_err(|err| {
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

    fn apply_pyspark_function_no_args(&self) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let python_function: Bound<PyAny> = get_python_function(self, py)?;
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let record_batch_from_pylist: Bound<PyAny> =
                get_pyarrow_record_batch_from_pylist_function(py)?;
            let pyarrow_schema: Bound<PyAny> = get_pyarrow_schema(&self.schema, py)?;
            let pyarrow_record_batch_kwargs: Bound<PyDict> =
                build_pyarrow_record_batch_kwargs(py, pyarrow_schema)?;

            let results: Bound<PyAny> = python_function
                .call1((py.None(), (PyList::empty_bound(py),)))
                .map_err(|e| {
                    DataFusionError::Execution(format!("PySpark UDTF no args Result: {}", e))
                })?;
            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark UDTF no args Error calling list(): {}",
                        err
                    ))
                })?
                .get_item(0)
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark UDTF no args Result list() first get_item(0): {}",
                        err
                    ))
                })?;
            let results: Bound<PyAny> = builtins_list.call1((results,)).map_err(|err| {
                DataFusionError::Internal(format!(
                    "PySpark UDTF no args Error converting tuple to list: {}",
                    err
                ))
            })?;
            let results: Bound<PyList> =
                list_of_tuples_to_list_of_dicts(py, &results, &self.schema)?;

            let record_batch: Bound<PyAny> = record_batch_from_pylist
                .call((results,), Some(&pyarrow_record_batch_kwargs))
                .map_err(|err| {
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
        println!("CHECK HERE EXPRS: {:?}", exprs);
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

        // let mut input_types = Vec::new();
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
                    // input_types.push(array_ref.data_type().clone());
                    input_arrays.push(array_ref);
                }
                Expr::Alias(Alias { ref expr, .. }) => {
                    if let Expr::Literal(ref scalar_value) = **expr {
                        let array_ref = scalar_value.to_array().map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to convert scalar to array: {}",
                                e
                            ))
                        })?;
                        // input_types.push(array_ref.data_type().clone());
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

        println!("CHECK HERE INPUT_ARRAYS: {:?}", input_arrays);
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

fn list_of_tuples_to_list_of_dicts<'py>(
    py: Python<'py>,
    results: &Bound<'py, PyAny>,
    schema: &SchemaRef,
) -> Result<Bound<'py, PyList>, DataFusionError> {
    let fields = schema.fields();
    let list_of_dicts: Vec<Bound<PyDict>> = results
        .iter()
        .map_err(|err| DataFusionError::Internal(format!("results iter: {}", err)))?
        .map(|result| {
            result
                .map_err(|err| DataFusionError::Internal(format!("result iter: {}", err)))
                .and_then(|result| {
                    let dict: Bound<PyDict> = PyDict::new_bound(py);
                    for (i, field) in fields.iter().enumerate() {
                        let field_name = field.name().as_str();
                        let value = result.get_item(i).map_err(|err| {
                            DataFusionError::Internal(format!("result get_item({}): {}", i, err))
                        })?;
                        dict.set_item(field_name, value).map_err(|err| {
                            DataFusionError::Internal(format!(
                                "dict set_item({}, {}): {}",
                                field_name, i, err
                            ))
                        })?;
                    }
                    Ok(dict)
                })
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    Ok(PyList::new_bound(py, list_of_dicts))
}
