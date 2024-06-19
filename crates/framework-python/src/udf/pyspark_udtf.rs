// TODO: This PR got too big, going to create a new one for the rest of the work

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
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
use pyo3::types::PyTuple;

use crate::cereal::pyspark_udtf::PySparkUDTF as CerealPySparkUDTF;
use crate::udf::get_python_builtins_list_function;

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
    python_function: CerealPySparkUDTF,
    #[allow(dead_code)]
    deterministic: bool,
    #[allow(dead_code)]
    eval_type: i32,
}

impl PySparkUDTF {
    pub fn new(
        function_name: String,
        input_types: Vec<DataType>,
        schema: SchemaRef,
        python_function: CerealPySparkUDTF,
        deterministic: bool,
        eval_type: i32,
    ) -> Self {
        Self {
            function_name,
            input_types,
            schema,
            python_function,
            deterministic,
            eval_type,
        }
    }

    fn apply_python_function(&self, args: &[ArrayRef]) -> Result<RecordBatch> {
        Python::with_gil(|py| {
            let builtins_list: Bound<PyAny> = get_python_builtins_list_function(py)?;
            let python_function = self
                .python_function
                .0
                .clone_ref(py)
                .into_bound(py)
                .get_item(0)
                .map_err(|err| DataFusionError::Internal(format!("python_function {}", err)))?;

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
                        DataFusionError::Execution(format!("PySpark Arrow UDF Result: {e:?}"))
                    })?;
            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark Arrow UDF Error calling list(): {:?}",
                        err
                    ))
                })?
                .get_item(0)
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "PySpark Arrow UDF Result list() first get_item(0): {:?}",
                        err
                    ))
                })?;

            let results_data: Bound<PyAny> = results.get_item(0).map_err(|err| {
                DataFusionError::Internal(format!(
                    "PySpark Arrow UDF Result list get_item(0): {:?}",
                    err
                ))
            })?;
            let _results_datatype: Bound<PyAny> = results.get_item(1).map_err(|err| {
                DataFusionError::Internal(format!(
                    "PySpark Arrow UDF Result list get_item(0): {:?}",
                    err
                ))
            })?;

            let array_data = ArrayData::from_pyarrow_bound(&results_data).map_err(|err| {
                DataFusionError::Internal(format!("PySpark Arrow UDF array_data {:?}", err))
            })?;
            let array_ref = make_array(array_data);

            let record_batch =
                RecordBatch::try_new(self.schema.clone(), vec![array_ref]).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to create RecordBatch: {:?}", e))
                })?;

            Ok(record_batch)
        })
    }
}

impl TableFunctionImpl for PySparkUDTF {
    // https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_udtf.html
    // These args can either be scalar exprs or table args that represent entire input tables.
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let mut input_types = Vec::new();
        let mut input_arrays = Vec::new();

        for expr in exprs {
            match expr {
                Expr::Literal(scalar_value) => {
                    let array_ref = scalar_value.to_array().map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to convert scalar to array: {:?}",
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

        let batches = self.apply_python_function(&input_arrays)?;

        Ok(Arc::new(PySparkUDT::new(
            self.schema.clone(),
            vec![batches],
        )))
    }
}
