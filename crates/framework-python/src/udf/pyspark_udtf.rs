// TODO: This PR got too big, going to create a new one for the rest of the work

use crate::cereal::partial_pyspark_udf::PartialPySparkUDF;
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use std::sync::Arc;

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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
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
    function_name: String,
    input_types: Vec<DataType>,
    schema: SchemaRef,
    python_function: PartialPySparkUDF,
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
        python_function: PartialPySparkUDF,
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

    // fn apply_python_function(&self, input_arrays: Vec<ArrayRef>) -> Result<RecordBatch> {
    //     Python::with_gil(|py| {
    //     })
    // }
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

        let schema = Schema::new(
            input_types
                .iter()
                .enumerate()
                .map(|(i, data_type)| {
                    // Assuming the columns are named as "col0", "col1", etc.
                    Field::new(&format!("col{}", i), data_type.clone(), true)
                })
                .collect::<Vec<_>>(),
        );
        let schema_ref = Arc::new(schema);

        let record_batch = RecordBatch::try_new(schema_ref.clone(), input_arrays).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create RecordBatch: {:?}", e))
        })?;

        // let transformed_batch = self.apply_python_function(input_arrays)?;

        Ok(Arc::new(PySparkUDT::new(schema_ref, vec![record_batch])))
    }
}
