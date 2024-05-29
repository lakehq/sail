use crate::partial_python_udf::PartialPythonUDF;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, Expr, TableType};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PythonUDT {
    output_schema: SchemaRef,
    // limit: Option<usize>,
    // batches: Vec<RecordBatch>,
}

impl PythonUDT {
    pub fn new(output_schema: SchemaRef) -> Self {
        Self { output_schema }
    }
}

// #[async_trait]
// impl TableProvider for PythonUDT {
//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }
//
//     fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }
//
//     fn table_type(&self) -> TableType {
//         TableType::Base
//     }
//
//     async fn scan(
//         &self,
//         state: &SessionState,
//         projection: Option<&Vec<usize>>,
//         filters: &[Expr],
//         limit: Option<usize>,
//     ) -> Result<Arc<dyn ExecutionPlan>> {
//     }
// }

#[derive(Debug, Clone)]
pub struct PythonUDTF {
    function_name: String,
    input_types: Vec<DataType>,
    output_schema: SchemaRef,
    python_function: PartialPythonUDF,
    #[allow(dead_code)]
    deterministic: bool,
    #[allow(dead_code)]
    eval_type: i32,
}

impl PythonUDTF {
    pub fn new(
        function_name: String,
        input_types: Vec<DataType>,
        output_schema: SchemaRef,
        python_function: PartialPythonUDF,
        deterministic: bool,
        eval_type: i32,
    ) -> Self {
        Self {
            function_name,
            input_types,
            output_schema,
            python_function,
            deterministic,
            eval_type,
        }
    }
}

// impl TableFunctionImpl for PythonUDTF {
//     // https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_udtf.html
//     // These args can either be scalar exprs or table args that represent entire input tables.
//     fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
//         let mut input_types = Vec::new();
//         let mut input_arrays = Vec::new();
//
//         for expr in exprs {
//             match expr {
//                 Expr::Literal(scalar_value) => {
//                     let array_ref = scalar_value.to_array().map_err(|e| {
//                         DataFusionError::Execution(format!(
//                             "Failed to convert scalar to array: {:?}",
//                             e
//                         ))
//                     })?;
//                     input_types.push(array_ref.data_type().clone());
//                     input_arrays.push(array_ref);
//                 }
//                 _ => {
//                     // TODO: Support table args
//                     return Err(DataFusionError::NotImplemented(
//                         "Only literal expressions are supported in Python UDTFs for now"
//                             .to_string(),
//                     ));
//                 }
//             }
//         }
//
//         Ok(Arc::new(PythonUDT::new(self.output_schema.clone())))
//     }
// }
