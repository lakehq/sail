use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::exec_err;
use datafusion_expr::Expr;
use pyo3::Python;
use sail_common::udf::StreamUDF;

use crate::cereal::pyspark_udtf::PySparkUdtfPayload;
use crate::error::PyUdfResult;
use crate::stream::PyMapStream;
use crate::utils::spark::PySpark;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum PySparkUdtfKind {
    Table,
    ArrowTable,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PySparkUDTF {
    kind: PySparkUdtfKind,
    name: String,
    payload: Vec<u8>,
    /// The data types of UDTF arguments.
    /// The UDTF accepts input batch which contains columns of the input execution plan,
    /// followed by columns corresponding to the UDTF arguments.
    /// The output batch contains columns of the input execution plan, followed by columns
    /// corresponding to the UDTF output.
    argument_types: Vec<DataType>,
    /// The output schema of the UDTF output stream, consisting of the columns of the input
    /// execution plan, followed by the columns of the UDTF output.
    output_schema: SchemaRef,
    deterministic: bool,
}

#[derive(PartialEq, PartialOrd)]
struct PySparkUDTFOrd<'a> {
    kind: PySparkUdtfKind,
    name: &'a str,
    payload: &'a [u8],
    argument_types: &'a [DataType],
    deterministic: &'a bool,
}

impl<'a> From<&'a PySparkUDTF> for PySparkUDTFOrd<'a> {
    fn from(udtf: &'a PySparkUDTF) -> Self {
        Self {
            kind: udtf.kind,
            name: &udtf.name,
            payload: &udtf.payload,
            argument_types: &udtf.argument_types,
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
    pub fn new(
        kind: PySparkUdtfKind,
        name: String,
        payload: Vec<u8>,
        argument_types: Vec<DataType>,
        output_schema: SchemaRef,
        deterministic: bool,
    ) -> Self {
        Self {
            kind,
            name,
            payload,
            argument_types,
            output_schema,
            deterministic,
        }
    }
}

impl TableFunctionImpl for PySparkUDTF {
    fn call(&self, _: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        exec_err!("Python UDTF should not be called as a table function")
    }
}

impl StreamUDF for PySparkUDTF {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn invoke(&self, input: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        let function = Python::with_gil(|py| -> PyUdfResult<_> {
            let udtf = PySparkUdtfPayload::load(py, &self.payload)?;
            let udtf = match self.kind {
                PySparkUdtfKind::Table => {
                    PySpark::table_udf(py, udtf, &self.argument_types, &self.output_schema)?
                }
                PySparkUdtfKind::ArrowTable => PySpark::arrow_table_udf(py, udtf)?,
            };
            Ok(udtf.unbind())
        })?;
        Ok(Box::pin(PyMapStream::new(
            input,
            function,
            self.output_schema.clone(),
        )))
    }
}
