use std::cmp::Ordering;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::Result;
use pyo3::Python;
use sail_common::udf::MapIterUDF;

use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::error::PyUdfResult;
use crate::stream::PyOutputStream;
use crate::utils::spark::PySpark;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum PySparkMapIterKind {
    Pandas,
    Arrow,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PySparkMapIterUDF {
    kind: PySparkMapIterKind,
    name: String,
    payload: Vec<u8>,
    output_schema: SchemaRef,
}

impl PySparkMapIterUDF {
    pub fn new(
        kind: PySparkMapIterKind,
        name: String,
        payload: Vec<u8>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            kind,
            name,
            payload,
            output_schema,
        }
    }

    pub fn kind(&self) -> PySparkMapIterKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(PartialEq, PartialOrd)]
struct PySparkMapIterUDFOrd<'a> {
    kind: PySparkMapIterKind,
    name: &'a String,
    payload: &'a Vec<u8>,
}

impl<'a> From<&'a PySparkMapIterUDF> for PySparkMapIterUDFOrd<'a> {
    fn from(udf: &'a PySparkMapIterUDF) -> Self {
        Self {
            kind: udf.kind,
            name: &udf.name,
            payload: &udf.payload,
        }
    }
}

impl PartialOrd for PySparkMapIterUDF {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PySparkMapIterUDFOrd::from(self).partial_cmp(&other.into())
    }
}

impl MapIterUDF for PySparkMapIterUDF {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn invoke(&self, input: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        let function = Python::with_gil(|py| -> PyUdfResult<_> {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            let udf = match self.kind {
                PySparkMapIterKind::Pandas => {
                    PySpark::map_pandas_iter_udf(py, udf, self.output_schema.clone())?
                }
                PySparkMapIterKind::Arrow => PySpark::map_arrow_iter_udf(py, udf)?,
            };
            Ok(udf.unbind())
        })?;
        Ok(Box::pin(PyOutputStream::new(
            input,
            function,
            self.output_schema.clone(),
        )))
    }
}
