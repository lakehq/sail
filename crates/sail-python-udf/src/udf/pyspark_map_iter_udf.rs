use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::Result;
use pyo3::Python;
use sail_common_datafusion::udf::StreamUDF;
use sail_common_datafusion::utils::rename_record_batch_stream;

use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::error::PyUdfResult;
use crate::python::spark::PySpark;
use crate::stream::PyMapStream;

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
    input_names: Vec<String>,
    output_schema: SchemaRef,
    config: Arc<PySparkUdfConfig>,
}

impl PySparkMapIterUDF {
    pub fn new(
        kind: PySparkMapIterKind,
        name: String,
        payload: Vec<u8>,
        input_names: Vec<String>,
        output_schema: SchemaRef,
        config: Arc<PySparkUdfConfig>,
    ) -> Self {
        Self {
            kind,
            name,
            payload,
            input_names,
            output_schema,
            config,
        }
    }

    pub fn kind(&self) -> PySparkMapIterKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn input_names(&self) -> &[String] {
        &self.input_names
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }
}

#[derive(PartialEq, PartialOrd)]
struct PySparkMapIterUDFOrd<'a> {
    kind: PySparkMapIterKind,
    name: &'a String,
    payload: &'a Vec<u8>,
    input_names: &'a [String],
    config: &'a PySparkUdfConfig,
}

impl<'a> From<&'a PySparkMapIterUDF> for PySparkMapIterUDFOrd<'a> {
    fn from(udf: &'a PySparkMapIterUDF) -> Self {
        Self {
            kind: udf.kind,
            name: &udf.name,
            payload: &udf.payload,
            input_names: &udf.input_names,
            config: &udf.config,
        }
    }
}

impl PartialOrd for PySparkMapIterUDF {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PySparkMapIterUDFOrd::from(self).partial_cmp(&other.into())
    }
}

impl StreamUDF for PySparkMapIterUDF {
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
                PySparkMapIterKind::Pandas => PySpark::map_pandas_iter_udf(py, udf, &self.config)?,
                PySparkMapIterKind::Arrow => PySpark::map_arrow_iter_udf(py, udf, &self.config)?,
            };
            Ok(udf.unbind())
        })?;
        Ok(Box::pin(PyMapStream::new(
            rename_record_batch_stream(input, &self.input_names)?,
            function,
            self.output_schema.clone(),
        )))
    }
}
