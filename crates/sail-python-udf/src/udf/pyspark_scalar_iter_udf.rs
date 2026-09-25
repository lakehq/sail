use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_common::{Result, plan_err};
use educe::Educe;
use pyo3::Python;
use sail_common_datafusion::udf::{StreamUDF, StreamUDFProjection};

use crate::buffer::PyPassthroughBuffer;
use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::error::PyUdfResult;
use crate::python::spark::PySpark;
use crate::stream::PyMapStream;

#[derive(Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct PySparkScalarPandasIterUDF {
    name: String,
    payload: Vec<u8>,
    #[educe(PartialOrd(ignore))]
    output_schema: SchemaRef,
    config: Arc<PySparkUdfConfig>,
}

impl PySparkScalarPandasIterUDF {
    pub fn try_new(
        name: String,
        payload: Vec<u8>,
        output_schema: SchemaRef,
        config: Arc<PySparkUdfConfig>,
    ) -> Result<Self> {
        if output_schema.fields().is_empty() {
            return plan_err!("scalar iterator UDF requires an output field");
        }
        Ok(Self {
            name,
            payload,
            output_schema,
            config,
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }
}

impl StreamUDF for PySparkScalarPandasIterUDF {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn required_input_columns(
        &self,
        output_columns: &[usize],
        input_columns: usize,
    ) -> Option<Vec<usize>> {
        let passthrough_columns = self.output_schema.fields().len() - 1;
        Some(
            (0..input_columns)
                .filter(|i| *i >= passthrough_columns || output_columns.contains(i))
                .collect(),
        )
    }

    fn project_input(&self, input_columns: &[usize]) -> Result<Option<StreamUDFProjection>> {
        let passthrough_columns = self.output_schema.fields().len() - 1;
        let output_columns = input_columns
            .iter()
            .copied()
            .filter(|i| *i < passthrough_columns)
            .chain(std::iter::once(passthrough_columns))
            .collect::<Vec<_>>();
        Ok(Some(StreamUDFProjection {
            udf: Arc::new(Self::try_new(
                self.name.clone(),
                self.payload.clone(),
                Arc::new(self.output_schema.project(&output_columns)?),
                Arc::clone(&self.config),
            )?),
            output_columns,
        }))
    }

    fn invoke(
        &self,
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let passthrough_columns = self.output_schema.fields().len() - 1;
        let output_name = self.output_schema.field(passthrough_columns).name();
        let function = Python::attach(|py| -> PyUdfResult<_> {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            Ok(PySpark::scalar_pandas_iter_udf(
                py,
                udf,
                passthrough_columns,
                output_name,
                &self.config,
                PyPassthroughBuffer::new(context),
            )?
            .unbind())
        })?;
        Ok(Box::pin(PyMapStream::new(
            input,
            function,
            Arc::clone(&self.output_schema),
            self.config.arrow_use_large_var_types,
        )))
    }
}
