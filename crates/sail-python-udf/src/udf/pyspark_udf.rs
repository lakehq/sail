use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::{PyObject, Python};

use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::PyUdfResult;
use crate::lazy::LazyPyObject;
use crate::python::spark::PySpark;

#[derive(Debug, Clone, Copy)]
pub enum PySparkUdfKind {
    Batch,
    ArrowBatch,
    ScalarPandas,
    ScalarPandasIter,
}

#[derive(Debug)]
pub struct PySparkUDF {
    signature: Signature,
    kind: PySparkUdfKind,
    name: String,
    payload: Vec<u8>,
    deterministic: bool,
    input_types: Vec<DataType>,
    output_type: DataType,
    config: Arc<PySparkUdfConfig>,
    udf: LazyPyObject,
}

impl PySparkUDF {
    pub fn new(
        kind: PySparkUdfKind,
        name: String,
        payload: Vec<u8>,
        deterministic: bool,
        input_types: Vec<DataType>,
        output_type: DataType,
        config: Arc<PySparkUdfConfig>,
    ) -> Self {
        Self {
            signature: Signature::exact(
                input_types.clone(),
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            kind,
            name,
            payload,
            deterministic,
            input_types,
            output_type,
            config,
            udf: LazyPyObject::new(),
        }
    }

    pub fn kind(&self) -> PySparkUdfKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn deterministic(&self) -> bool {
        self.deterministic
    }

    pub fn input_types(&self) -> &[DataType] {
        &self.input_types
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }

    fn udf(&self, py: Python) -> Result<PyObject> {
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            let udf = match self.kind {
                PySparkUdfKind::Batch => {
                    PySpark::batch_udf(py, udf, &self.input_types, &self.output_type, &self.config)?
                }
                PySparkUdfKind::ArrowBatch => PySpark::arrow_batch_udf(py, udf, &self.config)?,
                PySparkUdfKind::ScalarPandas => PySpark::scalar_pandas_udf(py, udf, &self.config)?,
                PySparkUdfKind::ScalarPandasIter => {
                    PySpark::scalar_pandas_iter_udf(py, udf, &self.config)?
                }
            };
            Ok(udf.unbind())
        })?;
        Ok(udf.clone_ref(py))
    }
}

impl ScalarUDFImpl for PySparkUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(args)?;
        let udf = Python::with_gil(|py| self.udf(py))?;
        let data = Python::with_gil(|py| -> PyUdfResult<_> {
            let output = udf.call1(py, (args.try_to_py(py)?, number_rows))?;
            Ok(ArrayData::try_from_py(py, &output)?)
        })?;
        let array = cast(&make_array(data), &self.output_type)?;
        Ok(ColumnarValue::Array(array))
    }
}
