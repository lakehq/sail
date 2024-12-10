use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::{PyObject, Python};

use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::PyUdfResult;
use crate::lazy::LazyPyObject;
use crate::utils::spark::PySpark;

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
    function_name: String,
    function: Vec<u8>,
    deterministic: bool,
    input_types: Vec<DataType>,
    output_type: DataType,
    udf: LazyPyObject,
}

impl PySparkUDF {
    pub fn new(
        kind: PySparkUdfKind,
        function_name: String,
        function: Vec<u8>,
        deterministic: bool,
        input_types: Vec<DataType>,
        output_type: DataType,
    ) -> Self {
        Self {
            signature: Signature::exact(
                input_types.clone(),
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            kind,
            function_name,
            function,
            deterministic,
            input_types,
            output_type,
            udf: LazyPyObject::new(),
        }
    }

    pub fn kind(&self) -> PySparkUdfKind {
        self.kind
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn function(&self) -> &[u8] {
        &self.function
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

    fn udf(&self, py: Python) -> PyUdfResult<PyObject> {
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.function)?;
            let wrapper = match self.kind {
                PySparkUdfKind::Batch => {
                    PySpark::batch_udf(py, udf, &self.input_types, &self.output_type)?
                }
                PySparkUdfKind::ArrowBatch => {
                    PySpark::arrow_batch_udf(py, udf, &self.input_types, &self.output_type)?
                }
                PySparkUdfKind::ScalarPandas => {
                    PySpark::scalar_pandas_udf(py, udf, &self.input_types, &self.output_type)?
                }
                PySparkUdfKind::ScalarPandasIter => {
                    PySpark::scalar_pandas_iter_udf(py, udf, &self.input_types, &self.output_type)?
                }
            };
            Ok(wrapper.unbind())
        })?;
        Ok(udf.clone_ref(py))
    }
}

impl ScalarUDFImpl for PySparkUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.function_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(args)?;
        let output = Python::with_gil(|py| -> PyUdfResult<_> {
            let output = self
                .udf(py)?
                .call1(py, (args.try_to_py(py)?, number_rows))?;
            Ok(ArrayData::try_from_py(py, &output)?)
        })?;
        Ok(ColumnarValue::Array(make_array(output)))
    }
}
