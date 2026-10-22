use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayData, ArrayRef, make_array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use pyo3::{Py, PyAny, Python};

use crate::cereal::pyspark_udf::PySparkUdfPayload;
use crate::config::PySparkUdfConfig;
use crate::conversion::{TryFromPy, TryToPy};
use crate::error::{PyUdfError, PyUdfResult};
use crate::lazy::LazyPyObject;
use crate::python::spark::PySpark;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PySparkUdfKind {
    Batch,
    ArrowBatch,
    ScalarPandas,
    // Extracted into partition stream execution during analysis.
    ScalarPandasIter,
    // Spark 4.0 Arrow-native scalar UDF types
    ScalarArrow,
    ScalarArrowIter,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PySparkUDF {
    signature: Signature,
    kind: PySparkUdfKind,
    name: String,
    payload: Vec<u8>,
    deterministic: bool,
    input_types: Vec<DataType>,
    output_type: DataType,
    /// The metadata of the output field, such as the user-defined type of the output.
    output_metadata: BTreeMap<String, String>,
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
            output_metadata: BTreeMap::new(),
            config,
            udf: LazyPyObject::new(),
        }
    }

    pub fn with_output_metadata(mut self, output_metadata: BTreeMap<String, String>) -> Self {
        self.output_metadata = output_metadata;
        self
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

    pub fn output_metadata(&self) -> &BTreeMap<String, String> {
        &self.output_metadata
    }

    pub fn config(&self) -> &Arc<PySparkUdfConfig> {
        &self.config
    }

    /// Returns the Python function, which is created with the input fields of the first call,
    /// since the fields have the user-defined types of the arguments that the function receives.
    fn udf(&self, py: Python, input_fields: &[FieldRef]) -> Result<Py<PyAny>> {
        let udf = self.udf.get_or_try_init(py, || {
            let udf = PySparkUdfPayload::load(py, &self.payload)?;
            let udf = match self.kind {
                PySparkUdfKind::Batch => {
                    PySpark::batch_udf(py, udf, input_fields, &self.output_type, &self.config)?
                }
                PySparkUdfKind::ArrowBatch => PySpark::arrow_batch_udf(py, udf, &self.config)?,
                PySparkUdfKind::ScalarPandas => PySpark::scalar_pandas_udf(py, udf, &self.config)?,
                PySparkUdfKind::ScalarPandasIter => {
                    return Err(PyUdfError::internal(
                        "scalar iterator UDF requires partition stream execution",
                    ));
                }
                // Arrow-native: no Pandas conversion, pass Arrow arrays directly
                PySparkUdfKind::ScalarArrow => PySpark::scalar_arrow_udf(py, udf, &self.config)?,
                PySparkUdfKind::ScalarArrowIter => {
                    PySpark::scalar_arrow_iter_udf(py, udf, &self.config)?
                }
            };
            Ok(udf.unbind())
        })?;
        Ok(udf.clone_ref(py))
    }
}

impl ScalarUDFImpl for PySparkUDF {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            Field::new(self.name(), self.output_type.clone(), true).with_metadata(
                self.output_metadata
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            ..
        } = args;
        let args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(&args)?;
        let udf = Python::attach(|py| self.udf(py, &arg_fields))?;
        let data = Python::attach(|py| -> PyUdfResult<_> {
            let output = udf.call1(
                py,
                (
                    args.try_to_py(py, self.config.arrow_use_large_var_types)?,
                    number_rows,
                ),
            )?;
            Ok(ArrayData::try_from_py(py, &output)?)
        })?;
        let array = cast(&make_array(data), &self.output_type)?;
        Ok(ColumnarValue::Array(array))
    }
}
