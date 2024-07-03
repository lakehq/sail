use std::any::Any;

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::cereal::partial_python_udf::PartialPythonUDF;
use crate::udf::{get_udf_name, PythonFunctionType};

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,
    function_name: String,
    output_type: DataType,
    python_function: PythonFunctionType,
}

impl PythonUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
        python_function: PartialPythonUDF,
        output_type: DataType,
    ) -> Self {
        let function_name = get_udf_name(&function_name, &python_function.0);
        Self {
            signature: Signature::exact(
                input_types,
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            function_name,
            output_type,
            python_function: PythonFunctionType::PartialPythonUDF(python_function),
        }
    }
}

impl ScalarUDFImpl for PythonUDF {
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(args)?;

        let array_data: Result<ArrayData, DataFusionError> = Python::with_gil(|py| {
            let python_function: Bound<PyAny> = self.python_function.get_python_function(py)?;

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

            let results: Bound<PyAny> = python_function
                .call1(py_args)
                .map_err(|err| DataFusionError::External(err.into()))?;

            let array_data = ArrayData::from_pyarrow_bound(&results)
                .map_err(|err| DataFusionError::External(err.into()))?;
            Ok(array_data)
        });

        Ok(ColumnarValue::Array(make_array(array_data?)))
    }
}
