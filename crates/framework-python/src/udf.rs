use std::any::Any;

use crate::partial_python_udf::PartialPythonUDF;
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::types::PyBytes;
use pyo3::{
    prelude::*,
    types::{PyDict, PyIterator, PyTuple},
};

use crate::pyarrow::{FromPyArrow, ToPyArrow};

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,
    function_name: String,
    output_type: DataType,
    python_function: PartialPythonUDF,
}

impl PythonUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
        python_function: PartialPythonUDF,
        output_type: DataType,
    ) -> Self {
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
            python_function,
            output_type,
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
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{:?} should only be called with a single argument",
                self.name()
            )));
        }

        let (array_ref, is_scalar) = match &args[0] {
            ColumnarValue::Array(arr) => (arr.clone(), false),
            ColumnarValue::Scalar(scalar) => {
                let arr = scalar.to_array().map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to convert scalar to array: {:?}",
                        e
                    ))
                })?;
                (arr, true)
            }
        };

        let array_len = array_ref.len().clone();

        let processed_array: Result<ArrayRef, DataFusionError> = Python::with_gil(|py| {
            let result_tuple = self.python_function.0.clone_ref(py).into_bound(py);

            let python_function = result_tuple
                .get_item(0)
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;
            let deserializer = result_tuple
                .get_item(2)
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;
            let serializer = result_tuple
                .get_item(3)
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;

            let py_args = array_ref
                .into_data()
                .to_pyarrow(py)
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?
                .call_method0(py, pyo3::intern!(py, "to_pylist"))
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?
                .clone_ref(py)
                .into_bound(py);
            println!("CHECK HERE py_args: {:?}", py_args);
            //
            // let py_iter = PyIterator::from_bound_object(&py_args)
            //     .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;
            let split_index = 0;
            let input_iterator = deserializer
                .call_method1("load_stream", (py_args,))
                .map_err(|err| DataFusionError::Internal(format!("meow1 {:?}", err)))?;
            let output_iterator = python_function
                .call1((split_index, input_iterator))
                .map_err(|e| DataFusionError::Execution(format!("meow2 {:?}", e)))?;
            let outfile = PyModule::import_bound(py, pyo3::intern!(py, "io"))
                .and_then(|io| io.getattr(pyo3::intern!(py, "BytesIO")))
                .and_then(|bytes_io| bytes_io.call0())
                .map_err(|e| DataFusionError::Internal(format!("meow3 {:?}", e)))?;
            let results = serializer
                .call_method1("dump_stream", (output_iterator, outfile))
                .map_err(|err| DataFusionError::Internal(format!("meow4 {:?}", err)))?;
            //
            // let results = python_function
            //     .call1((py_iter,))
            //     .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

            let pyarrow_output_type = self
                .output_type
                .to_pyarrow(py)
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;

            let kwargs: Bound<PyDict> = PyDict::new_bound(py);
            kwargs
                .set_item("type", pyarrow_output_type)
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;

            let result: Bound<PyAny> = PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
                .and_then(|pyarrow| pyarrow.getattr(pyo3::intern!(py, "array")))
                .and_then(|array| array.call((results,), Some(&kwargs)))
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;

            let array_data = ArrayData::from_pyarrow_bound(&result)
                .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?;

            let array = make_array(array_data);
            Ok(array)
        });

        Ok(ColumnarValue::Array(processed_array?))
    }
}
