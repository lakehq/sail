use arrow::array::types;
use std::any::Any;

use crate::partial_python_udf::PartialPythonUDF;
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

use crate::pyarrow::{FromPyArrow, ToPyArrow};

use crate::utils::{array_ref_to_columnar_value, downcast_array_ref, execute_python_function};

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,
    // TODO: See what we exactly need from below fields.
    function_name: String,
    output_type: DataType,
    #[allow(dead_code)]
    eval_type: i32,
    python_function: PartialPythonUDF,
}

impl PythonUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
        python_function: PartialPythonUDF,
        output_type: DataType,
        eval_type: i32, // TODO: Incorporate this
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
            eval_type,
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

        let processed_array = Python::with_gil(|py| {
            let python_function = self
                .python_function
                .0
                .clone_ref(py)
                .into_bound(py)
                .get_item(0)
                .unwrap();

            let py_args = array_ref
                .into_data()
                .to_pyarrow(py)
                .unwrap()
                .call_method0(py, pyo3::intern!(py, "to_pylist"))
                .unwrap()
                .clone_ref(py)
                .into_bound(py)
                .get_item(0) // TODO: Build array out of output data type.
                .unwrap();
            let py_args = PyTuple::new_bound(py, &[py_args]);

            let result = python_function
                .call1(py_args)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
                .unwrap();

            let kwargs = PyDict::new_bound(py);
            kwargs
                .set_item("type", self.output_type.to_pyarrow(py).unwrap())
                .unwrap();

            let result = PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
                .and_then(|pyarrow| pyarrow.getattr(pyo3::intern!(py, "array")))
                .and_then(|array| array.call(([result],), Some(&kwargs)))
                .unwrap();

            // TODO: This is now native python object so the line below breaks.
            let array_data = ArrayData::from_pyarrow_bound(&result).unwrap();
            make_array(array_data)
        });

        // let array = downcast_array_ref::<types::Int32Type>(&processed_array)?;
        // Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
        //     array.value(0),
        // ))))

        Ok(array_ref_to_columnar_value(
            processed_array,
            &self.output_type,
            is_scalar,
        )?)
    }
}
