use std::any::Any;
use std::sync::OnceLock;

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::Result;
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::prelude::{PyAnyMethods, PyTypeMethods};
use pyo3::types::{PyIterator, PyList, PyTuple};
use pyo3::{Bound, PyAny, Python};
use sail_common::spec;

use crate::cereal::pyspark_udf::PySparkUdfObject;
use crate::cereal::PythonFunction;
use crate::error::PyUdfResult;
use crate::udf::get_udf_name;
use crate::utils::builtins::PyBuiltins;
use crate::utils::pyarrow::{
    to_pyarrow_data_type, PyArrow, PyArrowArray, PyArrowArrayOptions, PyArrowToPandasOptions,
};

#[derive(Debug)]
pub struct PySparkUDF {
    signature: Signature,
    function_name: String,
    // TODO: We should not keep spec information in the UDF.
    //   We should define different UDFs for different eval types
    //   and let the plan resolver create the correct UDF instance.
    deterministic: bool,
    eval_type: spec::PySparkUdfType,
    input_types: Vec<DataType>,
    output_type: DataType,
    python_bytes: Vec<u8>,
    python_function: OnceLock<Result<PySparkUdfObject>>,
}

impl PySparkUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        eval_type: spec::PySparkUdfType,
        input_types: Vec<DataType>,
        output_type: DataType,
        python_bytes: Vec<u8>,
        construct_udf_name: bool,
    ) -> Self {
        let function_name = if construct_udf_name {
            get_udf_name(&function_name, &python_bytes)
        } else {
            function_name
        };
        Self {
            signature: Signature::exact(
                input_types.clone(),
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            function_name,
            deterministic,
            input_types,
            output_type,
            eval_type,
            python_bytes,
            python_function: OnceLock::new(),
        }
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn deterministic(&self) -> bool {
        self.deterministic
    }

    pub fn eval_type(&self) -> &spec::PySparkUdfType {
        &self.eval_type
    }

    pub fn input_types(&self) -> &[DataType] {
        &self.input_types
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub fn python_bytes(&self) -> &[u8] {
        &self.python_bytes
    }

    pub fn python_function(&self) -> Result<&PySparkUdfObject> {
        self.python_function
            .get_or_init(|| -> Result<PySparkUdfObject> {
                Ok(PySparkUdfObject::load(&self.python_bytes)?)
            })
            .as_ref()
            .map_err(|e| DataFusionError::Internal(format!("Python function error: {e}")))
    }
}

fn call_arrow_udf(
    py: Python,
    function: &PySparkUdfObject,
    args: Vec<ArrayRef>,
    output_type: &DataType,
) -> PyUdfResult<ArrayData> {
    let udf = function.function(py)?;
    let pyarrow_array = PyArrow::array(
        py,
        PyArrowArrayOptions {
            r#type: Some(to_pyarrow_data_type(py, output_type)?),
            from_pandas: Some(true),
        },
    )?;
    let pyarrow_array_to_pandas = PyArrowArray::to_pandas(
        py,
        PyArrowToPandasOptions {
            use_pandas_nullable_types: true,
        },
    )?;

    let py_args = args
        .iter()
        .map(|arg| {
            let arg = arg.into_data().to_pyarrow(py)?.clone_ref(py).into_bound(py);
            let arg = pyarrow_array_to_pandas.call1((arg,))?;
            Ok(arg)
        })
        .collect::<PyUdfResult<Vec<_>>>()?;
    let py_args = PyTuple::new_bound(py, &py_args);

    let result = udf.call1((py.None(), (py_args,)))?;
    let result = PyBuiltins::list(py)?.call1((result,))?.get_item(0)?;

    let data = result.get_item(0)?;
    let _data_type = result.get_item(1)?;
    let data = pyarrow_array.call1((data,))?;

    Ok(ArrayData::from_pyarrow_bound(&data)?)
}

fn call_pandas_udf(
    py: Python,
    function: &PySparkUdfObject,
    args: Vec<ArrayRef>,
    output_type: &DataType,
) -> PyUdfResult<ArrayData> {
    let udf = function.function(py)?;
    let pyarrow_array = PyArrow::array(
        py,
        PyArrowArrayOptions {
            r#type: Some(to_pyarrow_data_type(py, output_type)?),
            from_pandas: Some(true),
        },
    )?;
    let pyarrow_array_to_pandas = PyArrowArray::to_pandas(
        py,
        PyArrowToPandasOptions {
            use_pandas_nullable_types: true,
        },
    )?;

    let py_args = args
        .iter()
        .map(|arg| {
            let arg = arg.into_data().to_pyarrow(py)?.clone_ref(py).into_bound(py);
            let arg = pyarrow_array_to_pandas.call1((arg,))?;
            Ok(arg)
        })
        .collect::<PyUdfResult<Vec<_>>>()?;
    let py_args = PyTuple::new_bound(py, &py_args);

    let result = udf.call1((py.None(), (py_args,)))?;
    let result = PyBuiltins::list(py)?.call1((result,))?.get_item(0)?;

    let data = result.get_item(0)?;
    let _data_type = result.get_item(1)?;
    let data = pyarrow_array.call1((data,))?;

    Ok(ArrayData::from_pyarrow_bound(&data)?)
}

fn call_udf(
    py: Python,
    function: &PySparkUdfObject,
    args: Vec<ArrayRef>,
    eval_type: spec::PySparkUdfType,
    deterministic: bool,
    output_type: &DataType,
) -> PyUdfResult<ArrayData> {
    let udf = function.function(py)?;
    let py_list = PyBuiltins::list(py)?;
    let py_str = PyBuiltins::str(py)?;
    let pyarrow_array = PyArrow::array(
        py,
        PyArrowArrayOptions {
            r#type: Some(to_pyarrow_data_type(py, output_type)?),
            from_pandas: Some(false),
        },
    )?;
    let pyarrow_array_to_pylist = PyArrowArray::to_pylist(py)?;

    let py_args_columns_list = args
        .iter()
        .map(|arg| {
            let arg = arg.into_data().to_pyarrow(py)?.clone_ref(py).into_bound(py);
            let arg = pyarrow_array_to_pylist.call1((arg,))?;
            Ok(arg)
        })
        .collect::<PyUdfResult<Vec<_>>>()?;
    let py_args_tuple = PyTuple::new_bound(py, &py_args_columns_list);
    // TODO: Do zip in Rust for performance.
    let py_args_zip = py.eval_bound("zip", None, None)?.call1(&py_args_tuple)?;
    let py_args = PyIterator::from_bound_object(&py_args_zip)?;

    let mut already_str: bool = false;
    let results = py_args
        .map(|py_arg| -> PyUdfResult<Bound<PyAny>> {
            let py_arg = py_arg?;
            let result = udf.call1((py.None(), (py_arg,)))?;
            let result = py_list.call1((result,))?.get_item(0)?;

            if matches!(eval_type, spec::PySparkUdfType::Batched)
                && deterministic
                && *output_type == DataType::Utf8
            {
                if already_str {
                    return Ok(result);
                }
                let result_type = result.get_type();
                let result_data_type_name = result_type.name()?;
                if result_data_type_name != "str" {
                    let result = py_str.call1((result,))?;
                    return Ok(result);
                } else {
                    already_str = true;
                }
            }
            Ok(result)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let results = pyarrow_array.call1((results,))?;
    Ok(ArrayData::from_pyarrow_bound(&results)?)
}

fn call_udf_no_args(
    py: Python,
    function: &PySparkUdfObject,
    eval_type: spec::PySparkUdfType,
    deterministic: bool,
    output_type: &DataType,
) -> PyUdfResult<ArrayData> {
    let udf = function.function(py)?;
    let py_list = PyBuiltins::list(py)?;
    let py_str = PyBuiltins::str(py)?;
    let pyarrow_array = PyArrow::array(
        py,
        PyArrowArrayOptions {
            r#type: Some(to_pyarrow_data_type(py, output_type)?),
            from_pandas: Some(false),
        },
    )?;

    let result = udf.call1((py.None(), (PyList::empty_bound(py),)))?;
    let result = py_list.call1((result,))?.get_item(0)?;
    let result_type = result.get_type();
    let result_data_type_name = result_type.name()?;

    let result = if matches!(eval_type, spec::PySparkUdfType::Batched)
        && deterministic
        && *output_type == DataType::Utf8
        && result_data_type_name != "str"
    {
        py_str.call1((result,))?
    } else {
        result
    };

    let result = pyarrow_array.call1(([result],))?;
    Ok(ArrayData::from_pyarrow_bound(&result)?)
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // We intentionally call self.python_function() in invoke() instead of in the constructor.
        // This is because the Sail Driver may serialize the UDF in `try_encode_udf`.
        let python_function = self.python_function()?;
        let args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(args)?;

        let array_data = if self.eval_type.is_arrow_udf() {
            Python::with_gil(|py| call_arrow_udf(py, python_function, args, &self.output_type))?
        } else if self.eval_type.is_pandas_udf() {
            Python::with_gil(|py| call_pandas_udf(py, python_function, args, &self.output_type))?
        } else {
            Python::with_gil(|py| {
                call_udf(
                    py,
                    python_function,
                    args,
                    self.eval_type,
                    self.deterministic,
                    &self.output_type,
                )
            })?
        };
        Ok(ColumnarValue::Array(make_array(array_data)))
    }

    fn invoke_no_args(&self, _number_rows: usize) -> Result<ColumnarValue> {
        let python_function = self.python_function()?;
        let array_data = Python::with_gil(|py| {
            call_udf_no_args(
                py,
                python_function,
                self.eval_type,
                self.deterministic,
                &self.output_type,
            )
        })?;
        Ok(ColumnarValue::Array(make_array(array_data)))
    }
}
