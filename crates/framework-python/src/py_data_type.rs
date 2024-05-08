use crate::impl_py_state_serialization;
use crate::utils::process_array_ref_with_python_function;
use datafusion::arrow::array::{as_struct_array, types, ArrayRef, PrimitiveArray, StructArray, *};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion_common::DataFusionError;
use pyo3::types::PyTuple;
use pyo3::{
    prelude::{pyclass, pymethods, IntoPy, PyAnyMethods, PyObject, PyResult, Python, ToPyObject},
    types::{PyBytes, PyDict, PyList},
    Bound, PyTypeInfo,
};
use serde::{Deserialize, Serialize};
use serde_bytes::Bytes;

#[pyclass(module = "sail.types")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PyDataType {
    pub data_type: DataType,
}

impl From<DataType> for PyDataType {
    fn from(value: DataType) -> Self {
        PyDataType { data_type: value }
    }
}

impl From<PyDataType> for DataType {
    fn from(item: PyDataType) -> Self {
        item.data_type
    }
}

impl_py_state_serialization!(PyDataType);

macro_rules! to_pyobject {
    ($array_type: ty, $py:expr, $array:expr, $i:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        array.value($i).into_py($py)
    }};
}

pub fn to_pyobject(py: Python<'_>, array: &dyn Array, i: usize) -> PyResult<PyObject> {
    if array.is_null(i) {
        return Ok(py.None());
    }
    Ok(match array.data_type() {
        DataType::Null => py.None(),
        DataType::Boolean => to_pyobject!(BooleanArray, py, array, i),
        DataType::Int8 => to_pyobject!(Int8Array, py, array, i),
        DataType::Int16 => to_pyobject!(Int16Array, py, array, i),
        DataType::Int32 => to_pyobject!(Int32Array, py, array, i),
        DataType::Int64 => to_pyobject!(Int64Array, py, array, i),
        DataType::UInt8 => to_pyobject!(UInt8Array, py, array, i),
        DataType::UInt16 => to_pyobject!(UInt16Array, py, array, i),
        DataType::UInt32 => to_pyobject!(UInt32Array, py, array, i),
        DataType::UInt64 => to_pyobject!(UInt64Array, py, array, i),
        DataType::Float32 => to_pyobject!(Float32Array, py, array, i),
        DataType::Float64 => to_pyobject!(Float64Array, py, array, i),
        DataType::Utf8 => to_pyobject!(StringArray, py, array, i),
        DataType::LargeUtf8 => to_pyobject!(LargeStringArray, py, array, i),
        DataType::Binary => to_pyobject!(BinaryArray, py, array, i),
        DataType::LargeBinary => to_pyobject!(LargeBinaryArray, py, array, i),
        DataType::List(_field) => {
            let array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let list = array.value(i);
            let mut values = Vec::with_capacity(list.len());
            for j in 0..list.len() {
                values.push(to_pyobject(py, list.as_ref(), j)?);
            }
            values.into_py(py)
        }
        DataType::Struct(fields) => {
            println!("CHECK HERE FIELDS: {:?}", fields);
            let array = as_struct_array(array);
            let object = py.eval_bound("Struct()", None, None)?;
            // let pydict = PyDict::new_bound(py);
            for (j, field) in fields.iter().enumerate() {
                let value = to_pyobject(py, array.column(j).as_ref(), i)?;
                object.setattr(field.name().as_str(), value)?;
                // pydict.set_item(field.name().as_str(), value.clone())?;
                // pydict.setattr(field.name().as_str(), value.clone())?;
            }
            // pydict.into()
            object.into()
        }
        _ => todo!(),
    })
}
//
// pub fn execute_python_function_test(
//     array_ref: &ArrayRef,
//     python_function: &PyObject,
//     output_type: &DataType,
// ) -> Result<ArrayRef, DataFusionError> {
//     let test = array_ref.clone();
//     // let py_data_type = PyDataType::from(array_ref.data_type().clone());
//
//     let processed_array = match array_ref.data_type() {
//         DataType::Struct(fields) => {
//             let struct_array = as_struct_array(array_ref);
//             Python::with_gil(|py| {
//                 let pyobject = to_pyobject(py, struct_array, 0).unwrap();
//                 let result = python_function.call1(py, PyTuple::new_bound(py, &[pyobject]))
//                     .and_then(|obj| obj.extract(py));
//                 test
//             })
//         }
//         _ => { Err(DataFusionError::NotImplemented("DataType".to_string())) }?,
//     };
//
//     Ok(processed_array)
// }

pub trait IntoField: private::Sealed {
    fn into_field(self, default_name: &str) -> Field;
}
impl IntoField for Field {
    fn into_field(self, _default_name: &str) -> Field {
        self
    }
}
impl IntoField for DataType {
    fn into_field(self, default_name: &str) -> Field {
        Field::new(default_name, self, true)
    }
}
mod private {
    use datafusion::arrow::datatypes::{DataType, Field};

    pub trait Sealed {}
    impl Sealed for Field {}
    impl Sealed for DataType {}
}
