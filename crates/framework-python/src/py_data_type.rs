use crate::impl_py_state_serialization;
use crate::utils::process_array_ref_with_python_function;
use datafusion::arrow::array::{as_struct_array, types, ArrayRef, PrimitiveArray, StructArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::DataFusionError;
use pyo3::types::PyTuple;
use pyo3::{
    prelude::{pyclass, pymethods, PyAnyMethods, PyObject, PyResult, Python, ToPyObject},
    types::{PyBytes, PyList},
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
