use crate::impl_py_state_serialization;
use datafusion::arrow::datatypes::DataType;
use pyo3::{
    prelude::{pyclass, pymethods, PyAnyMethods, PyObject, PyResult, Python, ToPyObject},
    types::PyBytes,
    PyTypeInfo,
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
