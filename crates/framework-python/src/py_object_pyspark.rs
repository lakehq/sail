use datafusion_common::DataFusionError;
use pyo3::prelude::{Py, PyAny, PyAnyMethods, PyModule, PyObject, Python, ToPyObject};
use pyo3::types::PyBytes;
use serde::de::{self, value::BorrowedBytesDeserializer, Deserialize, Deserializer, Visitor};
use serde_bytes::Bytes;

#[derive(Debug, Clone)]
pub struct PythonObjectWrapper {
    pub function: PyObject,
    pub return_type: PyObject,
}

struct PyObjectVisitor;

impl<'de> Visitor<'de> for PyObjectVisitor {
    type Value = PythonObjectWrapper;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a byte array containing the pickled Python object")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Python::with_gil(|py| {
            PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
                .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
                .and_then(|loads| loads.call1((v,)))
                .and_then(|py_tuple| {
                    let obj = py_tuple.get_item(0).map(|item| item.to_object(py));
                    let return_type = py_tuple.get_item(1).map(|item| item.to_object(py));
                    Ok(PythonObjectWrapper {
                        function: obj?,
                        return_type: return_type?,
                    })
                })
                .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))
        })
    }
}

impl<'de> Deserialize<'de> for PythonObjectWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PyObjectVisitor)
    }
}

pub fn deserialize_py_object_pyspark(
    command: &[u8],
) -> Result<PythonObjectWrapper, de::value::Error> {
    let bytes = Bytes::new(command);
    let deserializer: BorrowedBytesDeserializer<de::value::Error> =
        BorrowedBytesDeserializer::new(bytes);
    PythonObjectWrapper::deserialize(deserializer)
}

pub fn load_python_function(py: Python, command: &[u8]) -> Result<Py<PyAny>, DataFusionError> {
    let binary_sequence = PyBytes::new_bound(py, command);

    let python_function_tuple =
        PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
            .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
            .and_then(|loads| Ok(loads.call1((binary_sequence,))?))
            .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

    let python_function = python_function_tuple
        .get_item(0)
        .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

    if !python_function.is_callable() {
        return Err(DataFusionError::Execution(
            "Expected a callable Python function".to_string(),
        ));
    }

    Ok(python_function.into())
}
