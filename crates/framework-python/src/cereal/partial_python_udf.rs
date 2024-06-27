use std::hash::{Hash, Hasher};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use serde::de::{self, IntoDeserializer, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::Bytes;

#[derive(Debug, Clone)]
pub struct PartialPythonUDF(pub PyObject);

impl Serialize for PartialPythonUDF {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Python::with_gil(|py| {
            PyModule::import_bound(py, pyo3::intern!(py, "cloudpickle"))
                .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "dumps")))
                .and_then(|dumps| dumps.call1((self.0.clone_ref(py).into_bound(py),)))
                .and_then(|py_bytes| {
                    let bytes = Bytes::new(py_bytes.downcast::<PyBytes>()?.as_bytes());
                    Ok(serializer.serialize_bytes(bytes))
                })
                .map_err(|e| serde::ser::Error::custom(format!("Pickle Error: {}", e)))?
        })
    }
}

struct PartialPythonUDFVisitor;

impl<'de> Visitor<'de> for PartialPythonUDFVisitor {
    type Value = PartialPythonUDF;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a byte array containing the pickled Python object")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Python::with_gil(|py| {
            // TODO: Python error is converted to a serde Error,
            //  so we cannot propagate the original error back to the client.
            PyModule::import_bound(py, pyo3::intern!(py, "cloudpickle"))
                .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
                .and_then(|loads| loads.call1((v,)))
                .map(|py_tuple| PartialPythonUDF(py_tuple.to_object(py)))
                .map_err(|e| E::custom(format!("Pickle Error: {}", e)))
        })
    }
}

impl<'de> Deserialize<'de> for PartialPythonUDF {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PartialPythonUDFVisitor)
    }
}

impl Hash for PartialPythonUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let py_object_hash: PyResult<isize> = Python::with_gil(|py| self.0.bind(py).hash());
        match py_object_hash {
            Ok(py_object_hash) => py_object_hash.hash(state),
            Err(_) => serde_json::to_vec(self).unwrap().hash(state),
        }
    }
}

impl PartialEq for PartialPythonUDF {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| self.0.bind(py).eq(other.0.bind(py)).unwrap())
    }
}

impl Eq for PartialPythonUDF {}

pub fn deserialize_partial_python_udf(
    command: &[u8],
) -> Result<PartialPythonUDF, de::value::Error> {
    let bytes = Bytes::new(command);
    PartialPythonUDF::deserialize(bytes.into_deserializer())
}
