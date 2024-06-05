use std::hash::{Hash, Hasher};

use pyo3::{
    prelude::*,
    types::{PyBytes, PyModule},
};
use serde::de::{self, IntoDeserializer, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::Bytes;

const NON_UDF: i32 = 0;
const SQL_BATCHED_UDF: i32 = 100;
const SQL_ARROW_BATCHED_UDF: i32 = 101;

const SQL_SCALAR_PANDAS_UDF: i32 = 200;
const SQL_GROUPED_MAP_PANDAS_UDF: i32 = 201;
const SQL_GROUPED_AGG_PANDAS_UDF: i32 = 202;
const SQL_WINDOW_AGG_PANDAS_UDF: i32 = 203;
const SQL_SCALAR_PANDAS_ITER_UDF: i32 = 204;
const SQL_MAP_PANDAS_ITER_UDF: i32 = 205;
const SQL_COGROUPED_MAP_PANDAS_UDF: i32 = 206;
const SQL_MAP_ARROW_ITER_UDF: i32 = 207;
const SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE: i32 = 208;

const SQL_TABLE_UDF: i32 = 300;
const SQL_ARROW_TABLE_UDF: i32 = 301;

#[derive(Debug, Clone)]
pub struct PartialPythonUDF(pub PyObject);

impl Serialize for PartialPythonUDF {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Python::with_gil(|py| {
            PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
                .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "dumps")))
                .and_then(|dumps| dumps.call1((self.0.clone_ref(py).into_bound(py),)))
                .and_then(|py_bytes| {
                    let bytes = Bytes::new(py_bytes.downcast::<PyBytes>()?.as_bytes());
                    Ok(serializer.serialize_bytes(bytes))
                })
                .map_err(|e| serde::ser::Error::custom(format!("Pickle Error: {:?}", e)))?
        })
    }
}

struct PartialPythonUDFVisitor {
    eval_type: i32,
    num_args: i32,
}

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
            // PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
            //     .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
            //     .and_then(|loads| loads.call1((v,)))
            //     .and_then(|py_tuple| Ok(PartialPythonUDF(py_tuple.to_object(py))))
            //     .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))

            let mut data: Vec<u8> = Vec::new();

            if self.eval_type == SQL_ARROW_BATCHED_UDF
                || self.eval_type == SQL_SCALAR_PANDAS_UDF
                || self.eval_type == SQL_COGROUPED_MAP_PANDAS_UDF
                || self.eval_type == SQL_SCALAR_PANDAS_ITER_UDF
                || self.eval_type == SQL_MAP_PANDAS_ITER_UDF
                || self.eval_type == SQL_MAP_ARROW_ITER_UDF
                || self.eval_type == SQL_GROUPED_MAP_PANDAS_UDF
                || self.eval_type == SQL_GROUPED_AGG_PANDAS_UDF
                || self.eval_type == SQL_WINDOW_AGG_PANDAS_UDF
                || self.eval_type == SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE
            {
                data.extend(&0i32.to_be_bytes()); // num_conf
            }

            data.extend(&1i32.to_be_bytes()); // num_udfs
            data.extend(&self.num_args.to_be_bytes()); // num_args
            data.extend(&0i32.to_be_bytes()); // arg_offsets

            // let arg_offsets: Vec<i32> = vec![1]; // Your actual argument offsets
            // data.extend(&(arg_offsets.len() as i32).to_be_bytes()); // length of arg_offsets list
            // for &offset in &arg_offsets {
            //     data.extend(&offset.to_be_bytes()); // actual arg_offsets values
            // }

            data.extend(&1i32.to_be_bytes()); // num functions
            data.extend(&(v.len() as i32).to_be_bytes()); // len of the function
            data.extend_from_slice(v);
            let data = data.as_slice();

            let infile = PyModule::import_bound(py, pyo3::intern!(py, "io"))
                .and_then(|io| io.getattr(pyo3::intern!(py, "BytesIO")))
                // .and_then(|bytes_io| bytes_io.call1((PyBytes::new_bound(py, &data),)))
                .and_then(|bytes_io| bytes_io.call1((data,)))
                .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))?;
            let pickle_ser = PyModule::import_bound(py, pyo3::intern!(py, "pyspark.serializers"))
                .and_then(|serializers| serializers.getattr(pyo3::intern!(py, "CPickleSerializer")))
                .and_then(|serializer| serializer.call0())
                .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))?;

            PyModule::import_bound(py, pyo3::intern!(py, "pyspark.worker"))
                .and_then(|worker| worker.getattr(pyo3::intern!(py, "read_udfs")))
                .and_then(|read_udfs| read_udfs.call1((pickle_ser, infile, self.eval_type)))
                .and_then(|py_tuple| Ok(PartialPythonUDF(py_tuple.to_object(py))))
                .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))
        })
    }
}

// impl<'de> Deserialize<'de> for PartialPythonUDF {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         deserializer.deserialize_bytes(PartialPythonUDFVisitor)
//     }
// }

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

// pub fn deserialize_partial_python_udf(
//     command: &[u8],
//     eval_type: &i32,
// ) -> Result<PartialPythonUDF, de::value::Error> {
//     let bytes = Bytes::new(command);
//     PartialPythonUDF::deserialize(bytes.into_deserializer(), eval_type)
// }

pub fn deserialize_partial_python_udf(
    command: &[u8],
    eval_type: &i32,
    num_args: &i32,
) -> Result<PartialPythonUDF, de::value::Error> {
    let bytes = Bytes::new(command);
    let visitor = PartialPythonUDFVisitor {
        eval_type: *eval_type,
        num_args: *num_args,
    };
    let deserializer = bytes.into_deserializer();
    deserializer.deserialize_bytes(visitor)
}
