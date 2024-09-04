use std::hash::{Hash, Hasher};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use sail_common::config::SparkUdfConfig;
use serde::de::{self, IntoDeserializer, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::Bytes;

use crate::cereal::{is_pyspark_arrow_udf, is_pyspark_pandas_udf};

#[derive(Debug, Clone)]
pub struct PartialPySparkUDF(pub PyObject);

impl Serialize for PartialPySparkUDF {
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
                .map_err(|e| serde::ser::Error::custom(format!("Pickle Error: {}", e)))?
        })
    }
}

struct PartialPySparkUDFVisitor;

impl<'de> Visitor<'de> for PartialPySparkUDFVisitor {
    type Value = PartialPySparkUDF;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a byte array containing the pickled Python object")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // build_pyspark_udf_payload adds eval_type to the beginning of the payload
        let (eval_type_bytes, v) = v.split_at(std::mem::size_of::<i32>());
        let eval_type = i32::from_be_bytes(
            eval_type_bytes
                .try_into()
                .map_err(|e| E::custom(format!("eval_type from_be_bytes: {}", e)))?,
        );
        Python::with_gil(|py| {
            // TODO: Python error is converted to a serde Error,
            //  so we cannot propagate the original error back to the client.
            let infile: Bound<PyAny> = PyModule::import_bound(py, pyo3::intern!(py, "io"))
                .and_then(|io| io.getattr(pyo3::intern!(py, "BytesIO")))
                .and_then(|bytes_io| bytes_io.call1((v,)))
                .map_err(|e| E::custom(format!("PartialPySparkUDFVisitor BytesIO Error: {}", e)))?;
            let pickle_ser: Bound<PyAny> =
                PyModule::import_bound(py, pyo3::intern!(py, "pyspark.serializers"))
                    .and_then(|serializers| {
                        serializers.getattr(pyo3::intern!(py, "CPickleSerializer"))
                    })
                    .and_then(|serializer| serializer.call0())
                    .map_err(|e| {
                        E::custom(format!(
                            "PartialPySparkUDFVisitor CPickleSerializer Error: {}",
                            e
                        ))
                    })?;
            PyModule::import_bound(py, pyo3::intern!(py, "pyspark.worker"))
                .and_then(|worker| worker.getattr(pyo3::intern!(py, "read_udfs")))
                .and_then(|read_udfs| read_udfs.call1((pickle_ser, infile, eval_type)))
                .map(|py_tuple| PartialPySparkUDF(py_tuple.to_object(py)))
                .map_err(|e| E::custom(format!("PartialPySparkUDFVisitor Pickle Error: {}", e)))
        })
    }
}

impl<'de> Deserialize<'de> for PartialPySparkUDF {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PartialPySparkUDFVisitor)
    }
}

impl Hash for PartialPySparkUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let py_object_hash: PyResult<isize> = Python::with_gil(|py| self.0.bind(py).hash());
        match py_object_hash {
            Ok(py_object_hash) => py_object_hash.hash(state),
            Err(_) => serde_json::to_vec(self).unwrap().hash(state),
        }
    }
}

impl PartialEq for PartialPySparkUDF {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| self.0.bind(py).eq(other.0.bind(py)).unwrap())
    }
}

impl Eq for PartialPySparkUDF {}

pub fn deserialize_partial_pyspark_udf(
    python_version: &str,
    command: &[u8],
    eval_type: &i32,
    num_args: &i32,
    spark_udf_config: &SparkUdfConfig,
) -> Result<PartialPySparkUDF, de::value::Error> {
    let pyo3_python_version: String = Python::with_gil(|py| py.version().to_string());
    if !pyo3_python_version.starts_with(python_version) {
        return Err(de::Error::custom(format!(
            "Python version mismatch. Version used to compile the UDF must match the version used to run the UDF. Version used to compile the UDF: {}. Version used to run the UDF: {}",
            python_version,
            pyo3_python_version,
        )));
    }
    let data: Vec<u8> = build_pyspark_udf_payload(command, eval_type, num_args, spark_udf_config);
    let bytes = Bytes::new(data.as_slice());
    PartialPySparkUDF::deserialize(bytes.into_deserializer())
}

pub fn build_pyspark_udf_payload(
    command: &[u8],
    eval_type: &i32,
    num_args: &i32,
    spark_udf_config: &SparkUdfConfig,
) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::new();
    data.extend(&eval_type.to_be_bytes()); // Add eval_type for extraction in visit_bytes
    if is_pyspark_arrow_udf(eval_type) || is_pyspark_pandas_udf(eval_type) {
        let configs = [
            &spark_udf_config.timezone,
            &spark_udf_config.pandas_window_bound_types,
            &spark_udf_config.pandas_grouped_map_assign_columns_by_name,
            &spark_udf_config.pandas_convert_to_arrow_array_safely,
            &spark_udf_config.arrow_max_records_per_batch,
        ];
        let mut num_conf: i32 = 0;
        let mut temp_data: Vec<u8> = Vec::new();
        for config in configs {
            if let Some(value) = &config.value {
                temp_data.extend(&(config.key.len() as i32).to_be_bytes()); // len of the key
                temp_data.extend(config.key.as_bytes());
                temp_data.extend(&(value.len() as i32).to_be_bytes()); // len of the value
                temp_data.extend(value.as_bytes());
                num_conf += 1;
            }
        }
        data.extend(&num_conf.to_be_bytes()); // num_conf
        data.extend(&temp_data);
    }
    data.extend(&1i32.to_be_bytes()); // num_udfs
    data.extend(&num_args.to_be_bytes()); // num_args
    for index in 0..*num_args {
        data.extend(&index.to_be_bytes()); // arg_offsets
    }
    data.extend(&1i32.to_be_bytes()); // num functions
    data.extend(&(command.len() as i32).to_be_bytes()); // len of the function
    data.extend_from_slice(command);

    data
}
