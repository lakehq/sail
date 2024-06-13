use std::hash::{Hash, Hasher};

use pyo3::{
    prelude::*,
    types::{PyBytes, PyModule},
};
use serde::de::{self, IntoDeserializer, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::Bytes;

use framework_common::config::SparkUdfConfig;

pub const PY_SPARK_NON_UDF: i32 = 0;
pub const PY_SPARK_SQL_BATCHED_UDF: i32 = 100;
pub const PY_SPARK_SQL_ARROW_BATCHED_UDF: i32 = 101;
pub const PY_SPARK_SQL_SCALAR_PANDAS_UDF: i32 = 200;
pub const PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF: i32 = 201;
pub const PY_SPARK_SQL_GROUPED_AGG_PANDAS_UDF: i32 = 202;
pub const PY_SPARK_SQL_WINDOW_AGG_PANDAS_UDF: i32 = 203;
pub const PY_SPARK_SQL_SCALAR_PANDAS_ITER_UDF: i32 = 204;
pub const PY_SPARK_SQL_MAP_PANDAS_ITER_UDF: i32 = 205;
pub const PY_SPARK_SQL_COGROUPED_MAP_PANDAS_UDF: i32 = 206;
pub const PY_SPARK_SQL_MAP_ARROW_ITER_UDF: i32 = 207;
pub const PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE: i32 = 208;
pub const PY_SPARK_SQL_TABLE_UDF: i32 = 300;
pub const PY_SPARK_SQL_ARROW_TABLE_UDF: i32 = 301;

pub fn is_pyspark_arrow_udf(udf_type: i32) -> bool {
    udf_type == PY_SPARK_SQL_ARROW_BATCHED_UDF
        || udf_type == PY_SPARK_SQL_MAP_ARROW_ITER_UDF
        || udf_type == PY_SPARK_SQL_ARROW_TABLE_UDF
}

pub fn is_pyspark_pandas_udf(udf_type: i32) -> bool {
    udf_type == PY_SPARK_SQL_SCALAR_PANDAS_UDF
        || udf_type == PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF
        || udf_type == PY_SPARK_SQL_GROUPED_AGG_PANDAS_UDF
        || udf_type == PY_SPARK_SQL_WINDOW_AGG_PANDAS_UDF
        || udf_type == PY_SPARK_SQL_SCALAR_PANDAS_ITER_UDF
        || udf_type == PY_SPARK_SQL_MAP_PANDAS_ITER_UDF
        || udf_type == PY_SPARK_SQL_COGROUPED_MAP_PANDAS_UDF
        || udf_type == PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE
}

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
                .map_err(|e| serde::ser::Error::custom(format!("Pickle Error: {:?}", e)))?
        })
    }
}

struct PartialPySparkUDFVisitor {
    eval_type: i32,
    num_args: i32,
    spark_udf_config: SparkUdfConfig,
}

impl<'de> Visitor<'de> for PartialPySparkUDFVisitor {
    type Value = PartialPySparkUDF;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a byte array containing the pickled Python object")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut data: Vec<u8> = Vec::new();
        if is_pyspark_arrow_udf(self.eval_type) || is_pyspark_pandas_udf(self.eval_type) {
            let configs = [
                &self.spark_udf_config.timezone,
                &self.spark_udf_config.pandas_window_bound_types,
                &self
                    .spark_udf_config
                    .pandas_grouped_map_assign_columns_by_name,
                &self.spark_udf_config.pandas_convert_to_arrow_array_safely,
                &self.spark_udf_config.arrow_max_records_per_batch,
            ];
            let mut num_conf: i32 = 0;
            let mut temp_data: Vec<u8> = Vec::new();
            for config in &configs {
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
        data.extend(&self.num_args.to_be_bytes()); // num_args
        for index in 0..self.num_args {
            data.extend(&index.to_be_bytes()); // arg_offsets
        }
        data.extend(&1i32.to_be_bytes()); // num functions
        data.extend(&(v.len() as i32).to_be_bytes()); // len of the function
        data.extend_from_slice(v);
        let data: &[u8] = data.as_slice();

        Python::with_gil(|py| {
            let infile: Bound<PyAny> = PyModule::import_bound(py, pyo3::intern!(py, "io"))
                .and_then(|io| io.getattr(pyo3::intern!(py, "BytesIO")))
                .and_then(|bytes_io| bytes_io.call1((data,)))
                .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))?;
            let pickle_ser: Bound<PyAny> =
                PyModule::import_bound(py, pyo3::intern!(py, "pyspark.serializers"))
                    .and_then(|serializers| {
                        serializers.getattr(pyo3::intern!(py, "CPickleSerializer"))
                    })
                    .and_then(|serializer| serializer.call0())
                    .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))?;
            PyModule::import_bound(py, pyo3::intern!(py, "pyspark.worker"))
                .and_then(|worker| worker.getattr(pyo3::intern!(py, "read_udfs")))
                .and_then(|read_udfs| read_udfs.call1((pickle_ser, infile, self.eval_type)))
                .and_then(|py_tuple| Ok(PartialPySparkUDF(py_tuple.to_object(py))))
                .map_err(|e| E::custom(format!("Pickle Error: {:?}", e)))
        })
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
            "Python version mismatch. Version used to compile the UDF must match the version used to run the UDF. Version used to compile the UDF: {:?}. Version used to run the UDF: {:?}",
            python_version,
            pyo3_python_version,
        )));
    }
    let bytes = Bytes::new(command);
    let visitor = PartialPySparkUDFVisitor {
        eval_type: *eval_type,
        num_args: *num_args,
        spark_udf_config: spark_udf_config.clone(),
    };
    let deserializer = bytes.into_deserializer();
    deserializer.deserialize_bytes(visitor)
}
