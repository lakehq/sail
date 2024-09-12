use std::hash::{Hash, Hasher};

use datafusion_common::{plan_datafusion_err, plan_err, Result};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use sail_common::config::SparkUdfConfig;
use sail_common::spec;

use crate::cereal::PythonFunction;
use crate::error::{PyUdfError, PyUdfResult};

#[derive(Debug, Clone)]
pub struct PySparkUdfObject(pub PyObject);

impl PythonFunction for PySparkUdfObject {
    fn load(v: &[u8]) -> PyUdfResult<Self> {
        // build_pyspark_udf_payload adds eval_type to the beginning of the payload
        let (eval_type_bytes, v) = v.split_at(std::mem::size_of::<i32>());
        let eval_type = i32::from_be_bytes(
            eval_type_bytes
                .try_into()
                .map_err(|e| PyUdfError::invalid(format!("eval_type from_be_bytes: {e}")))?,
        );
        Python::with_gil(|py| {
            let infile: Bound<PyAny> = PyModule::import_bound(py, pyo3::intern!(py, "io"))
                .and_then(|io| io.getattr(pyo3::intern!(py, "BytesIO")))
                .and_then(|bytes_io| bytes_io.call1((v,)))?;
            let pickle_ser: Bound<PyAny> =
                PyModule::import_bound(py, pyo3::intern!(py, "pyspark.serializers"))
                    .and_then(|serializers| {
                        serializers.getattr(pyo3::intern!(py, "CPickleSerializer"))
                    })
                    .and_then(|serializer| serializer.call0())?;
            let obj = PyModule::import_bound(py, pyo3::intern!(py, "pyspark.worker"))
                .and_then(|worker| worker.getattr(pyo3::intern!(py, "read_udfs")))
                .and_then(|read_udfs| read_udfs.call1((pickle_ser, infile, eval_type)))
                .map(|py_tuple| PySparkUdfObject(py_tuple.to_object(py)))?;
            Ok(obj)
        })
    }

    fn function<'py>(&self, py: Python<'py>) -> PyUdfResult<Bound<'py, PyAny>> {
        Ok(self.0.clone_ref(py).into_bound(py).get_item(0)?)
    }
}

// We use the memory address of the Python object to determine equality
// and hash the function object. This is a conservative approach, but
// it is efficient and cannot fail.

impl Hash for PySparkUdfObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = self.0.as_ptr() as usize;
        ptr.hash(state);
    }
}

impl PartialEq for PySparkUdfObject {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
    }
}

impl Eq for PySparkUdfObject {}

pub fn deserialize_partial_pyspark_udf(
    python_version: &str,
    command: &[u8],
    eval_type: spec::PySparkUdfType,
    num_args: usize,
    spark_udf_config: &SparkUdfConfig,
) -> Result<PySparkUdfObject> {
    let pyo3_python_version: String = Python::with_gil(|py| py.version().to_string());
    if !pyo3_python_version.starts_with(python_version) {
        return plan_err!(
            "Python version mismatch. Version used to compile the UDF must match the version used to run the UDF. Version used to compile the UDF: {}. Version used to run the UDF: {}",
            python_version,
            pyo3_python_version
        );
    }
    let data: Vec<u8> = build_pyspark_udf_payload(command, eval_type, num_args, spark_udf_config)?;
    Ok(PySparkUdfObject::load(&data)?)
}

pub fn build_pyspark_udf_payload(
    command: &[u8],
    eval_type: spec::PySparkUdfType,
    num_args: usize,
    spark_udf_config: &SparkUdfConfig,
) -> Result<Vec<u8>> {
    let mut data: Vec<u8> = Vec::new();
    data.extend(&i32::from(eval_type).to_be_bytes()); // Add eval_type for extraction in visit_bytes
    if eval_type.is_arrow_udf() || eval_type.is_pandas_udf() {
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
    let num_args: i32 = num_args
        .try_into()
        .map_err(|e| plan_datafusion_err!("num_args: {e}"))?;
    data.extend(&num_args.to_be_bytes()); // num_args
    for index in 0..num_args {
        data.extend(&index.to_be_bytes()); // arg_offsets
    }
    data.extend(&1i32.to_be_bytes()); // num functions
    data.extend(&(command.len() as i32).to_be_bytes()); // len of the function
    data.extend_from_slice(command);

    Ok(data)
}
