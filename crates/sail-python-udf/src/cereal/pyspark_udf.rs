use std::hash::{Hash, Hasher};

use datafusion_common::{plan_datafusion_err, Result};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use sail_common::config::SparkUdfConfig;
use sail_common::spec;

use crate::cereal::{check_python_udf_version, PythonFunction};
use crate::error::{PyUdfError, PyUdfResult};

#[derive(Debug)]
pub struct PySparkUdfObject(pub PyObject);

impl Clone for PySparkUdfObject {
    fn clone(&self) -> Self {
        let obj = Python::with_gil(|py| self.0.bind(py).to_object(py));
        Self(obj)
    }
}

impl PythonFunction for PySparkUdfObject {
    fn load(v: &[u8]) -> PyUdfResult<Self> {
        // build_pyspark_udf_payload adds eval_type to the beginning of the payload
        let (eval_type_bytes, v) = v.split_at(size_of::<i32>());
        let eval_type = i32::from_be_bytes(
            eval_type_bytes
                .try_into()
                .map_err(|e| PyUdfError::invalid(format!("eval_type from_be_bytes: {e}")))?,
        );
        Python::with_gil(|py| {
            let infile = PyModule::import_bound(py, intern!(py, "io"))?
                .getattr(intern!(py, "BytesIO"))?
                .call1((v,))?;
            let serializer = PyModule::import_bound(py, intern!(py, "pyspark.serializers"))?
                .getattr(intern!(py, "CPickleSerializer"))?
                .call0()?;
            let tuple = PyModule::import_bound(py, intern!(py, "pyspark.worker"))?
                .getattr(intern!(py, "read_udfs"))?
                .call1((serializer, infile, eval_type))?;
            Ok(PySparkUdfObject(tuple.to_object(py)))
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
    check_python_udf_version(python_version)?;
    let data = build_pyspark_udf_payload(command, eval_type, num_args, spark_udf_config)?;
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
        let mut num_config: i32 = 0;
        let mut config_data: Vec<u8> = Vec::new();
        for config in configs {
            if let Some(value) = &config.value {
                config_data.extend(&(config.key.len() as i32).to_be_bytes()); // len of the key
                config_data.extend(config.key.as_bytes());
                config_data.extend(&(value.len() as i32).to_be_bytes()); // len of the value
                config_data.extend(value.as_bytes());
                num_config += 1;
            }
        }
        data.extend(&num_config.to_be_bytes()); // num_config
        data.extend(&config_data);
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
