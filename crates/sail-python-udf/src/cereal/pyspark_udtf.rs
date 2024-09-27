use std::hash::{Hash, Hasher};

use arrow::pyarrow::ToPyArrow;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{plan_datafusion_err, Result};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use sail_common::config::SparkUdfConfig;
use sail_common::spec;

use crate::cereal::{check_python_udf_version, PythonFunction};
use crate::error::{PyUdfError, PyUdfResult};

#[derive(Debug)]
pub struct PySparkUdtfObject(pub PyObject);

impl PythonFunction for PySparkUdtfObject {
    fn load(v: &[u8]) -> PyUdfResult<Self> {
        // build_pyspark_udtf_payload adds eval_type to the beginning of the payload
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
                .getattr(intern!(py, "read_udtf"))?
                .call1((serializer, infile, eval_type))?;
            Ok(PySparkUdtfObject(tuple.to_object(py)))
        })
    }

    fn function<'py>(&self, py: Python<'py>) -> PyUdfResult<Bound<'py, PyAny>> {
        Ok(self.0.clone_ref(py).into_bound(py).get_item(0)?)
    }
}

impl Hash for PySparkUdtfObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = self.0.as_ptr() as usize;
        ptr.hash(state);
    }
}

impl PartialEq for PySparkUdtfObject {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
    }
}

impl Eq for PySparkUdtfObject {}

pub fn deserialize_pyspark_udtf(
    python_version: &str,
    command: &[u8],
    eval_type: spec::PySparkUdfType,
    num_args: usize,
    return_type: &DataType,
    spark_udf_config: &SparkUdfConfig,
) -> Result<PySparkUdtfObject> {
    check_python_udf_version(python_version)?;
    let data =
        build_pyspark_udtf_payload(command, eval_type, num_args, return_type, spark_udf_config)?;
    Ok(PySparkUdtfObject::load(&data)?)
}

pub fn build_pyspark_udtf_payload(
    command: &[u8],
    eval_type: spec::PySparkUdfType,
    num_args: usize,
    return_type: &DataType,
    spark_udf_config: &SparkUdfConfig,
) -> Result<Vec<u8>> {
    let mut data: Vec<u8> = Vec::new();
    data.extend(&i32::from(eval_type).to_be_bytes()); // Add eval_type for extraction in visit_bytes
    if eval_type.is_arrow_udf() {
        let configs = [
            &spark_udf_config.timezone,
            &spark_udf_config.pandas_convert_to_arrow_array_safely,
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
    let num_args: i32 = num_args
        .try_into()
        .map_err(|e| plan_datafusion_err!("num_args: {e}"))?;
    data.extend(&num_args.to_be_bytes()); // num_args
    for index in 0..num_args {
        data.extend(&index.to_be_bytes()); // arg_offsets
    }
    data.extend(&(command.len() as i32).to_be_bytes()); // len of the function
    data.extend_from_slice(command);

    Python::with_gil(|py| -> PyUdfResult<_> {
        let return_type = return_type.to_pyarrow(py)?.clone_ref(py).into_bound(py);
        let result = PyModule::import_bound(py, intern!(py, "pyspark.sql.pandas.types"))?
            .getattr(intern!(py, "from_arrow_type"))?
            .call1((return_type,))?
            .getattr("json")?
            .call0()?;
        let json_string: String = result.extract()?;
        let json_length = json_string.len();

        data.extend(&(json_length as u32).to_be_bytes());
        data.extend(json_string.as_bytes());
        Ok(())
    })?;

    Ok(data)
}
