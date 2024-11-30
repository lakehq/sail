use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use sail_common::spec;

use crate::config::SparkUdfConfig;
use crate::error::{PyUdfError, PyUdfResult};

pub struct PySparkUdtfObject;

impl PySparkUdtfObject {
    pub fn load(py: Python, v: &[u8]) -> PyUdfResult<PyObject> {
        // build_pyspark_udtf_payload adds eval_type to the beginning of the payload
        let (eval_type_bytes, v) = v.split_at(size_of::<i32>());
        let eval_type = i32::from_be_bytes(
            eval_type_bytes
                .try_into()
                .map_err(|e| PyValueError::new_err(format!("eval_type from_be_bytes: {e}")))?,
        );
        let infile = PyModule::import_bound(py, intern!(py, "io"))?
            .getattr(intern!(py, "BytesIO"))?
            .call1((v,))?;
        let serializer = PyModule::import_bound(py, intern!(py, "pyspark.serializers"))?
            .getattr(intern!(py, "CPickleSerializer"))?
            .call0()?;
        let tuple = PyModule::import_bound(py, intern!(py, "pyspark.worker"))?
            .getattr(intern!(py, "read_udtf"))?
            .call1((serializer, infile, eval_type))?;
        Ok(tuple.get_item(0)?.to_object(py))
    }
}

pub fn build_pyspark_udtf_payload(
    command: &[u8],
    eval_type: spec::PySparkUdfType,
    num_args: usize,
    return_type: &DataType,
    config: &SparkUdfConfig,
) -> PyUdfResult<Vec<u8>> {
    let mut data: Vec<u8> = Vec::new();
    data.extend(&i32::from(eval_type).to_be_bytes()); // Add eval_type for extraction in visit_bytes
    if eval_type.is_arrow_udf() {
        let config = config.to_key_value_pairs();
        data.extend((config.len() as i32).to_be_bytes()); // number of configuration options
        for (key, value) in config {
            data.extend(&(key.len() as i32).to_be_bytes()); // length of the key
            data.extend(key.as_bytes());
            data.extend(&(value.len() as i32).to_be_bytes()); // length of the value
            data.extend(value.as_bytes());
        }
    }
    let num_args: i32 = num_args
        .try_into()
        .map_err(|e| PyUdfError::invalid(format!("num_args: {e}")))?;
    data.extend(&num_args.to_be_bytes()); // number of arguments
    for index in 0..num_args {
        data.extend(&index.to_be_bytes()); // argument offset
    }
    data.extend(&(command.len() as i32).to_be_bytes()); // length of the function
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
