use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{intern, PyObject, Python, ToPyObject};

use crate::cereal::check_python_udf_version;
use crate::config::SparkUdfConfig;
use crate::error::{PyUdfError, PyUdfResult};

pub struct PySparkUdfPayload<'a> {
    pub python_version: &'a str,
    pub command: &'a [u8],
    pub eval_type: i32,
    pub arg_offsets: &'a [usize],
    pub config: &'a SparkUdfConfig,
}

impl PySparkUdfPayload<'_> {
    pub fn load(py: Python, data: &[u8]) -> PyUdfResult<PyObject> {
        let (eval_type, v) = data
            .split_at_checked(size_of::<i32>())
            .ok_or_else(|| PyUdfError::invalid("missing eval_type"))?;
        let eval_type = eval_type
            .try_into()
            .map_err(|e| PyValueError::new_err(format!("eval_type bytes: {e}")))?;
        let eval_type = i32::from_be_bytes(eval_type);
        let infile = PyModule::import_bound(py, intern!(py, "io"))?
            .getattr(intern!(py, "BytesIO"))?
            .call1((v,))?;
        let serializer = PyModule::import_bound(py, intern!(py, "pyspark.serializers"))?
            .getattr(intern!(py, "CPickleSerializer"))?
            .call0()?;
        let tuple = PyModule::import_bound(py, intern!(py, "pyspark.worker"))?
            .getattr(intern!(py, "read_udfs"))?
            .call1((serializer, infile, eval_type))?;
        Ok(tuple.get_item(0)?.to_object(py))
    }

    pub fn write(&self) -> PyUdfResult<Vec<u8>> {
        check_python_udf_version(self.python_version)?;
        let mut data: Vec<u8> = Vec::new();

        data.extend(&self.eval_type.to_be_bytes());

        let config = self.config.to_key_value_pairs();
        data.extend((config.len() as i32).to_be_bytes()); // number of configuration options
        for (key, value) in config {
            data.extend(&(key.len() as i32).to_be_bytes()); // length of the key
            data.extend(key.as_bytes());
            data.extend(&(value.len() as i32).to_be_bytes()); // length of the value
            data.extend(value.as_bytes());
        }

        data.extend(&1i32.to_be_bytes()); // number of UDFs

        let num_arg_offsets: i32 = self
            .arg_offsets
            .len()
            .try_into()
            .map_err(|e| PyUdfError::invalid(format!("num args: {e}")))?;
        data.extend(&num_arg_offsets.to_be_bytes()); // number of argument offsets
        for offset in self.arg_offsets {
            let offset: i32 = (*offset)
                .try_into()
                .map_err(|e| PyUdfError::invalid(format!("arg offset: {e}")))?;
            data.extend(&offset.to_be_bytes()); // argument offset
        }

        data.extend(&1i32.to_be_bytes()); // number of functions
        data.extend(&(self.command.len() as i32).to_be_bytes()); // length of the function
        data.extend_from_slice(self.command);

        Ok(data)
    }
}
