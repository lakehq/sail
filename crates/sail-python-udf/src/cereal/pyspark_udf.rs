use std::fmt::Debug;

use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyModule;
use sail_common::spec;

use crate::cereal::check_python_udf_version;
use crate::config::SparkUdfConfig;
use crate::error::{PyUdfError, PyUdfResult};

pub struct PySparkUdfObject {
    data: Vec<u8>,
    cell: GILOnceCell<PyObject>,
}

impl Debug for PySparkUdfObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySparkUdfObject")
            .field("data", &self.data)
            .finish()
    }
}

impl PySparkUdfObject {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            cell: GILOnceCell::new(),
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn get(&self, py: Python) -> PyUdfResult<PyObject> {
        self.cell
            .get_or_try_init(py, || Self::load(py, self.data()))
            .map(|x| x.clone_ref(py))
    }

    pub fn load(py: Python, data: &[u8]) -> PyUdfResult<PyObject> {
        if data.is_empty() {
            // FIXME: This is hacky. We should create a dedicated UDF/UDAF when the Python function
            //   is not needed (e.g. for the no-op UDAF), rather than using a dummy Python function.
            return Ok(py.None());
        }
        // build_pyspark_udf_payload adds eval_type to the beginning of the payload
        let (eval_type_bytes, v) = data.split_at(size_of::<i32>());
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
            .getattr(intern!(py, "read_udfs"))?
            .call1((serializer, infile, eval_type))?;
        Ok(tuple.get_item(0)?.to_object(py))
    }
}

pub fn build_pyspark_udf_payload(
    python_version: &str,
    command: &[u8],
    eval_type: spec::PySparkUdfType,
    arg_offsets: &[usize],
    config: &SparkUdfConfig,
) -> PyUdfResult<Vec<u8>> {
    check_python_udf_version(python_version)?;
    let mut data: Vec<u8> = Vec::new();
    data.extend(&i32::from(eval_type).to_be_bytes()); // Add eval_type for extraction in visit_bytes
    if eval_type.is_arrow_udf() || eval_type.is_pandas_udf() {
        let config = config.to_key_value_pairs();
        data.extend((config.len() as i32).to_be_bytes()); // number of configuration options
        for (key, value) in config {
            data.extend(&(key.len() as i32).to_be_bytes()); // length of the key
            data.extend(key.as_bytes());
            data.extend(&(value.len() as i32).to_be_bytes()); // length of the value
            data.extend(value.as_bytes());
        }
    }
    data.extend(&1i32.to_be_bytes()); // number of UDFs
    let num_arg_offsets: i32 = arg_offsets
        .len()
        .try_into()
        .map_err(|e| PyUdfError::invalid(format!("num args: {e}")))?;
    data.extend(&num_arg_offsets.to_be_bytes()); // number of argument offsets
    for offset in arg_offsets {
        let offset: i32 = (*offset)
            .try_into()
            .map_err(|e| PyUdfError::invalid(format!("arg offset: {e}")))?;
        data.extend(&offset.to_be_bytes()); // argument offset
    }
    data.extend(&1i32.to_be_bytes()); // number of functions
    data.extend(&(command.len() as i32).to_be_bytes()); // length of the function
    data.extend_from_slice(command);

    Ok(data)
}
