use datafusion::arrow::datatypes::DataType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{Bound, IntoPyObject, PyAny, Python, intern};
use sail_common::spec;

use crate::cereal::{
    PySparkVersion, build_input_types_json, check_python_udf_version, get_pyspark_version,
    should_write_config, supports_kwargs, write_conf, write_kwarg,
};
use crate::config::PySparkUdfConfig;
use crate::error::{PyUdfError, PyUdfResult};

pub struct PySparkUdfPayload;

impl PySparkUdfPayload {
    pub fn load<'py>(py: Python<'py>, data: &[u8]) -> PyUdfResult<Bound<'py, PyAny>> {
        let (eval_type, v) = data
            .split_at_checked(size_of::<i32>())
            .ok_or_else(|| PyUdfError::invalid("missing eval_type"))?;
        let eval_type = eval_type
            .try_into()
            .map_err(|e| PyValueError::new_err(format!("eval_type bytes: {e}")))?;
        let eval_type = i32::from_be_bytes(eval_type);
        let infile = PyModule::import(py, intern!(py, "io"))?
            .getattr(intern!(py, "BytesIO"))?
            .call1((v,))?;
        let serializer = PyModule::import(py, intern!(py, "pyspark.serializers"))?
            .getattr(intern!(py, "CPickleSerializer"))?
            .call0()?;
        let worker = PyModule::import(py, intern!(py, "pyspark.worker"))?;
        let read_udfs = worker.getattr(intern!(py, "read_udfs"))?;
        let tuple = match get_pyspark_version()? {
            PySparkVersion::V4_2 => {
                let runner_conf = worker
                    .getattr(intern!(py, "RunnerConf"))?
                    .call1((&infile,))?;
                let eval_conf = worker.getattr(intern!(py, "EvalConf"))?.call1((&infile,))?;
                read_udfs.call1((serializer, infile, eval_type, runner_conf, eval_conf))?
            }
            PySparkVersion::V3 | PySparkVersion::V4_0 | PySparkVersion::V4_1 => {
                read_udfs.call1((serializer, infile, eval_type))?
            }
        };
        tuple
            .get_item(0)?
            .into_pyobject(py)
            .map_err(|e| PyUdfError::PythonError(e.into()))
    }

    pub fn build(
        python_version: &str,
        command: &[u8],
        eval_type: spec::PySparkUdfType,
        arg_offsets: &[usize],
        input_types: &[DataType],
        // Per-argument kwarg name: None for positional, Some(key) for keyword
        kwarg_names: &[Option<String>],
        config: &PySparkUdfConfig,
    ) -> PyUdfResult<Vec<u8>> {
        check_python_udf_version(python_version)?;
        let pyspark_version = get_pyspark_version()?;
        let mut data: Vec<u8> = Vec::new();

        data.extend(i32::from(eval_type).to_be_bytes());

        match pyspark_version {
            PySparkVersion::V4_2 => {
                // Spark 4.2 reads both maps before dispatching to read_udfs.
                write_conf(&mut data, config.to_key_value_pairs());
                let mut eval_conf = vec![];
                if eval_type == spec::PySparkUdfType::ArrowBatched {
                    eval_conf.push((
                        "input_type".to_string(),
                        build_input_types_json(input_types, config.arrow_use_large_var_types)?,
                    ))
                }
                write_conf(&mut data, eval_conf);
            }
            PySparkVersion::V3 | PySparkVersion::V4_0 | PySparkVersion::V4_1 => {
                if should_write_config(eval_type) {
                    write_conf(&mut data, config.to_key_value_pairs());
                }

                // PySpark 4.1 reads input types for ArrowBatched UDFs.
                // PySpark 4.0.x does not read input types and would misparse the stream.
                if pyspark_version == PySparkVersion::V4_1
                    && eval_type == spec::PySparkUdfType::ArrowBatched
                {
                    let schema_json =
                        build_input_types_json(input_types, config.arrow_use_large_var_types)?;
                    data.extend((schema_json.len() as i32).to_be_bytes());
                    data.extend(schema_json.as_bytes());
                }
            }
        }

        match pyspark_version {
            PySparkVersion::V4_0 | PySparkVersion::V4_1 => {
                data.extend(0u8.to_be_bytes()); // profiling is not enabled
            }
            PySparkVersion::V3 | PySparkVersion::V4_2 => {}
        }

        data.extend(1i32.to_be_bytes()); // number of UDFs

        let num_arg_offsets: i32 = arg_offsets
            .len()
            .try_into()
            .map_err(|e| PyUdfError::invalid(format!("num args: {e}")))?;
        data.extend(num_arg_offsets.to_be_bytes()); // number of argument offsets

        let allow_kwargs = match pyspark_version {
            PySparkVersion::V4_2 => true,
            PySparkVersion::V4_0 | PySparkVersion::V4_1 => supports_kwargs(eval_type),
            PySparkVersion::V3 => false,
        };

        for (i, offset) in arg_offsets.iter().enumerate() {
            let offset: i32 = (*offset)
                .try_into()
                .map_err(|e| PyUdfError::invalid(format!("arg offset: {e}")))?;
            data.extend(offset.to_be_bytes()); // argument offset
            if allow_kwargs {
                write_kwarg(&mut data, kwarg_names, i);
            }
        }

        data.extend(1i32.to_be_bytes()); // number of functions
        data.extend((command.len() as i32).to_be_bytes()); // length of the function
        data.extend_from_slice(command);

        match pyspark_version {
            PySparkVersion::V4_2 => {
                // Spark 4.2 always sends this field, even with profiling disabled.
                data.extend(0i64.to_be_bytes());
            }
            PySparkVersion::V3 | PySparkVersion::V4_0 | PySparkVersion::V4_1 => {}
        }

        Ok(data)
    }
}
