use arrow_pyarrow::ToPyArrow;
use datafusion::arrow::datatypes::DataType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{intern, Bound, IntoPyObject, PyAny, PyResult, Python};
use sail_common::spec;

use crate::cereal::{
    build_input_types_json, check_python_udf_version, get_pyspark_version, should_write_config,
    supports_kwargs, write_kwarg, PySparkVersion,
};
use crate::config::PySparkUdfConfig;
use crate::error::{PyUdfError, PyUdfResult};

pub struct PySparkUdtfPayload;

impl PySparkUdtfPayload {
    pub fn load<'py>(py: Python<'py>, v: &[u8]) -> PyUdfResult<Bound<'py, PyAny>> {
        let (eval_type, v) = v
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
        let tuple = PyModule::import(py, intern!(py, "pyspark.worker"))?
            .getattr(intern!(py, "read_udtf"))?
            .call1((serializer, infile, eval_type))?;
        tuple
            .get_item(0)?
            .into_pyobject(py)
            .map_err(|e| PyUdfError::PythonError(e.into()))
    }

    pub fn build(
        python_version: &str,
        command: &[u8],
        eval_type: spec::PySparkUdfType,
        num_args: usize,
        input_types: &[DataType],
        kwargs: &[Option<String>],
        return_type: &DataType,
        config: &PySparkUdfConfig,
    ) -> PyUdfResult<Vec<u8>> {
        check_python_udf_version(python_version)?;
        let pyspark_version = get_pyspark_version()?;
        let mut data: Vec<u8> = Vec::new();

        data.extend(i32::from(eval_type).to_be_bytes()); // Add eval_type for extraction in visit_bytes

        if should_write_config(eval_type) {
            let config = config.to_key_value_pairs();
            data.extend((config.len() as i32).to_be_bytes()); // number of configuration options
            for (key, value) in config {
                data.extend((key.len() as i32).to_be_bytes()); // length of the key
                data.extend(key.as_bytes());
                data.extend((value.len() as i32).to_be_bytes()); // length of the value
                data.extend(value.as_bytes());
            }
        }

        // PySpark 4.1+ reads input types for ArrowTable UDTFs.
        // PySpark 4.0.x does not read input types and would misparse the stream.
        if matches!(pyspark_version, PySparkVersion::V4_1)
            && matches!(eval_type, spec::PySparkUdfType::ArrowTable)
        {
            let schema_json = build_input_types_json(input_types)?;
            data.extend((schema_json.len() as i32).to_be_bytes());
            data.extend(schema_json.as_bytes());
        }

        // PySpark 4.1+ reads table argument offsets for ArrowUdtf before the arg offsets.
        // PySpark 4.0.x does not use the ArrowUdtf eval type.
        if matches!(pyspark_version, PySparkVersion::V4_1)
            && matches!(eval_type, spec::PySparkUdfType::ArrowUdtf)
        {
            data.extend(0i32.to_be_bytes()); // num_table_arg_offsets = 0
        }

        let num_args: i32 = num_args
            .try_into()
            .map_err(|e| PyUdfError::invalid(format!("num_args: {e}")))?;
        let allow_kwargs = pyspark_version.is_v4() && supports_kwargs(eval_type);
        data.extend(num_args.to_be_bytes()); // number of arguments
        for index in 0..num_args {
            data.extend(index.to_be_bytes()); // argument offset
            if allow_kwargs {
                write_kwarg(&mut data, kwargs, index as usize);
            }
        }

        if pyspark_version.is_v4() {
            data.extend(0i32.to_be_bytes()); // number of partition child indexes
            data.extend(0u8.to_be_bytes()); // pickled analyze result is not present
        }

        data.extend((command.len() as i32).to_be_bytes()); // length of the function
        data.extend_from_slice(command);

        let type_string = Python::attach(|py| -> PyResult<String> {
            let return_type = return_type.to_pyarrow(py)?;
            PyModule::import(py, intern!(py, "pyspark.sql.pandas.types"))?
                .getattr(intern!(py, "from_arrow_type"))?
                .call1((return_type,))?
                .getattr(intern!(py, "json"))?
                .call0()?
                .extract()
        })?;
        data.extend((type_string.len() as u32).to_be_bytes());
        data.extend(type_string.as_bytes());

        if pyspark_version.is_v4() {
            // TODO: support UDTF name
            data.extend(0u32.to_be_bytes()); // length of UDTF name
        }

        Ok(data)
    }
}
