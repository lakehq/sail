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

/// The result of invoking a UDTF's `analyze` static method.
pub struct UdtfAnalyzeResult {
    /// The resolved return type (a struct type).
    pub return_type: DataType,
    /// The pickled `AnalyzeResult` object.
    pub pickled_analyze_result: Vec<u8>,
}

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

    /// Invokes the Python UDTF's `analyze` static method to determine the return type.
    ///
    /// This is called when the UDTF was defined without a fixed return type
    /// (i.e., it has an `analyze` static method that dynamically determines the schema).
    ///
    /// Returns the resolved return type (as an Arrow DataType) and the pickled AnalyzeResult.
    pub fn analyze(
        command: &[u8],
        input_types: &[DataType],
        kwargs: &[Option<String>],
    ) -> PyUdfResult<UdtfAnalyzeResult> {
        Python::attach(|py| -> PyResult<UdtfAnalyzeResult> {
            let pickle_ser = PyModule::import(py, intern!(py, "pyspark.serializers"))?
                .getattr(intern!(py, "CPickleSerializer"))?
                .call0()?;

            // Deserialize the UDTF handler class from the command bytes.
            let infile = PyModule::import(py, intern!(py, "io"))?
                .getattr(intern!(py, "BytesIO"))?
                .call1((command,))?;
            let read_command_fn = PyModule::import(py, intern!(py, "pyspark.worker_util"))?
                .getattr(intern!(py, "read_command"))?;
            let handler = read_command_fn.call1((&pickle_ser, &infile))?;

            // Build AnalyzeArgument list from the input types.
            let analyze_arg_cls = PyModule::import(py, intern!(py, "pyspark.sql.udtf"))?
                .getattr(intern!(py, "AnalyzeArgument"))?;
            let types_module = PyModule::import(py, intern!(py, "pyspark.sql.pandas.types"))?;
            let from_arrow_type = types_module.getattr(intern!(py, "from_arrow_type"))?;

            let mut args: Vec<Bound<'_, PyAny>> = Vec::new();
            let mut kw_args: Vec<(String, Bound<'_, PyAny>)> = Vec::new();

            for (i, dt) in input_types.iter().enumerate() {
                let arrow_type = dt.to_pyarrow(py)?;
                let spark_type = from_arrow_type.call1((&arrow_type,))?;
                let kw = pyo3::types::PyDict::new(py);
                kw.set_item("dataType", &spark_type)?;
                kw.set_item("value", py.None())?;
                kw.set_item("isTable", false)?;
                kw.set_item("isConstantExpression", false)?;
                let arg = analyze_arg_cls.call((), Some(&kw))?;

                if let Some(name) = kwargs.get(i).and_then(|k| k.as_deref()) {
                    kw_args.push((name.to_string(), arg));
                } else {
                    args.push(arg);
                }
            }

            // Call handler.analyze(*args, **kwargs)
            let analyze_method = handler.getattr(intern!(py, "analyze"))?;
            let py_args = pyo3::types::PyTuple::new(py, &args)?;
            let py_kwargs = pyo3::types::PyDict::new(py);
            for (name, arg) in &kw_args {
                py_kwargs.set_item(name, arg)?;
            }
            let result = analyze_method.call(&py_args, Some(&py_kwargs))?;

            // Extract the schema from the AnalyzeResult
            let schema = result.getattr(intern!(py, "schema"))?;
            let schema_json: String = schema.getattr(intern!(py, "json"))?.call0()?.extract()?;

            // Convert the schema JSON to an Arrow DataType
            let parse_datatype = PyModule::import(py, intern!(py, "pyspark.sql.types"))?
                .getattr(intern!(py, "_parse_datatype_json_string"))?;
            let spark_type = parse_datatype.call1((&schema_json,))?;

            // Convert PySpark StructType to Arrow schema
            let to_arrow_type = types_module.getattr(intern!(py, "to_arrow_type"))?;
            let arrow_type = to_arrow_type.call1((&spark_type,))?;
            let return_type: DataType =
                arrow_pyarrow::FromPyArrow::from_pyarrow_bound(&arrow_type)?;

            // Pickle the AnalyzeResult
            let pickled: Vec<u8> = pickle_ser
                .getattr(intern!(py, "dumps"))?
                .call1((&result,))?
                .extract()?;

            Ok(UdtfAnalyzeResult {
                return_type,
                pickled_analyze_result: pickled,
            })
        })
        .map_err(PyUdfError::from)
    }

    #[expect(clippy::too_many_arguments)]
    pub fn build(
        python_version: &str,
        command: &[u8],
        eval_type: spec::PySparkUdfType,
        num_args: usize,
        input_types: &[DataType],
        kwargs: &[Option<String>],
        return_type: &DataType,
        config: &PySparkUdfConfig,
        pickled_analyze_result: Option<&[u8]>,
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
            if let Some(pickled) = pickled_analyze_result {
                data.extend(1u8.to_be_bytes()); // pickled analyze result is present
                data.extend((pickled.len() as i32).to_be_bytes()); // length of pickled data
                data.extend(pickled);
            } else {
                data.extend(0u8.to_be_bytes()); // pickled analyze result is not present
            }
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
