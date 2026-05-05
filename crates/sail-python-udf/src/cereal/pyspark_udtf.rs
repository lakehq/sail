use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion_common::ScalarValue;
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

    /// Calls the Python UDTF's `analyze` static method to determine the return type dynamically.
    ///
    /// This is used when the UDTF does not have a fixed return type annotation and instead
    /// uses an `analyze` static method to determine the schema based on the argument types.
    ///
    /// # Arguments
    /// * `python_version` - The Python version used to compile the UDTF.
    /// * `command` - The CloudPickle-serialized UDTF class bytes.
    /// * `eval_type` - The evaluation type of the UDTF.
    /// * `argument_types` - The Arrow data types for each argument.
    /// * `argument_literals` - For each argument, `Some(sv)` if it's a constant expression
    ///   with value `sv` (which may be null), and `None` if it's not a constant expression.
    /// * `kwargs` - The keyword argument names for each argument (None for positional).
    pub fn analyze(
        python_version: &str,
        command: &[u8],
        eval_type: spec::PySparkUdfType,
        argument_types: &[DataType],
        argument_literals: &[Option<ScalarValue>],
        kwargs: &[Option<String>],
    ) -> PyUdfResult<DataType> {
        check_python_udf_version(python_version)?;
        let _ = eval_type; // eval_type is not needed for the analyze call itself

        Python::attach(|py| -> PyUdfResult<DataType> {
            // Load the UDTF handler by directly unpickling the command bytes.
            // The command is a CloudPickle-serialized UDTF class (compatible with pickle.loads).
            let cloudpickle = PyModule::import(py, intern!(py, "pyspark.cloudpickle"))
                .map_err(PyUdfError::PythonError)?;
            let handler = cloudpickle
                .getattr(intern!(py, "loads"))
                .map_err(PyUdfError::PythonError)?
                .call1((command,))
                .map_err(PyUdfError::PythonError)?;

            // Build the list of arguments: (arrow_type, is_constant, value_array, kwarg_name, is_table)
            let mut arguments: Vec<Bound<'_, PyAny>> = Vec::with_capacity(argument_types.len());
            for (i, dt) in argument_types.iter().enumerate() {
                let arrow_type = dt.to_pyarrow(py).map_err(PyUdfError::PythonError)?;
                let (is_constant, value_array) = match argument_literals.get(i) {
                    Some(Some(sv)) => {
                        // Constant expression: create a single-element PyArrow array.
                        // The value may be null (sv.is_null()) in which case the Python value
                        // will be None, representing a null literal constant expression.
                        let array = sv
                            .to_array()
                            .map_err(|e| PyValueError::new_err(e.to_string()))
                            .map_err(PyUdfError::PythonError)?;
                        let pyarrow_array = array
                            .to_data()
                            .to_pyarrow(py)
                            .map_err(PyUdfError::PythonError)?;
                        (true, Some(pyarrow_array))
                    }
                    // None (out of bounds) or Some(None) (not a constant expression)
                    _ => (false, None),
                };
                let kwarg_name: Option<&str> = kwargs.get(i).and_then(|k| k.as_deref());
                let is_table = false; // TABLE arguments are not yet supported
                let tuple = (arrow_type, is_constant, value_array, kwarg_name, is_table)
                    .into_pyobject(py)
                    .map_err(PyUdfError::PythonError)?;
                arguments.push(tuple.into_any());
            }

            let py_arguments = arguments
                .into_pyobject(py)
                .map_err(PyUdfError::PythonError)?;

            // Import and call the analyze_udtf helper from spark.py.
            // Any Python exception raised by the UDTF's analyze() method is converted to an
            // AnalysisError so that it surfaces as an AnalysisException on the client side.
            use crate::python::spark::PySpark;
            let schema_pyarrow = PySpark::analyze_udtf(py, handler, py_arguments)
                .map_err(|e| PyUdfError::AnalysisError(e.to_string()))?;

            // Convert PyArrow schema back to Arrow schema
            let schema =
                Schema::from_pyarrow_bound(&schema_pyarrow).map_err(PyUdfError::PythonError)?;
            Ok(DataType::Struct(schema.fields().clone()))
        })
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
