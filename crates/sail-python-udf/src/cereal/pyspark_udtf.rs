use std::hash::{Hash, Hasher};

use arrow::pyarrow::ToPyArrow;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{plan_datafusion_err, plan_err, DataFusionError, Result};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use sail_common::config::SparkUdfConfig;

use crate::cereal::{is_pyspark_arrow_udf, PythonFunction};

#[derive(Debug, Clone)]
pub struct PySparkUdtfObject(pub PyObject);

impl PythonFunction for PySparkUdtfObject {
    fn load(v: &[u8]) -> Result<Self> {
        // build_pyspark_udtf_payload adds eval_type to the beginning of the payload
        let (eval_type_bytes, v) = v.split_at(std::mem::size_of::<i32>());
        let eval_type = i32::from_be_bytes(
            eval_type_bytes
                .try_into()
                .map_err(|e| plan_datafusion_err!("eval_type from_be_bytes: {e}"))?,
        );
        Python::with_gil(|py| {
            let infile: Bound<PyAny> = PyModule::import_bound(py, pyo3::intern!(py, "io"))
                .and_then(|io| io.getattr(pyo3::intern!(py, "BytesIO")))
                .and_then(|bytes_io| bytes_io.call1((v,)))
                .map_err(|e| DataFusionError::External(e.into()))?;
            let pickle_ser: Bound<PyAny> =
                PyModule::import_bound(py, pyo3::intern!(py, "pyspark.serializers"))
                    .and_then(|serializers| {
                        serializers.getattr(pyo3::intern!(py, "CPickleSerializer"))
                    })
                    .and_then(|serializer| serializer.call0())
                    .map_err(|e| DataFusionError::External(e.into()))?;
            PyModule::import_bound(py, pyo3::intern!(py, "pyspark.worker"))
                .and_then(|worker| worker.getattr(pyo3::intern!(py, "read_udtf")))
                .and_then(|read_udtf| read_udtf.call1((pickle_ser, infile, eval_type)))
                .map(|py_tuple| PySparkUdtfObject(py_tuple.to_object(py)))
                .map_err(|e| DataFusionError::External(e.into()))
        })
    }

    fn function<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        self.0
            .clone_ref(py)
            .into_bound(py)
            .get_item(0)
            .map_err(|err| DataFusionError::External(err.into()))
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
    eval_type: &i32,
    num_args: &i32,
    return_type: &DataType,
    spark_udf_config: &SparkUdfConfig,
) -> Result<PySparkUdtfObject> {
    let pyo3_python_version: String = Python::with_gil(|py| py.version().to_string());
    if !pyo3_python_version.starts_with(python_version) {
        return plan_err!(
            "Python version mismatch. Version used to compile the UDTF must match the version used to run the UDTF. Version used to compile the UDTF: {}. Version used to run the UDTF: {}",
            python_version,
            pyo3_python_version
        );
    }
    let data: Vec<u8> =
        build_pyspark_udtf_payload(command, eval_type, num_args, return_type, spark_udf_config);
    PySparkUdtfObject::load(&data)
}

pub fn build_pyspark_udtf_payload(
    command: &[u8],
    eval_type: &i32,
    num_args: &i32,
    return_type: &DataType,
    spark_udf_config: &SparkUdfConfig,
) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::new();
    data.extend(&eval_type.to_be_bytes()); // Add eval_type for extraction in visit_bytes
    if is_pyspark_arrow_udf(eval_type) {
        let configs = [
            &spark_udf_config.timezone,
            &spark_udf_config.pandas_convert_to_arrow_array_safely,
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
    data.extend(&num_args.to_be_bytes()); // num_args
    for index in 0..*num_args {
        data.extend(&index.to_be_bytes()); // arg_offsets
    }
    data.extend(&(command.len() as i32).to_be_bytes()); // len of the function
    data.extend_from_slice(command);

    Python::with_gil(|py| {
        let return_type: Bound<PyAny> = return_type
            .to_pyarrow(py)
            .unwrap()
            .clone_ref(py)
            .into_bound(py);
        let result: Bound<PyAny> =
            PyModule::import_bound(py, pyo3::intern!(py, "pyspark.sql.pandas.types"))
                .and_then(|types| types.getattr(pyo3::intern!(py, "from_arrow_type")))
                .and_then(|from_arrow_type| from_arrow_type.call1((return_type,)))
                .and_then(|result| result.getattr("json"))
                .and_then(|json_method| json_method.call0())
                .unwrap();
        let json_string: String = result.extract().expect("Failed to extract JSON string");
        let json_length = json_string.len();

        data.extend(&(json_length as u32).to_be_bytes());
        data.extend(json_string.as_bytes());
    });

    data
}
