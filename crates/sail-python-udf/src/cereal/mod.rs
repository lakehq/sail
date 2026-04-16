use arrow_pyarrow::ToPyArrow;
use datafusion::arrow::datatypes::DataType;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{PyList, PyModule};
use pyo3::{intern, PyResult, Python};
use sail_common::spec;

use crate::error::{PyUdfError, PyUdfResult};

pub mod pyspark_udf;
pub mod pyspark_udtf;

enum PySparkVersion {
    V3,
    V4_0,
    V4_1,
}

impl PySparkVersion {
    fn is_v4(&self) -> bool {
        matches!(self, PySparkVersion::V4_0 | PySparkVersion::V4_1)
    }
}

fn get_pyspark_version() -> PyUdfResult<PySparkVersion> {
    use pyo3::prelude::PyAnyMethods;
    use pyo3::types::PyModule;

    Python::attach(|py| {
        let module = PyModule::import(py, "pyspark")?;
        let version: String = module.getattr("__version__")?.extract()?;
        if version.starts_with("3.") {
            Ok(PySparkVersion::V3)
        } else if version.starts_with("4.0.") {
            Ok(PySparkVersion::V4_0)
        } else if version.starts_with("4.") {
            Ok(PySparkVersion::V4_1)
        } else {
            Err(PyUdfError::invalid(format!(
                "unsupported PySpark version: {version}"
            )))
        }
    })
}

fn check_python_udf_version(version: &str) -> PyUdfResult<()> {
    let pyo3_version: String = Python::attach(|py| py.version().to_string());
    if pyo3_version.starts_with(version) {
        Ok(())
    } else {
        Err(PyUdfError::invalid(format!(
            "Python version used to compile the UDF ({version}) does not match the Python version at runtime ({pyo3_version})"
        )))
    }
}

fn supports_kwargs(eval_type: spec::PySparkUdfType) -> bool {
    use spec::PySparkUdfType;

    match eval_type {
        PySparkUdfType::None
        | PySparkUdfType::GroupedMapPandas
        | PySparkUdfType::GroupedMapArrow
        | PySparkUdfType::WindowAggPandas
        | PySparkUdfType::WindowAggArrow
        | PySparkUdfType::MapPandasIter
        | PySparkUdfType::CogroupedMapPandas
        | PySparkUdfType::CogroupedMapArrow
        | PySparkUdfType::MapArrowIter
        | PySparkUdfType::GroupedMapPandasWithState
        | PySparkUdfType::TransformWithStatePandas
        | PySparkUdfType::TransformWithStatePandasInitState
        | PySparkUdfType::TransformWithStatePythonRow
        | PySparkUdfType::TransformWithStatePythonRowInitState
        | PySparkUdfType::GroupedMapArrowIter
        | PySparkUdfType::GroupedMapPandasIter => false,
        PySparkUdfType::Table
        | PySparkUdfType::ArrowTable
        | PySparkUdfType::ArrowUdtf
        | PySparkUdfType::Batched
        | PySparkUdfType::ArrowBatched
        | PySparkUdfType::ScalarPandas
        | PySparkUdfType::GroupedAggPandas
        | PySparkUdfType::GroupedAggPandasIter
        | PySparkUdfType::ScalarPandasIter
        | PySparkUdfType::ScalarArrow
        | PySparkUdfType::ScalarArrowIter
        | PySparkUdfType::GroupedAggArrow
        | PySparkUdfType::GroupedAggArrowIter => true,
    }
}

/// Writes the keyword argument flag and name for a single argument into `data`.
/// If the argument at `index` is a keyword argument, writes `1u8` followed by
/// the length-prefixed UTF-8 name. Otherwise writes `0u8` (positional).
pub(crate) fn write_kwarg(data: &mut Vec<u8>, kwargs: &[Option<String>], index: usize) {
    if let Some(name) = kwargs.get(index).and_then(|k| k.as_deref()) {
        data.extend(1u8.to_be_bytes()); // keyword argument flag
        let name_bytes = name.as_bytes();
        data.extend((name_bytes.len() as i32).to_be_bytes());
        data.extend(name_bytes);
    } else {
        data.extend(0u8.to_be_bytes()); // positional argument flag
    }
}

fn should_write_config(eval_type: spec::PySparkUdfType) -> bool {
    use spec::PySparkUdfType;

    match eval_type {
        PySparkUdfType::None | PySparkUdfType::Batched | PySparkUdfType::Table => false,
        PySparkUdfType::ArrowBatched
        | PySparkUdfType::ScalarPandas
        | PySparkUdfType::GroupedMapPandas
        | PySparkUdfType::GroupedMapArrow
        | PySparkUdfType::GroupedAggPandas
        | PySparkUdfType::GroupedAggPandasIter
        | PySparkUdfType::WindowAggPandas
        | PySparkUdfType::WindowAggArrow
        | PySparkUdfType::ScalarPandasIter
        | PySparkUdfType::MapPandasIter
        | PySparkUdfType::CogroupedMapPandas
        | PySparkUdfType::CogroupedMapArrow
        | PySparkUdfType::MapArrowIter
        | PySparkUdfType::GroupedMapPandasWithState
        | PySparkUdfType::TransformWithStatePandas
        | PySparkUdfType::TransformWithStatePandasInitState
        | PySparkUdfType::TransformWithStatePythonRow
        | PySparkUdfType::TransformWithStatePythonRowInitState
        | PySparkUdfType::GroupedMapArrowIter
        | PySparkUdfType::GroupedMapPandasIter
        | PySparkUdfType::ScalarArrow
        | PySparkUdfType::ScalarArrowIter
        | PySparkUdfType::GroupedAggArrow
        | PySparkUdfType::GroupedAggArrowIter
        | PySparkUdfType::ArrowTable
        | PySparkUdfType::ArrowUdtf => true,
    }
}

/// Builds a JSON string representing a PySpark StructType schema from Arrow input types.
/// This is used by PySpark 4.x's `read_udfs` to deserialize input type information
/// for `SQL_ARROW_BATCHED_UDF`.
fn build_input_types_json(input_types: &[DataType]) -> PyUdfResult<String> {
    Python::attach(|py| -> PyResult<String> {
        let types_module = PyModule::import(py, intern!(py, "pyspark.sql.types"))?;
        let struct_type_cls = types_module.getattr(intern!(py, "StructType"))?;
        let struct_field_cls = types_module.getattr(intern!(py, "StructField"))?;
        let from_arrow_type = PyModule::import(py, intern!(py, "pyspark.sql.pandas.types"))?
            .getattr(intern!(py, "from_arrow_type"))?;

        let fields: Vec<_> = input_types
            .iter()
            .enumerate()
            .map(|(i, dt)| -> PyResult<_> {
                let arrow_type = dt.to_pyarrow(py)?;
                let spark_type = from_arrow_type.call1((arrow_type,))?;
                struct_field_cls.call1((format!("_{i}"), spark_type, true))
            })
            .collect::<PyResult<_>>()?;

        let py_fields = PyList::new(py, &fields)?;
        let schema = struct_type_cls.call1((py_fields,))?;
        schema.getattr(intern!(py, "json"))?.call0()?.extract()
    })
    .map_err(PyUdfError::from)
}
