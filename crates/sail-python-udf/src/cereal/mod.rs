use pyo3::Python;
use sail_common::spec;

use crate::error::{PyUdfError, PyUdfResult};

pub mod pyspark_udf;
pub mod pyspark_udtf;

enum PySparkVersion {
    V3,
    V4,
}

fn get_pyspark_version() -> PyUdfResult<PySparkVersion> {
    use pyo3::prelude::PyAnyMethods;
    use pyo3::types::PyModule;

    Python::attach(|py| {
        let module = PyModule::import(py, "pyspark")?;
        let version: String = module.getattr("__version__")?.extract()?;
        if version.starts_with("3.") {
            Ok(PySparkVersion::V3)
        } else if version.starts_with("4.") {
            Ok(PySparkVersion::V4)
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
        | PySparkUdfType::MapPandasIter
        | PySparkUdfType::CogroupedMapPandas
        | PySparkUdfType::CogroupedMapArrow
        | PySparkUdfType::MapArrowIter
        | PySparkUdfType::GroupedMapPandasWithState => false,
        PySparkUdfType::Table
        | PySparkUdfType::ArrowTable
        | PySparkUdfType::Batched
        | PySparkUdfType::ArrowBatched
        | PySparkUdfType::ScalarPandas
        | PySparkUdfType::GroupedAggPandas
        | PySparkUdfType::ScalarPandasIter => true,
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
        | PySparkUdfType::WindowAggPandas
        | PySparkUdfType::ScalarPandasIter
        | PySparkUdfType::MapPandasIter
        | PySparkUdfType::CogroupedMapPandas
        | PySparkUdfType::CogroupedMapArrow
        | PySparkUdfType::MapArrowIter
        | PySparkUdfType::GroupedMapPandasWithState
        | PySparkUdfType::ArrowTable => true,
    }
}

/// Writes the keyword argument flag and name for a single argument.
/// If the argument is a keyword argument, writes `1u8` followed by the
/// length-prefixed keyword name. Otherwise, writes `0u8`.
fn write_kwarg(data: &mut Vec<u8>, kwargs: &[Option<String>], index: usize) {
    if let Some(name) = kwargs.get(index).and_then(|k| k.as_deref()) {
        data.extend(1u8.to_be_bytes()); // keyword argument
        let name_bytes = name.as_bytes();
        data.extend((name_bytes.len() as i32).to_be_bytes());
        data.extend(name_bytes);
    } else {
        data.extend(0u8.to_be_bytes()); // not a keyword argument
    }
}
