use pyo3::Python;
use sail_common::spec;

use crate::error::{PyUdfError, PyUdfResult};

pub mod pyspark_udf;
pub mod pyspark_udtf;

fn check_python_udf_version(version: &str) -> PyUdfResult<()> {
    let pyo3_version: String = Python::with_gil(|py| py.version().to_string());
    if pyo3_version.starts_with(version) {
        Ok(())
    } else {
        Err(PyUdfError::invalid(format!(
            "Python version used to compile the UDF ({}) does not match the Python version at runtime ({})",
            version,
            pyo3_version
        )))
    }
}

fn should_write_config(eval_type: spec::PySparkUdfType) -> bool {
    use spec::PySparkUdfType;

    match eval_type {
        PySparkUdfType::None | PySparkUdfType::Batched | PySparkUdfType::Table => false,
        PySparkUdfType::ArrowBatched
        | PySparkUdfType::ScalarPandas
        | PySparkUdfType::GroupedMapPandas
        | PySparkUdfType::GroupedAggPandas
        | PySparkUdfType::WindowAggPandas
        | PySparkUdfType::ScalarPandasIter
        | PySparkUdfType::MapPandasIter
        | PySparkUdfType::CogroupedMapPandas
        | PySparkUdfType::MapArrowIter
        | PySparkUdfType::GroupedMapPandasWithState
        | PySparkUdfType::ArrowTable => true,
    }
}
