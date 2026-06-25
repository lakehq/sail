use std::path::{Component, Path, PathBuf};

use num_bigint::BigUint;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{pyclass, Python};
use sha2::{Digest, Sha256};

use crate::error::{PyUdfError, PyUdfResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PySparkPythonArtifact {
    pub name: String,
    pub python_path: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
#[pyclass(frozen, from_py_object)]
pub struct PySparkUdfConfig {
    #[pyo3(get)]
    pub session_timezone: String,
    #[pyo3(get, name = "window_bound_types")]
    pub pandas_window_bound_types: Option<String>,
    #[pyo3(get, name = "assign_columns_by_name")]
    pub pandas_grouped_map_assign_columns_by_name: bool,
    #[pyo3(get, name = "arrow_convert_safely")]
    pub pandas_convert_to_arrow_array_safely: bool,
    #[pyo3(get)]
    pub arrow_max_records_per_batch: usize,
    #[pyo3(get)]
    pub python_udf_pandas_conversion_enabled: bool,
    #[pyo3(get)]
    pub python_udtf_pandas_conversion_enabled: bool,
    #[pyo3(get)]
    pub python_udf_pandas_int_to_decimal_coercion_enabled: bool,
    #[pyo3(get)]
    pub binary_as_bytes: bool,
    #[pyo3(get)]
    pub python_artifact_paths: Vec<String>,
    pub python_artifacts: Vec<PySparkPythonArtifact>,
}

impl Default for PySparkUdfConfig {
    fn default() -> Self {
        Self {
            session_timezone: "UTC".to_string(),
            pandas_window_bound_types: None,
            pandas_grouped_map_assign_columns_by_name: true,
            pandas_convert_to_arrow_array_safely: false,
            arrow_max_records_per_batch: 10000,
            python_udf_pandas_conversion_enabled: false,
            python_udtf_pandas_conversion_enabled: false,
            python_udf_pandas_int_to_decimal_coercion_enabled: false,
            binary_as_bytes: true,
            python_artifact_paths: vec![],
            python_artifacts: vec![],
        }
    }
}

impl PySparkUdfConfig {
    pub fn install_python_artifacts(&self, py: Python) -> PyUdfResult<()> {
        if self.python_artifact_paths.is_empty() && self.python_artifacts.is_empty() {
            return Ok(());
        }
        let sys = PyModule::import(py, "sys")?;
        let path_list = sys.getattr("path")?;
        let paths = self.resolve_python_artifact_paths()?;
        for path in paths {
            let contains: bool = path_list
                .call_method1("__contains__", (path.as_str(),))?
                .extract()?;
            if !contains {
                path_list.call_method1("insert", (0, path.as_str()))?;
            }
        }
        PyModule::import(py, "importlib")?.call_method0("invalidate_caches")?;
        Ok(())
    }

    fn resolve_python_artifact_paths(&self) -> PyUdfResult<Vec<String>> {
        if self.python_artifacts.is_empty() {
            let mut paths = Vec::with_capacity(self.python_artifact_paths.len());
            for path in &self.python_artifact_paths {
                if !Path::new(path).exists() {
                    return Err(PyUdfError::invalid(format!(
                        "Python artifact path is not accessible in this worker: {path}"
                    )));
                }
                paths.push(path.clone());
            }
            return Ok(paths);
        }

        let mut paths = Vec::with_capacity(self.python_artifacts.len());
        for artifact in &self.python_artifacts {
            if !Path::new(&artifact.python_path).exists() {
                paths.push(materialize_python_artifact(&artifact.name, &artifact.data)?);
            } else {
                paths.push(artifact.python_path.clone());
            }
        }
        Ok(paths)
    }

    pub fn with_pandas_window_bound_types(mut self, value: Option<String>) -> Self {
        self.pandas_window_bound_types = value;
        self
    }

    /// Converts the configuration to a list of key-value pairs,
    /// so that it can be read by `worker.py` in PySpark.
    /// Missing values are not included.
    pub fn to_key_value_pairs(&self) -> Vec<(String, String)> {
        let mut out = vec![];
        out.push((
            "spark.sql.session.timeZone".to_string(),
            self.session_timezone.clone(),
        ));
        if let Some(value) = &self.pandas_window_bound_types {
            out.push(("pandas_window_bound_types".to_string(), value.clone()));
        }
        out.push((
            "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName".to_string(),
            self.pandas_grouped_map_assign_columns_by_name.to_string(),
        ));
        out.push((
            "spark.sql.execution.pandas.convertToArrowArraySafely".to_string(),
            self.pandas_convert_to_arrow_array_safely.to_string(),
        ));
        out.push((
            "spark.sql.execution.arrow.maxRecordsPerBatch".to_string(),
            self.arrow_max_records_per_batch.to_string(),
        ));
        out.push((
            "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled".to_string(),
            self.python_udf_pandas_conversion_enabled.to_string(),
        ));
        out.push((
            "spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled".to_string(),
            self.python_udtf_pandas_conversion_enabled.to_string(),
        ));
        out.push((
            "spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled".to_string(),
            self.python_udf_pandas_int_to_decimal_coercion_enabled
                .to_string(),
        ));
        out.push((
            "spark.sql.execution.pyspark.binaryAsBytes".to_string(),
            self.binary_as_bytes.to_string(),
        ));
        out
    }
}

fn materialize_python_artifact(name: &str, data: &[u8]) -> PyUdfResult<String> {
    let relative_path = validate_artifact_relative_path(name)?;
    let hash = BigUint::from_bytes_be(&Sha256::digest(data)).to_str_radix(36);
    let target_path = std::env::temp_dir()
        .join("sail-python-artifacts")
        .join(hash)
        .join(&relative_path);
    if !target_path.exists() {
        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&target_path, data)?;
    }
    python_artifact_import_path(name, &target_path)
}

fn validate_artifact_relative_path(path: &str) -> PyUdfResult<PathBuf> {
    let path = Path::new(path);
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(PyUdfError::invalid(format!(
                    "Python artifact name must be a relative path without '.' or '..': {}",
                    path.display()
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(PyUdfError::invalid(
            "Python artifact name must not be empty",
        ));
    }
    Ok(out)
}

fn python_artifact_import_path(name: &str, target_path: &Path) -> PyUdfResult<String> {
    let Some(file_name) = name.strip_prefix("pyfiles/") else {
        return Err(PyUdfError::invalid(format!(
            "Python artifact name must use the pyfiles/ prefix: {name}"
        )));
    };
    if file_name.ends_with(".py") {
        let dir = target_path.parent().ok_or_else(|| {
            PyUdfError::internal(format!(
                "Python artifact file has no parent directory: {}",
                target_path.display()
            ))
        })?;
        Ok(dir.to_string_lossy().into_owned())
    } else if file_name.ends_with(".zip")
        || file_name.ends_with(".egg")
        || file_name.ends_with(".jar")
    {
        Ok(target_path.to_string_lossy().into_owned())
    } else {
        Err(PyUdfError::invalid(format!(
            "unsupported Python artifact type: {file_name}"
        )))
    }
}
