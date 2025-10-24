// SPDX-License-Identifier: Apache-2.0

//! PythonDataSourceFormat - TableFormat implementation for generic Python data sources.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, Result as DFResult};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{DeleteInfo, SinkInfo, SourceInfo, TableFormat};

use crate::provider::PythonTableProvider;

/// Generic TableFormat for Python-based data sources.
///
/// This format allows users to implement data sources in Python and use them
/// with Lakesail's distributed query engine.
///
/// # Usage
///
/// ```python
/// spark.read.format("python") \
///     .option("python_module", "my_package.my_datasource") \
///     .option("python_class", "MyDataSource") \
///     .option("custom_option", "value") \
///     .load()
/// ```
///
/// Any Python class that implements these methods can be used:
/// - `infer_schema(options: dict) -> pa.Schema`
/// - `plan_partitions(options: dict) -> List[dict]`
/// - `read_partition(partition_spec: dict, options: dict) -> Iterator[pa.RecordBatch]`
pub struct PythonDataSourceFormat {
    /// Format name (e.g., "python", "jdbc")
    name: String,
    /// Default Python module (optional)
    default_module: Option<String>,
    /// Default Python class (optional)
    default_class: Option<String>,
}

impl PythonDataSourceFormat {
    pub fn new() -> Self {
        Self {
            name: "python".to_string(),
            default_module: None,
            default_class: None,
        }
    }

    /// Create a new format with a custom name and default module/class.
    ///
    /// This allows creating specialized formats (like "jdbc") that are just
    /// pre-configured instances of the generic Python data source format.
    pub fn with_name_and_defaults(
        name: impl Into<String>,
        module: impl Into<String>,
        class: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            default_module: Some(module.into()),
            default_class: Some(class.into()),
        }
    }
}

impl Default for PythonDataSourceFormat {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableFormat for PythonDataSourceFormat {
    fn name(&self) -> &str {
        &self.name
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> DFResult<Arc<dyn TableProvider>> {
        // Merge all option sets (later sets override earlier ones)
        let mut merged_options = std::collections::HashMap::new();
        for option_set in &info.options {
            merged_options.extend(option_set.clone());
        }

        // Extract module and class (use defaults if not provided)
        let module = merged_options
            .get("python_module")
            .cloned()
            .or_else(|| self.default_module.clone())
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Configuration(
                    "Missing required option 'python_module'".to_string(),
                )
            })?;

        let class = merged_options
            .get("python_class")
            .cloned()
            .or_else(|| self.default_class.clone())
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Configuration(
                    "Missing required option 'python_class'".to_string(),
                )
            })?;

        // Remove Python-specific options from what we pass to the datasource
        let mut datasource_options = merged_options.clone();
        datasource_options.remove("python_module");
        datasource_options.remove("python_class");

        // Create provider
        let provider = PythonTableProvider::try_new(module, class, datasource_options)?;

        Ok(Arc::new(provider))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Writing to Python datasources is not yet implemented")
    }

    async fn create_deleter(
        &self,
        _ctx: &dyn Session,
        _info: DeleteInfo,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Deleting from Python datasources is not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::CString;

    use datafusion::prelude::SessionContext;
    use datafusion_common::Constraints;
    use futures::executor::block_on;
    use pyo3::exceptions::PyValueError;
    use pyo3::types::{PyAnyMethods, PyModule};
    use pyo3::{PyResult, Python};
    use serde_json::json;

    use super::*;
    use crate::provider::PythonTableProvider;

    fn ensure_pyarrow_installed() -> bool {
        Python::with_gil(|py| py.import("pyarrow").is_ok())
    }

    fn register_test_module(py: Python<'_>) -> PyResult<()> {
        let code = CString::new(
            r#"
import pyarrow as pa

class TestDataSource:
    def infer_schema(self, options):
        return pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
        ])

    def plan_partitions(self, options):
        return [{"partition_id": 0, "predicate": None}]

    def read_partition(self, partition_spec, options):
        table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        return table.to_batches()
"#,
        )
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let filename = CString::new("test_python_datasource.py")
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let module_name = CString::new("test_python_datasource")
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let module = PyModule::from_code(
            py,
            code.as_c_str(),
            filename.as_c_str(),
            module_name.as_c_str(),
        )?;
        let sys = py.import("sys")?;
        sys.getattr("modules")?
            .set_item("test_python_datasource", module)?;
        Ok(())
    }

    #[test]
    fn test_python_format_uses_defaults_and_merges_options() -> std::result::Result<(), String> {
        pyo3::prepare_freethreaded_python();
        if !ensure_pyarrow_installed() {
            eprintln!("skipping python format tests: pyarrow not available");
            return Ok(());
        }

        Python::with_gil(register_test_module).map_err(|err| err.to_string())?;

        let format = PythonDataSourceFormat::with_name_and_defaults(
            "jdbc_test",
            "test_python_datasource",
            "TestDataSource",
        );
        let ctx = SessionContext::default();
        let state = ctx.state();

        let options1 = HashMap::from([("url".to_string(), "jdbc:ignored".to_string())]);
        let options2 = HashMap::from([
            ("url".to_string(), "jdbc:sqlite::memory:".to_string()),
            ("fetchsize".to_string(), "256".to_string()),
        ]);

        let info = SourceInfo {
            paths: vec![],
            schema: None,
            constraints: Constraints::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![options1, options2],
        };

        let provider =
            block_on(format.create_provider(&state, info)).map_err(|err| err.to_string())?;
        let python_provider = provider
            .as_any()
            .downcast_ref::<PythonTableProvider>()
            .ok_or_else(|| "expected PythonTableProvider".to_string())?;

        assert_eq!(python_provider.schema().fields().len(), 2);

        let exec = block_on(python_provider.scan(&state, None, &[], None))
            .map_err(|err| err.to_string())?;
        let python_exec = exec
            .as_any()
            .downcast_ref::<crate::exec::PythonExec>()
            .ok_or_else(|| "expected PythonExec".to_string())?;

        assert_eq!(python_exec.options()["url"], json!("jdbc:sqlite::memory:"));
        assert_eq!(python_exec.options()["fetchsize"], json!("256"));
        Ok(())
    }

    #[test]
    fn test_python_format_requires_module() -> std::result::Result<(), String> {
        pyo3::prepare_freethreaded_python();

        let format = PythonDataSourceFormat::new();
        let ctx = SessionContext::default();
        let state = ctx.state();

        let info = SourceInfo {
            paths: vec![],
            schema: None,
            constraints: Constraints::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![HashMap::new()],
        };

        let result = block_on(format.create_provider(&state, info));
        assert!(matches!(
            result,
            Err(datafusion::common::DataFusionError::Configuration(_))
        ));
        Ok(())
    }
}
