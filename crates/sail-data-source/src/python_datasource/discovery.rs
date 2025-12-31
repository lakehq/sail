/// Discovery system for Python datasources.
///
/// This module provides:
/// - Entry point discovery via `importlib.metadata.entry_points()`
/// - Thread-safe registry with `DashMap`
/// - Datasource validation for security
///
/// Entry points are registered under the group `sail.datasources`.
use dashmap::DashMap;
use datafusion_common::{exec_err, DataFusionError, Result};
use once_cell::sync::Lazy;
#[cfg(feature = "python")]
use pyo3::types::PyAnyMethods;

/// Global registry for Python datasources.
///
/// Stores pickled datasource classes for GIL-free access.
/// Thread-safe via `DashMap`.
pub static DATASOURCE_REGISTRY: Lazy<PythonDataSourceRegistry> =
    Lazy::new(PythonDataSourceRegistry::new);

/// Registry entry for a discovered datasource.
#[derive(Debug, Clone)]
pub struct DataSourceEntry {
    /// Name of the datasource (from entry point or register() call)
    pub name: String,
    /// Pickled datasource class (for GIL-free storage)
    pub pickled_class: Vec<u8>,
    /// Module path for debugging
    pub module_path: String,
}

/// Thread-safe registry for Python datasources.
pub struct PythonDataSourceRegistry {
    /// Map from datasource name to entry
    entries: DashMap<String, DataSourceEntry>,
}

impl PythonDataSourceRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Register a datasource entry.
    pub fn register(&self, entry: DataSourceEntry) {
        self.entries.insert(entry.name.clone(), entry);
    }

    /// Get a datasource by name.
    pub fn get(&self, name: &str) -> Option<DataSourceEntry> {
        self.entries.get(name).map(|e| e.clone())
    }

    /// List all registered datasource names.
    pub fn list(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.key().clone()).collect()
    }

    /// Check if a datasource is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Clear all entries (useful for testing).
    pub fn clear(&self) {
        self.entries.clear();
    }
}

impl Default for PythonDataSourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Discover datasources from Python entry points.
///
/// Scans `sail.datasources` entry point group and registers found classes.
///
/// # Security Note
/// This uses cloudpickle to serialize datasource classes. Only load
/// datasources from trusted packages - cloudpickle can execute arbitrary code.
#[cfg(feature = "python")]
pub fn discover_datasources() -> Result<usize> {
    pyo3::Python::attach(|py| {
        // Import importlib.metadata
        let metadata = py.import("importlib.metadata").map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to import importlib.metadata: {}",
                e
            ))))
        })?;

        // Get entry points for sail.datasources group (Python 3.10+ API)
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("group", "sail.datasources").map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to set kwargs: {}",
                e
            ))))
        })?;

        let eps = metadata
            .call_method("entry_points", (), Some(&kwargs))
            .map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::other(format!(
                    "Failed to get entry_points: {}",
                    e
                ))))
            })?;

        log::debug!("Discovered entry points for sail.datasources group");

        let mut count = 0;

        // Iterate over entry points
        if let Ok(eps_iter) = eps.try_iter() {
            for ep in eps_iter.flatten() {
                // Load the datasource class
                if let Ok(cls) = ep.call_method0("load") {
                    // Validate it's a proper datasource
                    if validate_datasource_class(py, &cls).is_ok() {
                        // Pickle the class for GIL-free storage
                        if let Ok(pickled) = pickle_class(py, &cls) {
                            let name = ep
                                .getattr("name")
                                .and_then(|n| n.extract::<String>())
                                .unwrap_or_else(|_| format!("unknown_{}", count));

                            let module_path = ep
                                .getattr("value")
                                .and_then(|v| v.extract::<String>())
                                .unwrap_or_default();

                            DATASOURCE_REGISTRY.register(DataSourceEntry {
                                name: name.clone(),
                                pickled_class: pickled,
                                module_path,
                            });

                            log::info!("Discovered datasource: {}", name);
                            count += 1;
                        }
                    }
                }
            }
        }

        // Also discover from the Python-side registry (from @register decorator)
        count += discover_from_python_registry(py)?;

        Ok(count)
    })
}

/// Discover datasources from the Python-side registry.
///
/// This finds datasources registered via the `@register` decorator in Python.
#[cfg(feature = "python")]
fn discover_from_python_registry(py: pyo3::Python<'_>) -> Result<usize> {
    // Try to import the datasource module
    let module = match py.import("pysail.spark.datasource") {
        Ok(m) => m,
        Err(_) => {
            // Module not available, try direct path
            match py.import("datasource") {
                Ok(m) => m,
                Err(_) => return Ok(0), // Neither import works, skip
            }
        }
    };

    // Get the _REGISTERED_DATASOURCES dict from base module
    let base_module = match module.getattr("base") {
        Ok(m) => m,
        Err(_) => {
            // Try getting from the module directly (if it re-exports)
            match module.getattr("_REGISTERED_DATASOURCES") {
                Ok(_) => module.clone().into_any(),
                Err(_) => return Ok(0),
            }
        }
    };

    let registry = match base_module.getattr("_REGISTERED_DATASOURCES") {
        Ok(r) => r,
        Err(_) => return Ok(0),
    };

    let mut count = 0;

    // Iterate over the registry dict
    if let Ok(items) = registry.call_method0("items") {
        if let Ok(items_iter) = items.try_iter() {
            for item in items_iter.flatten() {
                // Each item is (name, class)
                if let Ok((name, cls)) = item.extract::<(String, pyo3::Bound<'_, pyo3::PyAny>)>() {
                    // Skip if already registered
                    if DATASOURCE_REGISTRY.contains(&name) {
                        continue;
                    }

                    // Validate and pickle
                    if validate_datasource_class(py, &cls).is_ok() {
                        if let Ok(pickled) = pickle_class(py, &cls) {
                            let module_path = cls
                                .getattr("__module__")
                                .and_then(|m| m.extract::<String>())
                                .unwrap_or_default();

                            DATASOURCE_REGISTRY.register(DataSourceEntry {
                                name: name.clone(),
                                pickled_class: pickled,
                                module_path,
                            });

                            log::info!("Discovered datasource from registry: {}", name);
                            count += 1;
                        }
                    }
                }
            }
        }
    }

    Ok(count)
}

/// Validate that a Python class is a valid datasource.
///
/// Checks for required methods: `name`, `schema`, `reader`.
///
/// # Security
/// This validates the class structure, but cloudpickle can still execute
/// arbitrary code. Only use with trusted packages.
#[cfg(feature = "python")]
pub fn validate_datasource_class(
    py: pyo3::Python<'_>,
    cls: &pyo3::Bound<'_, pyo3::PyAny>,
) -> Result<()> {
    // Check required methods exist
    let required_methods = ["name", "schema", "reader"];

    for method in required_methods {
        if !cls.hasattr(method).unwrap_or(false) {
            return exec_err!(
                "Invalid datasource class: missing required method '{}'",
                method
            );
        }
    }

    // Verify it's callable (is a class)
    let builtins = py
        .import("builtins")
        .map_err(|e| DataFusionError::External(Box::new(std::io::Error::other(e.to_string()))))?;

    let callable = builtins
        .getattr("callable")
        .map_err(|e| DataFusionError::External(Box::new(std::io::Error::other(e.to_string()))))?;

    let is_callable = callable
        .call1((cls,))
        .and_then(|r| r.extract::<bool>())
        .unwrap_or(false);

    if !is_callable {
        return exec_err!("Invalid datasource: expected a class, got instance");
    }

    Ok(())
}

/// Validate a datasource instance has required methods.
#[cfg(feature = "python")]
#[allow(dead_code)]
pub fn validate_datasource_instance(
    _py: pyo3::Python<'_>,
    instance: &pyo3::Bound<'_, pyo3::PyAny>,
) -> Result<()> {
    let required_methods = ["name", "schema", "reader"];

    for method in required_methods {
        if !instance.hasattr(method).unwrap_or(false) {
            return exec_err!(
                "Invalid datasource instance: missing required method '{}'",
                method
            );
        }
    }

    Ok(())
}

/// Pickle a Python class for GIL-free storage.
#[cfg(feature = "python")]
fn pickle_class(py: pyo3::Python<'_>, cls: &pyo3::Bound<'_, pyo3::PyAny>) -> Result<Vec<u8>> {
    let cloudpickle = py.import("cloudpickle").map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to import cloudpickle: {}",
            e
        ))))
    })?;

    let pickled = cloudpickle.call_method1("dumps", (cls,)).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to pickle datasource class: {}",
            e
        ))))
    })?;

    pickled.extract::<Vec<u8>>().map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to extract pickled bytes: {}",
            e
        ))))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_operations() {
        let registry = PythonDataSourceRegistry::new();

        // Register an entry
        registry.register(DataSourceEntry {
            name: "test".to_string(),
            pickled_class: vec![1, 2, 3],
            module_path: "test.module:TestDataSource".to_string(),
        });

        // Get entry
        let entry = registry.get("test");
        assert!(entry.is_some());
        if let Some(e) = entry {
            assert_eq!(e.name, "test");
        }

        // List entries
        let names = registry.list();
        assert_eq!(names, vec!["test"]);

        // Check contains
        assert!(registry.contains("test"));
        assert!(!registry.contains("nonexistent"));

        // Clear
        registry.clear();
        assert!(!registry.contains("test"));
    }
}
