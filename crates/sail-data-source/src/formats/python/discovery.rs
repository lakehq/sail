//! Discovery system for Python datasources.
//!
//! This module provides:
//! - Entry point discovery via `importlib.metadata.entry_points()`
//! - Thread-safe registry with `DashMap`
//! - Datasource validation for security
//!
//! Entry points are registered under the group `sail.datasources`.
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use datafusion_common::{exec_err, Result};
use once_cell::sync::Lazy;
use pyo3::types::PyAnyMethods;

use super::error::py_err;

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

    /// Get entry for atomic check-and-insert operations.
    ///
    /// Returns a DashMap Entry that allows atomic insertion if the key is absent.
    pub fn entry(&self, name: &str) -> Entry<'_, String, DataSourceEntry> {
        self.entries.entry(name.to_string())
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
pub fn discover_datasources() -> Result<usize> {
    pyo3::Python::attach(|py| {
        let count = discover_from_entry_points(py).unwrap_or(0);
        let registry_count = discover_from_python_registry(py)?;
        Ok(count + registry_count)
    })
}

/// Discover datasources from Python entry points.
///
/// Returns the number of successfully registered datasources, or 0 if
/// the entry points module is not available.
fn discover_from_entry_points(py: pyo3::Python<'_>) -> Option<usize> {
    let base_module = py.import("pysail.spark.datasource.base").ok()?;
    let discover_fn = base_module.getattr("discover_entry_points").ok()?;
    let entries = discover_fn.call0().ok()?;
    let iter = entries.try_iter().ok()?;

    let mut count = 0;
    for entry in iter.flatten() {
        if try_register_entry(py, entry, "entry_point") {
            count += 1;
        }
    }
    Some(count)
}

/// Discover datasources from the Python-side registry.
///
/// This finds datasources registered via the `@register` decorator in Python.
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

    let items = match registry.call_method0("items") {
        Ok(i) => i,
        Err(_) => return Ok(0),
    };

    let items_iter = match items.try_iter() {
        Ok(i) => i,
        Err(_) => return Ok(0),
    };

    let mut count = 0;
    for item in items_iter.flatten() {
        if try_register_entry(py, item, "registry") {
            count += 1;
        }
    }

    Ok(count)
}

/// Try to register a single datasource entry.
///
/// Extracts the (name, class) tuple, validates the class, pickles it,
/// and registers it atomically. Returns true on success.
fn try_register_entry(
    py: pyo3::Python<'_>,
    entry: pyo3::Bound<'_, pyo3::PyAny>,
    source: &str,
) -> bool {
    // Extract (name, class) tuple
    let Ok((name, cls)) = entry.extract::<(String, pyo3::Bound<'_, pyo3::PyAny>)>() else {
        return false;
    };

    // Use entry API for atomic check-and-insert (fixes TOCTOU)
    let Entry::Vacant(vacant) = DATASOURCE_REGISTRY.entry(&name) else {
        return false; // Already registered
    };

    // Validate the datasource class
    if validate_datasource_class(py, &cls).is_err() {
        return false;
    }

    // Pickle the class
    let Ok(pickled) = pickle_class(py, &cls) else {
        return false;
    };

    // Get module path for debugging
    let module_path = cls
        .getattr("__module__")
        .and_then(|m| m.extract::<String>())
        .unwrap_or_default();

    // Register the entry
    vacant.insert(DataSourceEntry {
        name: name.clone(),
        pickled_class: pickled,
        module_path,
    });

    log::info!("Discovered datasource from {}: {}", source, name);
    true
}

/// Validate that a Python class is a valid datasource.
///
/// Checks for required methods: `name`, `schema`, `reader`.
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
    let builtins = py.import("builtins").map_err(py_err)?;

    let callable = builtins.getattr("callable").map_err(py_err)?;

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
fn pickle_class(py: pyo3::Python<'_>, cls: &pyo3::Bound<'_, pyo3::PyAny>) -> Result<Vec<u8>> {
    let cloudpickle = super::error::import_cloudpickle(py)?;

    let pickled = cloudpickle.call_method1("dumps", (cls,)).map_err(py_err)?;

    pickled.extract::<Vec<u8>>().map_err(py_err)
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
