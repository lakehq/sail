use std::sync::Arc;

use pyo3::prelude::*;
use sail_data_source::default_registry;
use sail_python_datasource::PythonDataSourceFormat;

/// Register a Python data source format with the global registry.
///
/// Args:
///     name: The format name (e.g., "jdbc", "mongodb")
///     module: The Python module containing the data source class (e.g., "pysail.jdbc.datasource")
///     class_name: The Python class name implementing the data source interface (e.g., "JDBCArrowDataSource")
///
/// Example:
///     >>> from pysail._native.datasource import register_python_format
///     >>> register_python_format("jdbc", "pysail.jdbc.datasource", "JDBCArrowDataSource")
#[pyfunction]
fn register_python_format(name: String, module: String, class_name: String) -> PyResult<()> {
    let format = PythonDataSourceFormat::with_name_and_defaults(name, module, class_name);
    default_registry().register_format(Arc::new(format));
    Ok(())
}

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "datasource")?;
    module.add_function(wrap_pyfunction!(register_python_format, &module)?)?;
    parent.add_submodule(&module)?;
    Ok(())
}
