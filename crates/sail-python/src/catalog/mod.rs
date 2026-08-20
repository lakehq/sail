use pyo3::create_exception;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use sail_catalog::error::{CatalogError, CatalogObject};

mod glue;
mod hms;
mod iceberg;
mod provider;
mod status;

create_exception!(_catalog, DatabaseNotFoundError, PyKeyError);
create_exception!(_catalog, TableNotFoundError, PyKeyError);
create_exception!(_catalog, ViewNotFoundError, PyKeyError);

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_catalog")?;
    module.add_class::<provider::PyCatalogProvider>()?;
    module.add_class::<status::PyDatabaseStatus>()?;
    module.add_class::<status::PyTableStatus>()?;
    module.add_class::<status::PyColumnStatus>()?;
    module.add(
        "DatabaseNotFoundError",
        parent.py().get_type::<DatabaseNotFoundError>(),
    )?;
    module.add(
        "TableNotFoundError",
        parent.py().get_type::<TableNotFoundError>(),
    )?;
    module.add(
        "ViewNotFoundError",
        parent.py().get_type::<ViewNotFoundError>(),
    )?;
    glue::register_module(&module)?;
    hms::register_module(&module)?;
    iceberg::register_module(&module)?;
    parent.add_submodule(&module)?;
    Ok(())
}

pub(super) fn to_py_error(error: CatalogError) -> PyErr {
    match error {
        CatalogError::NotFound(
            CatalogObject::Database | CatalogObject::Schema | CatalogObject::Namespace,
            name,
        ) => DatabaseNotFoundError::new_err(name),
        CatalogError::NotFound(CatalogObject::Table, name) => TableNotFoundError::new_err(name),
        CatalogError::NotFound(CatalogObject::View, name) => ViewNotFoundError::new_err(name),
        CatalogError::InvalidArgument(message) => PyValueError::new_err(message),
        error => PyRuntimeError::new_err(error.to_string()),
    }
}
