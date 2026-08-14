use pyo3::create_exception;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use sail_catalog::error::{CatalogError, CatalogObject};

mod hms;
mod status;

create_exception!(_hms, DatabaseNotFoundError, PyKeyError);
create_exception!(_hms, TableNotFoundError, PyKeyError);
create_exception!(_hms, ViewNotFoundError, PyKeyError);

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_hms")?;
    module.add_class::<hms::PyHmsCatalog>()?;
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
    parent.add_submodule(&module)?;
    Ok(())
}

pub(super) fn to_py_error(error: CatalogError) -> PyErr {
    match error {
        CatalogError::NotFound(CatalogObject::Database, name) => {
            DatabaseNotFoundError::new_err(name)
        }
        CatalogError::NotFound(CatalogObject::Table, name) => TableNotFoundError::new_err(name),
        CatalogError::NotFound(CatalogObject::View, name) => ViewNotFoundError::new_err(name),
        CatalogError::InvalidArgument(message) => PyValueError::new_err(message),
        error => PyRuntimeError::new_err(error.to_string()),
    }
}
