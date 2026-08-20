//! Python bindings for PySail.
//!
//! This module allows Python to interact with the Sail computation engine
//! by binding the Rust functions and types to Python.
mod catalog;
mod celeborn;
mod cli;
mod flight;
mod globals;
mod spark;

use pyo3::prelude::*;

/// Creates the `_native` Python module.
/// Registers the version constant, the `main` function,
/// and various submodules.
///
/// The module is declared free-threading-compatible (`gil_used = false`).
/// Without this declaration, importing `pysail` on a free-threaded (no-GIL)
/// CPython build would re-enable the GIL for the whole process. The assertion
/// is backed by an audit of the crates that touch Python (`sail-python`,
/// `sail-python-udf`, `sail-pyarrow`, and the Python data source support in
/// `sail-data-source`): all cached Python state — including the type caches
/// in `sail-pyarrow` and the PySpark version cache in the `sail-python-udf`
/// cereal module — goes through `PyOnceLock` or lock-based data structures
/// instead of relying on the GIL for mutual exclusion, and every
/// `Python::attach` site operates on per-call locals.
#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    celeborn::register_module(m)?;
    catalog::register_module(m)?;
    flight::register_module(m)?;
    spark::register_module(m)?;
    m.add_function(wrap_pyfunction!(cli::main, m)?)?;
    m.add("_SAIL_VERSION", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
