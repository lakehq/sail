//! Python bindings for PySail.
//!
//! This module allows Python to interact with the Sail computation engine
//! by binding the Rust functions and types to Python.
mod cli;
mod spark;

use pyo3::prelude::*;

/// Creates the `_native` module that binds Python to Rust.
/// Registers `spark` submodule and also, version constant
/// and also the `main` function.
///
/// # Arguments
///
/// * `m`: The module to register the bindings to.
///
/// # Returns
///
/// * `Ok(())` if the bindings are registered successfully.
/// * `Err(PyErr)` if the bindings are not registered successfully.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    spark::register_module(m)?;
    m.add_function(wrap_pyfunction!(cli::main, m)?)?;
    m.add("_SAIL_VERSION", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
