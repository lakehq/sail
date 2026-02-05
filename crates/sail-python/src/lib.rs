//! Python bindings for PySail.
//!
//! This module allows Python to interact with the Sail computation engine
//! by binding the Rust functions and types to Python.
mod cli;
mod globals;
mod spark;

use pyo3::prelude::*;

/// Creates the `_native` Python module.
/// Registers the version constant, the `main` function,
/// and various submodules.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    spark::register_module(m)?;
    m.add_function(wrap_pyfunction!(cli::main, m)?)?;
    m.add_function(wrap_pyfunction!(initialize, m)?)?;
    m.add("_SAIL_VERSION", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

/// Initializes the native module global state eagerly.
///
/// The native module can still function properly if this function is not called,
/// since the global state supports lazy initialization on first access.
/// But calling this function makes it easier to reason about the environment variables
/// used to load the configuration.
///
/// This function is idempotent on success and can be called multiple times.
///
/// This function should not be called when using `cli::main`, since the CLI
/// entrypoint initializes telemetry on its own and conflicts with the global state
/// that is supposed to be used only when using the library programmatically.
#[pyfunction]
fn initialize(py: Python<'_>) -> PyResult<()> {
    let _ = globals::GlobalState::instance(py)?;
    Ok(())
}
