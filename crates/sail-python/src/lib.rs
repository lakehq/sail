//! Python bindings for PySail.
//!
//! This module allows Python to interact with the Sail computation engine
//! by binding the Rust functions and types to Python.
mod cli;
mod globals;
mod spark;

use log::debug;
use pyo3::prelude::*;

/// Performs shutdown of telemetry.
/// Logs, metrics, and traces are flushed if the exporters are configured.
///
/// This function will be called automatically when the Python interpreter exits.
/// This function is not intended to be called directly by users.
#[pyfunction]
fn _shutdown_telemetry() {
    sail_telemetry::telemetry::shutdown_telemetry();
}

/// Creates the `_native` Python module.
/// Registers the version constant, the `main` function,
/// and various submodules.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    globals::GlobalState::initialize()?;

    let atexit = PyModule::import(m.py(), "atexit")?;
    atexit.call_method1("register", (wrap_pyfunction!(_shutdown_telemetry, m)?,))?;

    spark::register_module(m)?;
    m.add_function(wrap_pyfunction!(cli::main, m)?)?;
    m.add("_SAIL_VERSION", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
