//! Python bindings for PySail.
//!
//! This module allows Python to interact with the Sail computation engine
//! by binding the Rust functions and types to Python.
mod cli;
mod flight;
mod globals;
mod spark;

use pyo3::prelude::*;

// A `#[global_allocator]` only takes effect in the final linked artifact that compiles it in.
// The mimalloc static in `sail-cli` lives in its bin target (`main.rs`), so this
// cdylib does not inherit it (linking `sail-cli` as a lib causes no duplicate-allocator conflict).
// Without the static below, Rust code in the Python wheel falls back to the system allocator.
// This swaps the allocator for Rust allocations only; CPython's own allocator is untouched.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Creates the `_native` Python module.
/// Registers the version constant, the `main` function,
/// and various submodules.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    flight::register_module(m)?;
    spark::register_module(m)?;
    m.add_function(wrap_pyfunction!(cli::main, m)?)?;
    m.add("_SAIL_VERSION", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
