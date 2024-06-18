use pyo3::prelude::*;

pub mod cereal;
mod pyarrow_scratch;
pub mod udf;

// We need to make sure that SIGINT is not captured by Python
// so that the Rust program can handle it.
// See also:
//   https://github.com/PyO3/pyo3/issues/3218
//   https://github.com/PyO3/pyo3/issues/2576
const PYTHON_INIT_SCRIPT: &str = r#"
import signal

signal.signal(signal.SIGINT, signal.SIG_DFL)
"#;

pub fn init_python() -> PyResult<()> {
    Python::with_gil(|py| py.run_bound(PYTHON_INIT_SCRIPT, None, None))
}
