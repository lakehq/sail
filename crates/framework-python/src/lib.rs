use pyo3::prelude::*;

pub mod partial_python_udf;
pub mod pyarrow;
pub mod udf;
pub mod udtf;

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

#[macro_export]
macro_rules! impl_py_state_serialization {
    ($ty:ty) => {
        #[pymethods]
        impl $ty {
            pub fn __reduce__(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
                Ok((
                    Self::type_object_bound(py)
                        .getattr("_from_serialized")?
                        .to_object(py),
                    (PyBytes::new_bound(py, &bincode::serialize(&self).unwrap()).to_object(py),)
                        .to_object(py),
                ))
            }

            #[staticmethod]
            pub fn _from_serialized(py: Python, serialized: PyObject) -> PyResult<Self> {
                serialized
                    .extract::<&PyBytes>(py)
                    .map(|py_bytes| bincode::deserialize(Bytes::new(py_bytes.as_bytes())).unwrap())
            }
        }
    };
}
