use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Builder;

#[pyfunction]
fn start_spark_connect_server(py: Python<'_>) -> PyResult<()> {
    // TODO: improve signal handling and support graceful shutdown
    py.allow_threads(|| {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|err| {
                PyErr::new::<PyRuntimeError, _>(format!("failed to start the runtime: {:?}", err))
            })?;
        runtime
            .block_on(async { framework_spark_connect::entrypoint::serve().await })
            .map_err(|err| {
                PyErr::new::<PyRuntimeError, _>(format!(
                    "failed to run the Spark Connect server: {:?}",
                    err
                ))
            })?;
        Ok(())
    })
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(start_spark_connect_server, m)?)?;
    Ok(())
}
