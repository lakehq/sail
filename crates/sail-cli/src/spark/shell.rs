use std::ffi::CString;
use std::net::Ipv4Addr;

use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::{PyResult, Python};
use sail_spark_connect::entrypoint::serve;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const SHELL_SOURCE_CODE: &str = include_str!("shell.py");

pub fn run_pyspark_shell() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let (_tx, rx) = oneshot::channel::<()>();
    let (server_port, server_task) = runtime.block_on(async move {
        // Listen on only the loopback interface for security.
        let listener = TcpListener::bind((Ipv4Addr::new(127, 0, 0, 1), 0)).await?;
        let port = listener.local_addr()?.port();
        // We do not capture SIGINT for the server since the user may enter Ctrl+C when
        // interacting with the Python interpreter.
        // The server will be terminated when the Python interpreter exits.
        let shutdown = async {
            // Wait on a channel that will never be sent to.
            let _ = rx.await;
        };
        let task = async {
            let _ = serve(listener, shutdown).await;
        };
        <Result<_, Box<dyn std::error::Error>>>::Ok((port, task))
    })?;
    runtime.spawn(server_task);
    Python::with_gil(|py| -> PyResult<_> {
        let shell = PyModule::from_code(
            py,
            CString::new(SHELL_SOURCE_CODE)?.as_c_str(),
            CString::new("shell.py")?.as_c_str(),
            CString::new("shell")?.as_c_str(),
        )?;
        shell
            .getattr("run_pyspark_shell")?
            .call((server_port,), None)?;
        Ok(())
    })?;
    Ok(())
}
