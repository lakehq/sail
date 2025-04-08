use std::net::Ipv4Addr;
use std::sync::Arc;

use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_spark_connect::entrypoint::{serve, SessionManagerOptions};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::python::Modules;

pub fn run_pyspark_shell() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let runtime = RuntimeManager::try_new(&config.runtime)?;
    let options = SessionManagerOptions {
        config,
        runtime: runtime.handle(),
    };
    let (_tx, rx) = oneshot::channel::<()>();
    let handle = runtime.handle().primary().clone();
    let (server_port, server_task) = handle.block_on(async move {
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
            let _ = serve(listener, shutdown, options).await;
        };
        <Result<_, Box<dyn std::error::Error>>>::Ok((port, task))
    })?;
    handle.spawn(server_task);
    Python::with_gil(|py| -> PyResult<_> {
        let shell = Modules::SPARK_SHELL.load(py)?;
        shell
            .getattr("run_pyspark_shell")?
            .call((server_port,), None)?;
        Ok(())
    })?;
    Ok(())
}
