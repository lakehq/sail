use std::net::{IpAddr, Ipv4Addr};

use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use tokio::sync::oneshot;

use crate::python::Modules;
use crate::spark::server::with_spark_connect_server;

pub fn run_pyspark_shell() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = oneshot::channel::<()>();
    // Listen on only the loopback interface for security.
    let address = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    // We do not capture SIGINT for the server since the user may enter Ctrl+C when
    // interacting with the Python interpreter.
    // The server will be terminated when the Python interpreter exits.
    let shutdown = async {
        // The shutdown signal receiver will be notified when `tx` is dropped,
        // even if no value is sent through the channel.
        let _ = rx.await;
    };
    with_spark_connect_server(address, shutdown, |addr| async move {
        // Move `tx` to the async block so that it will be dropped after running the PySpark shell,
        // which will signal the server to shut down.
        // Note: `let _ = tx;` does not work!!!
        let _tx = tx;
        Python::attach(|py| -> PyResult<_> {
            let shell = Modules::SPARK_SHELL.load(py)?;
            shell
                .getattr("run_pyspark_shell")?
                .call((addr.port(),), None)?;
            Ok(())
        })?;
        Ok(())
    })?;
    Ok(())
}
