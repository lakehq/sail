use std::net::{IpAddr, Ipv4Addr};

use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use tokio::sync::oneshot;

use crate::python::Modules;
use crate::spark::server::with_spark_connect_server;

pub fn run_pyspark_script(file: String) -> Result<(), Box<dyn std::error::Error>> {
    // We follow the same setup as `run_pyspark_shell`.
    // Please refer to the comments in that function for details.
    let (tx, rx) = oneshot::channel::<()>();
    let address = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let shutdown = async {
        let _ = rx.await;
    };
    with_spark_connect_server(address, shutdown, |addr| async move {
        let _tx = tx;
        Python::attach(|py| -> PyResult<_> {
            let runner = Modules::SPARK_RUN.load(py)?;
            runner
                .getattr("run_pyspark_script")?
                .call1((addr.port(), file))?;
            Ok(())
        })?;
        Ok(())
    })?;
    Ok(())
}
