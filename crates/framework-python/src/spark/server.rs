use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{mem, thread};

use framework_spark_connect::entrypoint::serve;
use framework_telemetry::telemetry::init_telemetry;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::{Receiver, Sender};

pub(super) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = PyModule::new_bound(parent.py(), "server")?;
    module.add_class::<SparkConnectServer>()?;
    parent.add_submodule(&module)?;
    Ok(())
}

const SPARK_CONNECT_STACK_SIZE: usize = 1024 * 1024 * 8;

struct SparkConnectServerState {
    address: SocketAddr,
    handle: JoinHandle<PyResult<()>>,
    shutdown: Sender<()>,
}

impl SparkConnectServerState {
    /// Waits for the server to stop. If `shutdown` is `true`, sends a shutdown signal to the server
    /// before waiting.
    /// This method should be called within [Python::allow_threads]. Otherwise, the GIL is not
    /// released, and Python UDFs will be blocked when the server handles client requests.
    fn wait(self, shutdown: bool) -> PyResult<()> {
        if shutdown {
            self.shutdown.send(()).map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!(
                    "failed to send the shutdown signal: {:?}",
                    e
                ))
            })?;
        }
        self.handle.join().map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to join the server thread: {:?}", e))
        })?
    }
}

#[pyclass]
struct SparkConnectServer {
    #[pyo3(get)]
    ip: String,
    #[pyo3(get)]
    port: u16,
    runtime: Arc<Runtime>,
    state: Option<SparkConnectServerState>,
}

#[pymethods]
impl SparkConnectServer {
    #[new]
    #[pyo3(signature = (ip="0.0.0.0", port=50051))]
    fn new(ip: &str, port: u16) -> PyResult<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            // FIXME: make the stack size configurable
            .thread_stack_size(SPARK_CONNECT_STACK_SIZE)
            .enable_all()
            .build()?;
        Ok(Self {
            ip: ip.to_string(),
            port,
            state: None,
            runtime: Arc::new(runtime),
        })
    }

    #[getter]
    fn running(&self) -> bool {
        self.state.is_some()
    }

    #[getter]
    fn listening_address(&self) -> PyResult<Option<(String, u16)>> {
        match &self.state {
            Some(state) => Ok(Some((state.address.ip().to_string(), state.address.port()))),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (*, background=false))]
    fn start(&mut self, py: Python<'_>, background: bool) -> PyResult<()> {
        if self.state.is_some() {
            return Err(PyErr::new::<PyRuntimeError, _>(
                "the server is already started",
            ));
        }
        let ip = self.ip.parse().map_err(|_| {
            PyErr::new::<PyValueError, _>(format!("invalid IP address: {}", self.ip))
        })?;
        let address = SocketAddr::new(ip, self.port);
        let listener = self.runtime.block_on(TcpListener::bind(address))?;
        // TODO: configure Python logging to work with OpenTelemetry
        let with_telemetry = !background;
        self.state = Some(self.run(listener, with_telemetry)?);
        if !background {
            let state = self.state()?;
            py.allow_threads(move || state.wait(false))?;
        }
        Ok(())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        let state = self.state()?;
        py.allow_threads(move || state.wait(true))?;
        Ok(())
    }
}

impl SparkConnectServer {
    fn state(&mut self) -> PyResult<SparkConnectServerState> {
        match mem::take(&mut self.state) {
            None => Err(PyErr::new::<PyRuntimeError, _>(
                "the server is not started yet",
            )),
            Some(state) => Ok(state),
        }
    }

    async fn shutdown(rx: Receiver<()>) {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { }
            _ = rx => { }
        }
    }

    fn run_blocking(
        runtime: Arc<Runtime>,
        listener: TcpListener,
        rx: Receiver<()>,
        with_telemetry: bool,
    ) -> PyResult<()> {
        runtime
            .block_on(async {
                if with_telemetry {
                    // FIXME: This affects the global telemetry configuration.
                    init_telemetry()?;
                }
                serve(listener, Some(Self::shutdown(rx))).await
            })
            .map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!(
                    "failed to run the Spark Connect server: {:?}",
                    e
                ))
            })?;
        Ok(())
    }

    fn run(
        &self,
        listener: TcpListener,
        with_telemetry: bool,
    ) -> PyResult<SparkConnectServerState> {
        // Get the actual listener address.
        // A port is assigned by the OS if the port is 0 when creating the listener.
        let address = listener.local_addr()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let runtime = Arc::clone(&self.runtime);
        let handle = thread::Builder::new()
            .spawn(move || Self::run_blocking(runtime, listener, rx, with_telemetry))?;
        Ok(SparkConnectServerState {
            address,
            handle,
            shutdown: tx,
        })
    }
}
