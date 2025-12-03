use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{mem, thread};

use log::info;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_spark_connect::entrypoint::{serve, SessionManagerOptions};
use sail_telemetry::telemetry::init_telemetry;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::oneshot::{Receiver, Sender};

struct SparkConnectServerState {
    address: SocketAddr,
    handle: JoinHandle<PyResult<()>>,
    shutdown: Sender<()>,
}

impl SparkConnectServerState {
    /// Waits for the server to stop. If `shutdown` is `true`, sends a shutdown signal to the server
    /// before waiting.
    /// This method should be called within [Python::detach]. Otherwise, the GIL is not
    /// released, and Python UDFs will be blocked when the server handles client requests.
    fn wait(self, shutdown: bool) -> PyResult<()> {
        if shutdown {
            let _ = self.shutdown.send(());
        }
        self.handle.join().map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to join the server thread: {e:?}"))
        })??;
        info!("The Spark Connect server has stopped.");
        Ok(())
    }
}

#[pyclass]
pub(super) struct SparkConnectServer {
    #[pyo3(get)]
    ip: String,
    #[pyo3(get)]
    port: u16,
    config: Arc<AppConfig>,
    runtime: RuntimeManager,
    state: Option<SparkConnectServerState>,
}

#[pymethods]
impl SparkConnectServer {
    #[new]
    #[pyo3(signature = (ip, port, /))]
    fn new(ip: &str, port: u16) -> PyResult<Self> {
        let config = AppConfig::load().map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to load the application config: {e}"))
        })?;
        let runtime = RuntimeManager::try_new(&config.runtime).map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to create the runtime: {e}"))
        })?;
        Ok(Self {
            ip: ip.to_string(),
            port,
            state: None,
            config: Arc::new(config),
            runtime,
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

    #[pyo3(signature = (*, background))]
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
        let handle = self.runtime.handle();
        let listener = handle.primary().block_on(TcpListener::bind(address))?;
        self.state = Some(self.run(listener)?);
        if !background {
            let state = self.state()?;
            py.detach(move || state.wait(false))?;
        }
        Ok(())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        let state = self.state()?;
        py.detach(move || state.wait(true))?;
        Ok(())
    }

    fn init_telemetry(&self) -> PyResult<()> {
        // TODO: configure Python logging to work with OpenTelemetry
        // FIXME: avoid affecting the global telemetry configuration
        let handle = self.runtime.handle();
        handle
            .primary()
            .block_on(async { init_telemetry(&self.config.telemetry) })
            .map_err(|e| PyErr::new::<PyRuntimeError, _>(format!("{e:?}")))
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
        info!("Shutting down the Spark Connect server...");
    }

    fn run_blocking(
        handle: Handle,
        options: SessionManagerOptions,
        listener: TcpListener,
        rx: Receiver<()>,
    ) -> PyResult<()> {
        handle
            .block_on(async { serve(listener, Self::shutdown(rx), options).await })
            .map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!(
                    "failed to run the Spark Connect server: {e:?}"
                ))
            })?;
        Ok(())
    }

    fn run(&self, listener: TcpListener) -> PyResult<SparkConnectServerState> {
        let options = SessionManagerOptions {
            config: Arc::clone(&self.config),
            runtime: self.runtime.handle(),
        };
        // Get the actual listener address.
        // A port is assigned by the OS if the port is 0 when creating the listener.
        let address = listener.local_addr()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = self.runtime.handle();
        info!("Starting the Spark Connect server on {address}...");
        let handle = thread::Builder::new()
            .spawn(move || Self::run_blocking(handle.primary().clone(), options, listener, rx))?;
        Ok(SparkConnectServerState {
            address,
            handle,
            shutdown: tx,
        })
    }
}
