use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{mem, thread};

use log::info;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_flight::serve;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::oneshot::{Receiver, Sender};

use crate::globals::GlobalState;

struct FlightSqlServerState {
    address: SocketAddr,
    handle: JoinHandle<PyResult<()>>,
    shutdown: Sender<()>,
}

impl FlightSqlServerState {
    fn wait(self, shutdown: bool) -> PyResult<()> {
        if shutdown {
            let _ = self.shutdown.send(());
        }
        self.handle.join().map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to join the server thread: {e:?}"))
        })??;
        info!("The Flight SQL server has stopped.");
        Ok(())
    }
}

#[pyclass]
pub(super) struct FlightSqlServer {
    #[pyo3(get)]
    ip: String,
    #[pyo3(get)]
    port: u16,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    state: Option<FlightSqlServerState>,
}

#[pymethods]
impl FlightSqlServer {
    #[new]
    #[pyo3(signature = (ip, port, /))]
    fn new(py: Python<'_>, ip: &str, port: u16) -> PyResult<Self> {
        let config = AppConfig::load().map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to load the application config: {e}"))
        })?;
        let globals = GlobalState::instance(py)?;
        let runtime = globals.runtime.handle();
        Ok(Self {
            ip: ip.to_string(),
            port,
            config: Arc::new(config),
            runtime,
            state: None,
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
        let listener = self
            .runtime
            .primary()
            .block_on(TcpListener::bind(address))?;
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
}

impl FlightSqlServer {
    fn state(&mut self) -> PyResult<FlightSqlServerState> {
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
        info!("Shutting down the Flight SQL server...");
    }

    fn run_blocking(
        handle: Handle,
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        listener: TcpListener,
        rx: Receiver<()>,
    ) -> PyResult<()> {
        handle
            .block_on(async { serve(listener, Self::shutdown(rx), config, runtime).await })
            .map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!(
                    "failed to run the Flight SQL server: {e:?}"
                ))
            })?;
        Ok(())
    }

    fn run(&self, listener: TcpListener) -> PyResult<FlightSqlServerState> {
        let address = listener.local_addr()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let config = Arc::clone(&self.config);
        let runtime = self.runtime.clone();
        info!("Starting the Flight SQL server on {address}...");
        let handle = thread::Builder::new().spawn(move || {
            Self::run_blocking(runtime.primary().clone(), config, runtime, listener, rx)
        })?;
        Ok(FlightSqlServerState {
            address,
            handle,
            shutdown: tx,
        })
    }
}
