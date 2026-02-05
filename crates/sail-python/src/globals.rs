use std::collections::HashMap;
use std::env;
use std::ffi::CString;

use log::debug;
use pyo3::exceptions::{PyRuntimeError, PyRuntimeWarning};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use sail_common::config::{AppConfig, SAIL_ENV_VAR_PREFIX};
use sail_common::runtime::RuntimeManager;
use sail_telemetry::telemetry::{init_telemetry, ResourceOptions};

static GLOBALS: PyOnceLock<GlobalState> = PyOnceLock::new();

/// An approximate snapshot of environment variables used for application configuration.
pub struct EnvironmentSnapshot {
    vars: HashMap<String, String>,
}

impl EnvironmentSnapshot {
    // TODO: implement a systematic way to manage static config
    const SAIL_STATIC_ENV_VAR_PREFIXES: &[&str] = &[
        "SAIL_RUNTIME__ENABLE_SECONDARY",
        "SAIL_RUNTIME__STACK_SIZE",
        "SAIL_TELEMETRY__",
    ];

    pub fn initialize() -> Self {
        let vars = Self::capture();
        Self { vars }
    }

    fn capture() -> HashMap<String, String> {
        env::vars()
            .filter(|(k, _)| k.starts_with(SAIL_ENV_VAR_PREFIX))
            .collect()
    }

    pub fn warn_if_changed(&self, py: Python<'_>) -> PyResult<()> {
        let current = Self::capture();

        let mut changed = Vec::new();
        for (key, value) in &self.vars {
            match current.get(key) {
                Some(v) => {
                    if value != v {
                        changed.push(key.clone());
                    }
                }
                None => {
                    changed.push(key.clone());
                }
            }
        }
        for key in current.keys() {
            if !self.vars.contains_key(key) {
                changed.push(key.clone());
            }
        }

        changed.retain(|key| {
            Self::SAIL_STATIC_ENV_VAR_PREFIXES
                .iter()
                .any(|prefix| key.starts_with(prefix))
        });

        if !changed.is_empty() {
            let message = format!(
                "changes to the environment variables are ignored: {}",
                changed.join(", ")
            );
            PyErr::warn(
                py,
                py.get_type::<PyRuntimeWarning>().as_any(),
                CString::new(message)?.as_c_str(),
                0,
            )?;
        }
        Ok(())
    }
}

pub struct GlobalState {
    pub runtime: RuntimeManager,
    /// The environment variables when the global state is initialized.
    ///
    /// This is an approximation and may not be exactly the same as the
    /// environment variables used to initialize the global state.
    pub environment: EnvironmentSnapshot,
}

impl GlobalState {
    /// Gets the global state instance, initializing it if necessary.
    ///
    /// The global state is not supposed to be initialized or accessed
    /// if the current Python interpreter uses the Sail CLI entrypoint.
    /// Otherwise, an error will be raised due to conflicting telemetry
    /// initialization.
    ///
    /// This function may fail if the global state is initialized concurrently,
    /// since telemetry can only be initialized once. But this likely won't happen
    /// in practice due to how this function is used in the codebase.
    pub fn instance(py: Python<'_>) -> PyResult<&'static GlobalState> {
        let globals = GLOBALS.get_or_try_init(py, Self::initialize)?;
        globals.environment.warn_if_changed(py)?;
        Ok(globals)
    }

    fn initialize() -> PyResult<Self> {
        // We capture the environment variables before loading the configuration.
        // It is theoretically possible that the environment variables are modified
        // after we capture the snapshot and before loading the configuration, but
        // the snapshot we capture here is a good enough approximation to warn users
        // about changes to the environment variables later.
        let environment = EnvironmentSnapshot::initialize();

        let config = AppConfig::load().map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to load configuration: {e}"))
        })?;

        let runtime = RuntimeManager::try_new(&config.runtime).map_err(|e| {
            PyErr::new::<PyRuntimeError, _>(format!("failed to create the runtime: {e}"))
        })?;

        runtime
            .handle()
            .primary()
            .block_on(async {
                let resource = ResourceOptions { kind: "server" };
                init_telemetry(&config.telemetry, resource)
            })
            .map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!("failed to initialize telemetry: {e}"))
            })?;

        // `init_telemetry` would fail if OpenTelemetry is already initialized.
        // If we reach here, it means OpenTelemetry is initialized successfully
        // by this function call. So it is guaranteed that the shutdown hook is
        // registered only once.
        Python::attach(|py| -> PyResult<()> {
            let atexit = PyModule::import(py, "atexit")?;
            atexit.call_method1("register", (wrap_pyfunction!(_shutdown_telemetry, py)?,))?;
            debug!("OpenTelemetry shutdown hook registered");
            Ok(())
        })?;

        Ok(GlobalState {
            runtime,
            environment,
        })
    }
}

#[pyfunction]
fn _shutdown_telemetry() {
    sail_telemetry::telemetry::shutdown_telemetry();
}
