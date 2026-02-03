use std::collections::HashMap;
use std::env;
use std::ffi::CString;
use std::sync::{Arc, OnceLock};

use pyo3::exceptions::{PyRuntimeError, PyRuntimeWarning};
use pyo3::prelude::*;
use sail_common::config::{AppConfig, SAIL_ENV_VAR_PREFIX};
use sail_common::runtime::RuntimeManager;
use sail_telemetry::telemetry::{init_telemetry, ResourceOptions};

static GLOBALS: OnceLock<GlobalState> = OnceLock::new();

/// An approximate snapshot of environment variables used for application configuration.
pub struct EnvironmentSnapshot {
    vars: HashMap<String, String>,
}

impl EnvironmentSnapshot {
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
    pub config: Arc<AppConfig>,
    pub runtime: RuntimeManager,
    /// The environment variables when the global state is initialized.
    ///
    /// This is an approximation and may not be exactly the same as the
    /// environment variables used to load the configuration.
    pub environment: EnvironmentSnapshot,
}

impl GlobalState {
    pub fn instance() -> PyResult<&'static GlobalState> {
        GLOBALS
            .get()
            .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("global state not initialized"))
    }

    pub fn initialize() -> PyResult<()> {
        if GLOBALS.get().is_some() {
            return Ok(());
        }

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

        let state = GlobalState {
            config: Arc::new(config),
            runtime,
            environment,
        };

        if GLOBALS.set(state).is_err() {
            return Err(PyErr::new::<PyRuntimeError, _>(
                "global state already initialized",
            ));
        }
        Ok(())
    }
}
