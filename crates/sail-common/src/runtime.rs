// SPDX-License-Identifier: Apache-2.0

use tokio::runtime::{Handle, Runtime};

use crate::config::RuntimeConfig;
use crate::error::{CommonError, CommonResult};

#[derive(Debug)]
pub struct RuntimeManager {
    primary: Runtime,
    // FIXME:
    //  Ideally IO bound tasks should be run on the primary runtime,
    //  and CPU bound tasks on the secondary runtime.
    io: Runtime,
}

impl RuntimeManager {
    /// Attempts to initialize a new RuntimeManager instance.
    ///
    /// # Arguments
    ///
    /// * `config` - The runtime configuration to use.
    ///
    /// # Errors
    ///
    /// Returns a `CommonError` if the runtime manager fails to initialize.
    pub fn try_new(config: &RuntimeConfig) -> CommonResult<Self> {
        let primary = Self::build_runtime(config.stack_size)?;
        let io = Self::build_runtime(config.stack_size)?;
        Ok(Self { primary, io })
    }

    pub fn handle(&self) -> RuntimeHandle {
        let primary = self.primary.handle().clone();
        let io = self.io.handle().clone();
        RuntimeHandle { primary, io }
    }

    fn build_runtime(stack_size: usize) -> CommonResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .map_err(|e| CommonError::internal(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeHandle {
    primary: Handle,
    io: Handle,
}

impl RuntimeHandle {
    pub fn new(primary: Handle, io: Handle) -> Self {
        Self { primary, io }
    }

    pub fn primary(&self) -> &Handle {
        &self.primary
    }

    pub fn io(&self) -> &Handle {
        &self.io
    }
}

/// Wait for Ctrl-C or, on Unix, SIGTERM in a standalone Sail process.
pub async fn stop_signal() {
    #[cfg(unix)]
    let result = async {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    .await;

    #[cfg(not(unix))]
    let result = tokio::signal::ctrl_c().await;

    if let Err(e) = result {
        log::error!("failed to listen for a shutdown signal: {e}");
    }
}
